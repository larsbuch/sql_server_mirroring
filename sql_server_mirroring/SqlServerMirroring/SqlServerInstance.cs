using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo.Agent;
using System.Data.SqlClient;
using HelperFunctions;
using Microsoft.SqlServer.Management.Smo.Wmi;
using System.ServiceProcess;
using System.IO;

namespace SqlServerMirroring
{
    public class SqlServerInstance
    {
        private const string DIRECTORY_SPLITTER = "\\";
        private const string URI_SPLITTER = "\\";

        private Server _server;
        private ILogger _logger;
        private ManagedComputer _managedComputer;

        public SqlServerInstance(ILogger logger, string connectionString)
        {
            _logger = logger;
            ValidateConnectionStringAndDatabaseConnection(connectionString);
            _server = new Server(new ServerConnection(new SqlConnection(connectionString)));
            _managedComputer = new ManagedComputer("(local)");
        }

        private void ValidateConnectionStringAndDatabaseConnection(string connectionString)
        {
            try
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Database instance connection with connection string |{0}| failed.", connectionString), ex);
            }
        }

        #region Properties

        private Server DatabaseServerInstance
        {
            get
            {
                return _server;
            }
        }

        private ManagedComputer ComputerInstance
        {
            get
            {
                return _managedComputer;
            }
        }

        private ILogger Logger
        {
            get
            {
                return _logger;
            }
        }

        #endregion

        #region Instance methods

        public string Instance_Status()
        {
            return DatabaseServerInstance.Status.ToString();
        }

        public List<string> Instance_Endpoints()
        {
            List<string> returnList = new List<string>();
            foreach(Endpoint endpoint in DatabaseServerInstance.Endpoints)
            {
                returnList.Add(endpoint.ToString());
            }
            return returnList;
        }

        public Dictionary<string, string> Instance_Information()
        {
            Dictionary<string, string> serverInformation = new Dictionary<string, string>();
            serverInformation.Add("Instance Name", DatabaseServerInstance.InstanceName);
            serverInformation.Add("Instance Service Name", DatabaseServerInstance.ServiceName);
            serverInformation.Add("Instance Backup Directory", DatabaseServerInstance.BackupDirectory);
            serverInformation.Add("Instance Build Number", DatabaseServerInstance.BuildNumber.ToString());
            serverInformation.Add("Instance Collation", DatabaseServerInstance.Collation);
            serverInformation.Add("Instance Edition", DatabaseServerInstance.Edition);
            serverInformation.Add("Instance Engine Edition", DatabaseServerInstance.EngineEdition.ToString());
            serverInformation.Add("Instance Error Log Path", DatabaseServerInstance.ErrorLogPath);
            serverInformation.Add("Instance Language", DatabaseServerInstance.Language);
            serverInformation.Add("Instance Physical Machine Name", DatabaseServerInstance.Name);
            serverInformation.Add("Instance Named Pipes Enabled", DatabaseServerInstance.NamedPipesEnabled ? "Yes" : "No");
            serverInformation.Add("Instance OS Version", DatabaseServerInstance.OSVersion);
            serverInformation.Add("Instance Platform", DatabaseServerInstance.Platform);
            serverInformation.Add("Instance Product", DatabaseServerInstance.Product);
            serverInformation.Add("Instance Product Level", DatabaseServerInstance.ProductLevel);
            serverInformation.Add("Instance Service Start Mode", DatabaseServerInstance.ServiceStartMode.ToString());
            serverInformation.Add("Instance Tcp Enabled", DatabaseServerInstance.TcpEnabled ? "Yes" : "No");
            serverInformation.Add("Instance Server Type", DatabaseServerInstance.ServerType.ToString());
            serverInformation.Add("Instance Service Account", DatabaseServerInstance.ServiceAccount);
            serverInformation.Add("Instance Version", DatabaseServerInstance.Version.ToString());

            return serverInformation;
        }

        public Dictionary<string, string> SqlAgent_Information()
        {
            Dictionary<string, string> sqlAgentInformation = new Dictionary<string, string>();
            EnableAgentXps();
            try
            {
                sqlAgentInformation.Add("Sql Server Agent Auto Start", DatabaseServerInstance.JobServer.SqlAgentAutoStart ? "Yes" : "No");
                sqlAgentInformation.Add("Sql Server Agent Error Log", DatabaseServerInstance.JobServer.ErrorLogFile);
                sqlAgentInformation.Add("Sql Server Agent Log Level", DatabaseServerInstance.JobServer.AgentLogLevel.ToString());
                sqlAgentInformation.Add("Sql Server Agent Service Start Mode", DatabaseServerInstance.JobServer.ServiceStartMode.ToString());
                sqlAgentInformation.Add("Sql Server Agent Service Account", DatabaseServerInstance.JobServer.ServiceAccount);
            }
            catch (ExecutionFailureException efe)
            {
                if (efe.InnerException.Message.StartsWith("SQL Server blocked access to procedure 'dbo.sp_get_sqlagent_properties' of component 'Agent XPs'"))
                {
                    throw new SqlServerMirroringException("'Agent XPs' are disabled in Sql Server seems to be disabled and cannot be started", efe);
                }
                else
                {
                    throw;
                }
            }
            return sqlAgentInformation;
        }

        private void EnableAgentXps()
        {
            Configuration configuration = DatabaseServerInstance.Configuration;
            if (configuration.AgentXPsEnabled.RunValue == 0)
            {
                try
                {
                    configuration.AgentXPsEnabled.ConfigValue = 1;
                    configuration.Alter();
                    Logger.LogInfo("Enabling 'Agent XPs' succeeded");
                }
                catch (Exception e)
                {
                    throw new SqlServerMirroringException("Enabling 'Agent XPs' failed", e);
                }
            }
        }

        public bool WindowsAuthentificationActive()
        {
            if(DatabaseServerInstance.LoginMode == ServerLoginMode.Integrated || DatabaseServerInstance.LoginMode == ServerLoginMode.Mixed)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool SqlServerAuthentificationActive()
        {
            if (DatabaseServerInstance.LoginMode == ServerLoginMode.Normal || DatabaseServerInstance.LoginMode == ServerLoginMode.Mixed)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /* Lists the current MirrorStates of the databases on the instance */
        public IEnumerable<MirrorState> DatabaseMirrorStates
        {
            get
            {
                foreach (Database database in UserDatabases)
                {
                    yield return new MirrorState(database);
                }
            }
        }

        //Setup instance
        public bool CheckInstanceForMirroring()
        {
            EnableAgentXps();
            if (DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Manual ||
                DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Disabled)
            {
                Logger.LogWarning(string.Format("Sql Server service should be configured to start automatically. It is set to {0}", DatabaseServerInstance.ServiceStartMode.ToString()));
                return false;
            }
            if (!DatabaseServerInstance.JobServer.SqlAgentAutoStart)
            {
                Logger.LogWarning("Sql Agent not configured to auto start on server restart");
                return false;
            }
            if (!CheckSqlAgentRunning())
            {
                Logger.LogWarning("Sql Server Agent service is not running");
                return false;
            }
            return true;
        }

        private bool CheckSqlAgentRunning()
        {
            /* TODO: Check that it is the correct instance sql agent */
            foreach (Service service in SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
            {
                Logger.LogDebug(string.Format("Checking Sql Agent {0} with display name {1}", service.Name, service.DisplayName));
                if (service.ServiceState == ServiceState.Running)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            throw new SqlServerMirroringException("No Sql Agent installed on the server. Install Sql Agent to set up mirroring.");
        }

        private IEnumerable<Service> SqlServerServicesInstalled
        {
            get
            {
                foreach(Service service in ComputerInstance.Services)
                {
                    yield return service;
                }
            }
        }

        public void SetupInstanceForMirroring()
        {
            EnableAgentXps();
            if (DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Manual || 
                DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Disabled)
            {
                // TODO fix security issue (guess)
                Logger.LogDebug("Bug: Cannot change to automatic start");
                ChangeDatabaseServiceToAutomaticStart();
                Logger.LogInfo("Sql Server was set to Automatic start");
            }
            if (!DatabaseServerInstance.JobServer.SqlAgentAutoStart)
            {
                // TODO fix security issue (guess)
                Logger.LogDebug("Bug: Cannot change to automatic start");
                ChangeSqlAgentServiceToAutomaticStart();
                Logger.LogInfo("Sql Agent was set to Automatic start");
            }
            if (!CheckSqlAgentRunning())
            {
                // TODO fix security issue (guess)
                Logger.LogDebug("Bug: Cannot start service");
                StartSqlAgent();
                Logger.LogInfo("Sql Agent service was started");
            }
        }

        private void ChangeSqlAgentServiceToAutomaticStart()
        {
            /* TODO: Check that it is the correct instance sql server */
            foreach (Service service in SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
            {
                Logger.LogDebug(string.Format("Checking Sql Agent {0}({1}) in startup state {2} before change", service.Name, service.DisplayName, service.StartMode));
                //ServiceController sc = new ServiceController(service.Name);
                service.StartMode = Microsoft.SqlServer.Management.Smo.Wmi.ServiceStartMode.Auto;
                service.Alter();
                Logger.LogInfo(string.Format("Checking Sql Agent {0}({1}) in startup state {2} after change", service.Name, service.DisplayName, service.StartMode));
            }
        }

        private void ChangeDatabaseServiceToAutomaticStart()
        {
            /* TODO: Check that it is the correct instance sql server */
            foreach (Service service in SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlServer))
            {
                Logger.LogDebug(string.Format("Checking Sql Server {0}({1}) in startup state {2} before change", service.Name, service.DisplayName, service.StartMode));
                service.StartMode = Microsoft.SqlServer.Management.Smo.Wmi.ServiceStartMode.Auto;
                service.Alter();
                Logger.LogInfo(string.Format("Checking Sql Server {0}({1}) in startup state {2} after change", service.Name, service.DisplayName, service.StartMode));
            }
        }

        private void StartSqlAgent()
        {
            /* TODO: Check that it is the correct instance sql agent */
            foreach (Service service in SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
            {
                Logger.LogDebug(string.Format("Checking Sql Agent {0}({1}) in state {2}", service.Name, service.DisplayName, service.ServiceState));
                if (service.ServiceState != ServiceState.Running)
                {
                    int timeoutCounter = 0;
                    service.Start();
                    while (service.ServiceState != ServiceState.Running && timeoutCounter > ServiceStartTimeout)
                    {
                        timeoutCounter += ServiceStartTimeoutStep;
                        System.Threading.Thread.Sleep(ServiceStartTimeoutStep);
                        Console.WriteLine(string.Format("Waited {0} seconds for Sql Agent {1}({2}) starting: {3}", (timeoutCounter/1000), service.Name, service.DisplayName, service.ServiceState));
                        service.Refresh();
                    }
                    if(timeoutCounter > ServiceStartTimeout)
                    {
                        throw new SqlServerMirroringException(string.Format("Timed out waiting for Sql Agent {1}({2}) starting", service.Name, service.DisplayName));
                    }
                }
            }
        }

        public IEnumerable<Database> UserDatabases
        {
            get
            {
                foreach (Database database in DatabaseServerInstance.Databases)
                {
                    if (!database.IsSystemObject)
                    {
                        yield return database;
                    }
                }
            }
        }

        public int ServiceStartTimeout
        {
            get
            {
                /* 120 sec */
                return 120000;
            }
        }

        public int ServiceStartTimeoutStep
        {
            get
            {
                /* 5 sec */
                return 5000;
            }
        }

        public void StartUpMirrorCheck(Dictionary<string,ConfiguredDatabaseForMirroring> configuredMirrorDatabases, bool serverPrincipal)
        {
            foreach(MirrorState mirrorState in DatabaseMirrorStates)
            {
                if(mirrorState.IsConfiguredForMirroring)
                {
                    if(!configuredMirrorDatabases.ContainsKey(mirrorState.DatabaseName))
                    {
                        Logger.LogWarning(string.Format("Database {0} was set up for mirroring but is not in configuration", mirrorState.DatabaseName));
                        RemoveDatabaseFromMirroring(mirrorState, serverPrincipal);
                    }
                }
                else
                {
                    if(configuredMirrorDatabases.ContainsKey(mirrorState.DatabaseName))
                    {
                        Logger.LogWarning(string.Format("Database {0} is not set up for mirroring but is in configuration", mirrorState.DatabaseName));
                        ConfiguredDatabaseForMirroring configuredDatabase;
                        configuredMirrorDatabases.TryGetValue(mirrorState.DatabaseName, out configuredDatabase);
                        AddDatabaseToMirroring(configuredDatabase, serverPrincipal);
                    }
                }
            }
        }

        #endregion

        #region Individual Databases

        private void AddDatabaseToMirroring(ConfiguredDatabaseForMirroring configuredDatabase, bool serverPrincipal)
        {
            Database database = UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName)).First();
            if (database.RecoveryModel != RecoveryModel.Full)
            {
                try
                {
                    database.RecoveryModel = RecoveryModel.Full;
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Could not set database {0} to Full Recovery", database.Name), ex);
                }
            }
            if (!database.BrokerEnabled)
            {
                database.BrokerEnabled = true;
                database.Alter(TerminationClause.RollbackTransactionsImmediately);
            }
            if(serverPrincipal)
            {
                if(BackupDatabaseForMirrorServer(configuredDatabase))
                {
                    Logger.LogInfo("Backup created and moved to remote share");
                }
                else
                {
                    Logger.LogInfo("Backup created and moved to local share due to missing access to remote share");
                }
            }
            else
            {
                if(RestoreDatabase(configuredDatabase))
                {
                    Logger.LogInfo("Restored backup");
                }
                else
                {
                    Logger.LogInfo("Moved database from remote share and restored Backup");
                }
            }
            
            CreateEndpoint(configuredDatabase, serverPrincipal);

            // Create mirroring
            CreateMirroring(configuredDatabase, serverPrincipal);
        }

        private void CreateEndpoint(ConfiguredDatabaseForMirroring configuredDatabase, bool serverPrincipal)
        {
            // EndPoints Needed for each mirror database
            try
            {
                //Set up a database mirroring endpoint on the server before 
                //setting up a database mirror. 
                //Define an Endpoint object variable for database mirroring. 
                Endpoint ep = default(Endpoint);
                ep = new Endpoint(DatabaseServerInstance, configuredDatabase.Endpoint_Name);
                ep.ProtocolType = ProtocolType.Tcp;
                ep.EndpointType = EndpointType.DatabaseMirroring;
                //Specify the protocol ports. 
                ep.Protocol.Http.SslPort = configuredDatabase.Endpoint_SslPort;
                ep.Protocol.Tcp.ListenerPort = configuredDatabase.Endpoint_ListenerPort;
                //Specify the role of the payload. 
                ep.Payload.DatabaseMirroring.ServerMirroringRole = ServerMirroringRole.All;
                //Create the endpoint on the instance of SQL Server. 
                ep.Create();
                //Start the endpoint. 
                ep.Start();
                Logger.LogDebug(string.Format("Created endpoint for {0}. Endpoint in state {1}.", configuredDatabase.DatabaseName, ep.EndpointState));
            }
            catch(Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Creation of endpoint for {0} failed", configuredDatabase.DatabaseName), ex);
            }
        }

        private void RemoveDatabaseFromMirroring(MirrorState mirrorState, bool serverPrincipal)
        {
            Database database = UserDatabases.Where(s => s.Name.Equals(mirrorState.DatabaseName)).First();
            // Full recovery
            // ServiceBroker
            // EndPoints Needed for each mirror database ?
            throw new NotImplementedException();
        }

        //public void SetupPrincipalDatabases(Dictionary<string, ConfiguredDatabaseForMirroring> databasesToBeMirrored)
        //{
        //    foreach(ConfiguredDatabaseForMirroring)
        //    SetupDatabasesForMirroring(databasesToBeMirrored);
        //    // Setup mirroring with MirrorDatabase
        //    // backup database and move to shared drive on remote machine if possible
        //}
        //public void SetupSecondaryDatabases(Dictionary<string, ConfiguredDatabaseForMirroring> databasesToBeMirrored)
        //{
        //    SetupDatabasesForMirroring(databasesToBeMirrored);
        //    // Setup mirroring with MirrorDatabase (Database.ChangeMirroringState)
        //    // restore database
        //}


        //public void RemoveDatabasesFromMirroring(Dictionary<string, ConfiguredDatabaseForMirroring> mirroredDatabasesToBeRemovedFromMirroring)
        //{

        //}

        // Use MirrorState to find the existing state
        public bool CheckMirrorStateForSwitchingNeeded(Dictionary<string, ConfiguredDatabaseForMirroring> databasesToBeChecked)
        {
            //SMO: Database.MirroringStatus
            throw new NotImplementedException();
        }

        // Setup Backup with BackupDatabase (daily)
        public string BackupDatabase(ConfiguredDatabaseForMirroring configuredDatabase)
        {
            string localDirectoryForBackup = configuredDatabase.LocalDirectoryForBackup;
            DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, localDirectoryForBackup);

            Database database = UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName)).First();


            // Store the current recovery model in a variable. 
            int recoverymod;
            recoverymod = (int)db.DatabaseOptions.RecoveryModel;

            // Define a Backup object variable. 
            Backup bk = new Backup();

            // Specify the type of backup, the description, the name, and the database to be backed up. 
            bk.Action = BackupActionType.Database;
            bk.BackupSetDescription = "Full backup of Adventureworks2012";
            bk.BackupSetName = "AdventureWorks2012 Backup";
            bk.Database = "AdventureWorks2012";

            // Declare a BackupDeviceItem by supplying the backup device file name in the constructor, and the type of device is a file. 
            BackupDeviceItem bdi = default(BackupDeviceItem);
            bdi = new BackupDeviceItem("Test_Full_Backup1", DeviceType.File);

            // Add the device to the Backup object. 
            bk.Devices.Add(bdi);
            // Set the Incremental property to False to specify that this is a full database backup. 
            bk.Incremental = false;

            // Set the expiration date. 
            System.DateTime backupdate = new System.DateTime();
            backupdate = new System.DateTime(2006, 10, 5);
            bk.ExpirationDate = backupdate;

            // Specify that the log must be truncated after the backup is complete. 
            bk.LogTruncation = BackupTruncateLogType.Truncate;

            // Run SqlBackup to perform the full database backup on the instance of SQL Server. 
            bk.SqlBackup(srv);

            // Inform the user that the backup has been completed. 
            System.Console.WriteLine("Full Backup complete.");

            // Remove the backup device from the Backup object. 
            bk.Devices.Remove(bdi);


            // Create another file device for the differential backup and add the Backup object. 
            BackupDeviceItem bdid = default(BackupDeviceItem);
            bdid = new BackupDeviceItem("Test_Differential_Backup1", DeviceType.File);

            // Add the device to the Backup object. 
            bk.Devices.Add(bdid);

            // Set the Incremental property to True for a differential backup. 
            bk.Incremental = true;

            // Run SqlBackup to perform the incremental database backup on the instance of SQL Server. 
            bk.SqlBackup(srv);

            // Inform the user that the differential backup is complete. 
            System.Console.WriteLine("Differential Backup complete.");

            // Remove the device from the Backup object. 
            bk.Devices.Remove(bdid);






        }

        // Setup Backup with BackupDatabase (responsible for moving database backup to remote share)
        public bool BackupDatabaseForMirrorServer(ConfiguredDatabaseForMirroring configuredDatabase)
        {
            string fileName = BackupDatabase(configuredDatabase);
            string localDirectoryForBackup = configuredDatabase.LocalDirectoryForBackup;
            DirectoryHelper.TestReadWriteAccessToDirectory(Logger, localDirectoryForBackup);
            string localShareForBackup = configuredDatabase.LocalShareForBackup;
            string localShareName = configuredDatabase.LocalShareName;
            try
            {
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(Logger, localShareForBackup, localShareName);
                File.Move(localDirectoryForBackup + DIRECTORY_SPLITTER + fileName, localShareForBackup + DIRECTORY_SPLITTER + fileName);
            }
            catch (Exception e)
            {
                throw new SqlServerMirroringException("Failed moving backup file locally",e);
            }
            try
            {
                string remoteServer = configuredDatabase.RemoteServer;
                string remoteShare = configuredDatabase.RemoteShareForBackup;
                string remoteTempFolderForBackup = configuredDatabase.RemoteTempFolderForBackup;
                string fullRemoteTempFolderForBackup = URI_SPLITTER + URI_SPLITTER + remoteServer + URI_SPLITTER + remoteShare + URI_SPLITTER + remoteTempFolderForBackup;
                ShareHelper.ValidRemotePath(Logger, fullRemoteTempFolderForBackup);
                string remoteDeliveryFolderForBackup = configuredDatabase.RemoteDeliveryFolderForBackup;
                string fullRemoteDeliveryFolderForBackup = URI_SPLITTER + URI_SPLITTER + remoteServer + URI_SPLITTER + remoteShare + URI_SPLITTER + remoteDeliveryFolderForBackup;
                ShareHelper.ValidRemotePath(Logger, fullRemoteTempFolderForBackup);
                File.Move(localShareForBackup + DIRECTORY_SPLITTER + fileName, fullRemoteTempFolderForBackup + URI_SPLITTER + fileName);
                File.Move(fullRemoteTempFolderForBackup + URI_SPLITTER + fileName, fullRemoteDeliveryFolderForBackup + URI_SPLITTER + fileName);
                return true; // return true if moved to remote directory
            }
            catch (Exception)
            {
                /* Ignore the reason for the failure */
                return false; // return false if moved to remote directory
            }
        }

        // Setup Restore with RestoreDatabase (responsible for restoring database)
        public bool RestoreDatabase(ConfiguredDatabaseForMirroring configuredDatabase)
        {
            MoveRemoteFileToLocalRestore(configuredDatabase);

            string localDriveForRestore = configuredDatabase.LocalDriveForRestore;


            string fileName = GetNewesteFilename(configuredDatabase.DatabaseName, localDriveForRestore);

            // Define a Restore object variable.
            Restore rs = new Restore();

            // Set the NoRecovery property to true, so the transactions are not recovered. 
            rs.NoRecovery = true;

            // Add the device that contains the full database backup to the Restore object. 
            rs.Devices.Add(bdi);

            // Specify the database name. 
            rs.Database = "AdventureWorks2012";

            // Restore the full database backup with no recovery. 
            rs.SqlRestore(srv);

            // Inform the user that the Full Database Restore is complete. 
            Console.WriteLine("Full Database Restore complete.");

            // reacquire a reference to the database
            db = srv.Databases["AdventureWorks2012"];

            // Remove the device from the Restore object.
            rs.Devices.Remove(bdi);

            // Set the NoRecovery property to False. 
            rs.NoRecovery = false;

            // Add the device that contains the differential backup to the Restore object. 
            rs.Devices.Add(bdid);

            // Restore the differential database backup with recovery. 
            rs.SqlRestore(srv);

            // Inform the user that the differential database restore is complete. 
            System.Console.WriteLine("Differential Database Restore complete.");

            // Remove the device. 
            rs.Devices.Remove(bdid);



            throw new NotImplementedException();
        }

        private string GetNewesteFilename(string databaseName, string localDriveForRestore)
        {
            throw new NotImplementedException();
        }

        private void MoveRemoteFileToLocalRestore(ConfiguredDatabaseForMirroring configuredDatabase)
        {
            //TODO implement with creation of folders
        }

        public bool SetAllMirrorDatabasesForMaintainanceState(Dictionary<string, ConfiguredDatabaseForMirroring> databasesToBeRestored)
        {
            throw new NotImplementedException();
        }

        public bool RemoveAllMirrorDatabasesForMaintainanceState(Dictionary<string, ConfiguredDatabaseForMirroring> databasesToBeRestored)
        {
            throw new NotImplementedException();
        }


        #endregion
    }
}
