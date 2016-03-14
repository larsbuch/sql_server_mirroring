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
using System.Text.RegularExpressions;

namespace SqlServerMirroring
{
    public class SqlServerInstance
    {
        private const string DIRECTORY_SPLITTER = "\\";
        private const string URI_SPLITTER = "\\";

        private Server _server;
        private ILogger _logger;
        private ManagedComputer _managedComputer;
        private Dictionary<ServerStateEnum, ServerState> _serverStates;
        private ServerState _activeServerState;
        private Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> _configuredMirrorDatabases;

        public SqlServerInstance(ILogger logger, string connectionString)
        {
            _logger = logger;
            ValidateConnectionStringAndDatabaseConnection(connectionString);
            _server = new Server(new ServerConnection(new SqlConnection(connectionString)));
            _configuredMirrorDatabases = new Dictionary<DatabaseName, ConfiguredDatabaseForMirroring>();
            _managedComputer = new ManagedComputer("(local)");
            BuildServerStates();
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

        private void BuildServerStates()
        {
            _serverStates = new Dictionary<ServerStateEnum, ServerState>();
            _serverStates.Add(ServerStateEnum.STARTUP_STATE
                , new ServerState(ServerStateEnum.STARTUP_STATE, true,new List<ServerStateEnum>()
                { ServerStateEnum.SHUTTING_DOWN_STATE, ServerStateEnum.RUNNING_STATE}));
            _serverStates.Add(ServerStateEnum.RUNNING_STATE
                , new ServerState(ServerStateEnum.RUNNING_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.SHUTTING_DOWN_STATE, ServerStateEnum.MAINTENANCE_STATE, ServerStateEnum.FORCED_RUNNING_STATE
                , ServerStateEnum.MANUAL_FAILOVER_STATE, ServerStateEnum.FORCED_MANUAL_FAILOVER_STATE }));
            _serverStates.Add(ServerStateEnum.FORCED_RUNNING_STATE
                , new ServerState(ServerStateEnum.FORCED_RUNNING_STATE, true, new List<ServerStateEnum>()
                {ServerStateEnum.SHUTTING_DOWN_STATE, ServerStateEnum.MAINTENANCE_STATE, ServerStateEnum.RUNNING_STATE }));
            _serverStates.Add(ServerStateEnum.SHUTTING_DOWN_STATE
                , new ServerState(ServerStateEnum.SHUTTING_DOWN_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SHUTDOWN_STATE}));
            _serverStates.Add(ServerStateEnum.SHUTDOWN_STATE
                , new ServerState(ServerStateEnum.SHUTDOWN_STATE, true, new List<ServerStateEnum>()
                {ServerStateEnum.STARTUP_STATE, ServerStateEnum.MAINTENANCE_STATE }));
            _serverStates.Add(ServerStateEnum.MAINTENANCE_STATE
                , new ServerState(ServerStateEnum.MAINTENANCE_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.SHUTTING_DOWN_STATE, ServerStateEnum.RUNNING_STATE, ServerStateEnum.FORCED_RUNNING_STATE
                , ServerStateEnum.MANUAL_FAILOVER_STATE, ServerStateEnum.FORCED_MANUAL_FAILOVER_STATE}));
            _serverStates.Add(ServerStateEnum.MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.MANUAL_FAILOVER_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.SHUTTING_DOWN_STATE, ServerStateEnum.MAINTENANCE_STATE, ServerStateEnum.RUNNING_STATE}));
            _serverStates.Add(ServerStateEnum.FORCED_MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.FORCED_MANUAL_FAILOVER_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SHUTTING_DOWN_STATE, ServerStateEnum.FORCED_RUNNING_STATE}));
            if(!_serverStates.TryGetValue(ServerStateEnum.SHUTDOWN_STATE, out _activeServerState))
            {
                throw new SqlServerMirroringException("Could not get default Server State.");
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
        public ServerState ServerState
        {
            get
            {
                return _activeServerState;
            }
        }

        public Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> ConfiguredMirrorDatabases
        {
            get
            {
                return _configuredMirrorDatabases;
            }
            set
            {
                _configuredMirrorDatabases = value;
            }
        }

        #endregion

        #region Instance methods

        public bool IsValidServerStateChange(ServerStateEnum newState)
        {
            return _activeServerState.ValidTransition(newState);
        }

        public void MakeServerStateChange(ServerStateEnum newState)
        {
            if(IsValidServerStateChange(newState))
            {
                ServerState newServerState;
                if(_serverStates.TryGetValue(newState, out newServerState))
                {
                    Logger.LogInfo(string.Format("Server state changed from {0} to {1}.", _activeServerState.StateName, newServerState.StateName));
                    _activeServerState = newServerState;
                    StartNewServerState(newState);
                    Logger.LogDebug(string.Format("Server in new state {0}.", newServerState.StateName));
                }
                else
                {
                    throw new SqlServerMirroringException(string.Format("Server in state {0} could not get new state {1}.", _activeServerState.State.ToString(), newState.ToString()));
                }
            }
            else
            {
                throw new SqlServerMirroringException(string.Format("Server in state {0} does not allow state shange to {1}.", _activeServerState.State.ToString(), newState.ToString()));
            }
        }

        private void StartNewServerState(ServerStateEnum newState)
        {
            switch (newState)
            {
                case ServerStateEnum.STARTUP_STATE:
                    StartStartupState();
                    break;
                case ServerStateEnum.RUNNING_STATE:
                    StartRunningState();
                    break;
                case ServerStateEnum.FORCED_RUNNING_STATE:
                    StartForcedRunningState();
                    break;
                case ServerStateEnum.SHUTTING_DOWN_STATE:
                    StartShuttingDownState();
                    break;
                case ServerStateEnum.SHUTDOWN_STATE:
                    StartShutdownState();
                    break;
                case ServerStateEnum.MAINTENANCE_STATE:
                    StartMaintenanceState();
                    break;
                case ServerStateEnum.MANUAL_FAILOVER_STATE:
                    StartManualFailoverState();
                    break;
                case ServerStateEnum.FORCED_MANUAL_FAILOVER_STATE:
                    StartForcedManualFailoverState();
                    break;
                default:
                    throw new SqlServerMirroringException(string.Format("Unknown state {0}.", newState.ToString()));
            }
        }

        private void StartStartupState()
        {
            Logger.LogDebug("StartStartupState starting");
            try
            {
                // TODO
                if(!CheckInstanceForMirroring())
                {
                    SetupInstanceForMirroring();
                }
                // TODO replace true
                StartUpMirrorCheck(ConfiguredMirrorDatabases, true);

                // TODO start mirroring ??

                Logger.LogDebug("StartStartupState ended");
                MakeServerStateChange(ServerStateEnum.RUNNING_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Startup State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SHUTTING_DOWN_STATE);
            }
        }

        private void StartRunningState()
        {
            Logger.LogDebug("StartRunningState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartRunningState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Running State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SHUTTING_DOWN_STATE);
            }
        }

        private void StartForcedRunningState()
        {
            Logger.LogDebug("StartForcedRunningState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartForcedRunningState starting");
            }
            catch (Exception ex)
            {
                Logger.LogError("Forced Running State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SHUTTING_DOWN_STATE);
            }
        }

        private void StartShuttingDownState()
        {
            Logger.LogDebug("StartShuttingDownState starting");
            try
            {
                /* Does not do something special */
               Logger.LogDebug("StartShuttingDownState ended");
                MakeServerStateChange(ServerStateEnum.SHUTDOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SHUTDOWN_STATE);
            }
        }

        private void StartShutdownState()
        {
            Logger.LogDebug("StartShutdownState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartShutdownState ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutdown State could not be started.", ex);
            }
        }

        private void StartMaintenanceState()
        {
            Logger.LogDebug("StartMaintenanceState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartMaintenanceState starting");
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SHUTTING_DOWN_STATE);
            }
        }

        private void StartManualFailoverState()
        {
            Logger.LogDebug("StartManualFailoverState starting");
            try
            {
                throw new NotImplementedException();
                Logger.LogDebug("StartManualFailoverState ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SHUTTING_DOWN_STATE);
            }
        }

        private void StartForcedManualFailoverState()
        {
            Logger.LogDebug("StartForcedManualFailoverState starting");
            try
            {
                throw new NotImplementedException();
                Logger.LogDebug("StartForcedManualFailoverState endecd");
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SHUTTING_DOWN_STATE);
            }
        }

        public string Instance_Status()
        {
            return DatabaseServerInstance.Status.ToString();
        }

        public List<string> Instance_Endpoints()
        {
            List<string> returnList = new List<string>();
            foreach (Endpoint endpoint in DatabaseServerInstance.Endpoints)
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
            if (DatabaseServerInstance.LoginMode == ServerLoginMode.Integrated || DatabaseServerInstance.LoginMode == ServerLoginMode.Mixed)
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
                foreach (Service service in ComputerInstance.Services)
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
                        Console.WriteLine(string.Format("Waited {0} seconds for Sql Agent {1}({2}) starting: {3}", (timeoutCounter / 1000), service.Name, service.DisplayName, service.ServiceState));
                        service.Refresh();
                    }
                    if (timeoutCounter > ServiceStartTimeout)
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

        public void StartUpMirrorCheck(Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredMirrorDatabases, bool serverPrincipal)
        {
            foreach (MirrorState mirrorState in DatabaseMirrorStates)
            {
                if (mirrorState.IsConfiguredForMirroring)
                {
                    if (!configuredMirrorDatabases.ContainsKey(mirrorState.DatabaseName))
                    {
                        Logger.LogWarning(string.Format("Database {0} was set up for mirroring but is not in configuration", mirrorState.DatabaseName));
                        RemoveDatabaseFromMirroring(mirrorState, serverPrincipal);
                    }
                }
                else
                {
                    if (configuredMirrorDatabases.ContainsKey(mirrorState.DatabaseName))
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
            if (serverPrincipal)
            {
                if (BackupDatabaseForMirrorServer(configuredDatabase))
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
                if (RestoreDatabase(configuredDatabase))
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

        private void CreateMirroring(ConfiguredDatabaseForMirroring configuredDatabase, bool serverPrincipal)
        {
            Database database = UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).First();
            try
            {
                database.ChangeMirroringState(MirroringOption.Resume);
                database.Alter(TerminationClause.RollbackTransactionsImmediately);
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Removing mirroring failed for {0}", configuredDatabase.DatabaseName.ToString()), ex);
            }
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
                ep = new Endpoint(DatabaseServerInstance, configuredDatabase.DatabaseName.Endpoint_Name);
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
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Creation of endpoint for {0} failed", configuredDatabase.DatabaseName), ex);
            }
        }

        private void RemoveDatabaseFromMirroring(MirrorState mirrorState, bool serverPrincipal)
        {
            Database database = UserDatabases.Where(s => s.Name.Equals(mirrorState.DatabaseName.ToString())).First();
            try
            {
                database.ChangeMirroringState(MirroringOption.Off);
                database.Alter(TerminationClause.RollbackTransactionsImmediately);
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Removing mirroring failed for {0}", mirrorState.DatabaseName), ex);
            }
            try
            {
                string endpointName = mirrorState.DatabaseName.Endpoint_Name;
                foreach (Endpoint endpoint in DatabaseServerInstance.Endpoints)
                {
                    if (endpoint.Name.Equals(endpointName))
                    {
                        Logger.LogDebug(string.Format("Trying to stop and drop endpoint {0}", endpointName));
                        endpoint.Stop();
                        endpoint.Drop();
                        Logger.LogInfo(string.Format("Endpoint {0} has been removed.", endpointName));
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Removal of endpoint {0} failed.", mirrorState.DatabaseName.Endpoint_Name), ex);
            }

            // TODO remove step for non-witness systems
            try
            {
                database.BrokerEnabled = false;
                database.Alter(TerminationClause.RollbackTransactionsImmediately);
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Disabling service broker on {0} failed.", mirrorState.DatabaseName.ToString()), ex);
            }
        }

        // Use MirrorState to find the existing state
        public bool CheckMirrorStateForSwitchingNeeded(Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> databasesToBeChecked)
        {
            //SMO: Database.MirroringStatus
            throw new NotImplementedException();
        }

        // Setup Backup with BackupDatabase (daily)
        public string BackupDatabase(ConfiguredDatabaseForMirroring configuredDatabase)
        {
            try
            {
                DirectoryPath localDirectoryForBackup = configuredDatabase.LocalBackupDirectoryWithSubDirectory;
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, localDirectoryForBackup);

                Database database = UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).First();

                string fileName = configuredDatabase.DatabaseName + "_" + DateTime.Now.ToFileTime() + ".bak";
                string fullFileName = localDirectoryForBackup.PathString + DIRECTORY_SPLITTER + fileName;

                // Define a Backup object variable. 
                Backup bk = new Backup();

                // Specify the type of backup, the description, the name, and the database to be backed up. 
                bk.Action = BackupActionType.Database;
                bk.BackupSetDescription = "Full backup of " + configuredDatabase.DatabaseName.ToString();
                bk.BackupSetName = configuredDatabase.DatabaseName.ToString() + " Backup";
                bk.Database = configuredDatabase.DatabaseName.ToString();

                // Declare a BackupDeviceItem by supplying the backup device file name in the constructor, and the type of device is a file. 
                BackupDeviceItem bdi = default(BackupDeviceItem);
                bdi = new BackupDeviceItem(fullFileName, DeviceType.File);

                // Add the device to the Backup object. 
                bk.Devices.Add(bdi);
                // Set the Incremental property to False to specify that this is a full database backup. 
                bk.Incremental = false;

                // Set the expiration date. 
                System.DateTime backupdate = System.DateTime.Now.AddDays(configuredDatabase.BackupExpirationTime);
                bk.ExpirationDate = backupdate;

                // Specify that the log must be truncated after the backup is complete. 
                bk.LogTruncation = BackupTruncateLogType.Truncate;

                // Run SqlBackup to perform the full database backup on the instance of SQL Server. 
                bk.SqlBackup(DatabaseServerInstance);

                // Inform the user that the backup has been completed. 
                Logger.LogInfo(string.Format("Full backup of {0} done", configuredDatabase.DatabaseName));

                // Remove the backup device from the Backup object. 
                bk.Devices.Remove(bdi);

                return fileName;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Backup of database {0} failed", configuredDatabase.DatabaseName), ex);
            }
        }

        // Setup Backup with BackupDatabase (responsible for moving database backup to remote share)
        public bool BackupDatabaseForMirrorServer(ConfiguredDatabaseForMirroring configuredDatabase)
        {
            string fileName = BackupDatabase(configuredDatabase);
            DirectoryPath localBackupDirectoryWithSubDirectory = configuredDatabase.LocalBackupDirectoryWithSubDirectory;
            DirectoryHelper.TestReadWriteAccessToDirectory(Logger, localBackupDirectoryWithSubDirectory);
            DirectoryPath localLocalTransferDirectoryWithSubDirectory = configuredDatabase.LocalLocalTransferDirectoryWithSubDirectory;
            ShareName localShareName = configuredDatabase.LocalShareName;
            try
            {
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(Logger, localLocalTransferDirectoryWithSubDirectory, localShareName);
                CopyFileLocal(fileName, localBackupDirectoryWithSubDirectory, localLocalTransferDirectoryWithSubDirectory);
            }
            catch (Exception e)
            {
                throw new SqlServerMirroringException("Failed moving backup file locally",e);
            }
            try
            {
                UncPath remoteRemoteTransferDirectoryWithSubDirectory = configuredDatabase.RemoteRemoteTransferDirectoryWithSubDirectory;
                UncPath remoteRemoteDeliveryDirectoryWithSubDirectory = configuredDatabase.RemoteRemoteDeliveryDirectoryWithSubDirectory;
                MoveFileLocalToRemote(fileName, localLocalTransferDirectoryWithSubDirectory, remoteRemoteTransferDirectoryWithSubDirectory);
                MoveFileRemoteToRemote(fileName, remoteRemoteTransferDirectoryWithSubDirectory, remoteRemoteDeliveryDirectoryWithSubDirectory);
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
            string fileName;
            try {
                if (MoveRemoteFileToLocalRestoreAndDeleteOtherFiles(configuredDatabase))
                {

                    DirectoryPath localRestoreDircetoryWithSubDirectory = configuredDatabase.LocalRestoreDircetoryWithSubDirectory;

                    fileName = GetNewesteFilename(configuredDatabase.DatabaseName.ToString(), localRestoreDircetoryWithSubDirectory.ToString());

                    // Define a Restore object variable.
                    Restore rs = new Restore();

                    // Set the NoRecovery property to true, so the transactions are not recovered. 
                    rs.NoRecovery = true;
                    rs.ReplaceDatabase = true;

                    // Declare a BackupDeviceItem by supplying the backup device file name in the constructor, and the type of device is a file. 
                    BackupDeviceItem bdi = default(BackupDeviceItem);
                    bdi = new BackupDeviceItem(fileName, DeviceType.File);

                    // Add the device that contains the full database backup to the Restore object. 
                    rs.Devices.Add(bdi);

                    // Specify the database name. 
                    rs.Database = configuredDatabase.DatabaseName.ToString();

                    // Restore the full database backup with no recovery. 
                    rs.SqlRestore(DatabaseServerInstance);

                    // Inform the user that the Full Database Restore is complete. 
                    Console.WriteLine("Full Database Restore complete.");

                    return true;
                }
                else
                {
                    if (DatabaseExists(configuredDatabase.DatabaseName.ToString()))
                    {
                        Logger.LogInfo(string.Format("No backup to restore for {0}.", configuredDatabase.DatabaseName.ToString()));
                        return false;
                    }
                    else
                    {
                        throw new SqlServerMirroringException(string.Format("Could not find database to restore for {0}.", configuredDatabase.DatabaseName.ToString()));
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Restore failed for {0}", configuredDatabase.DatabaseName), ex);
            }
        }

        private bool DatabaseExists(string databaseName)
        {
            Database database = UserDatabases.Where(s => s.Name.Equals(databaseName)).First();
            if(database == null)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        private string GetNewesteFilename(string databaseName, string fullPathString)
        {
            FileInfo result = null;
            var directory = new DirectoryInfo(fullPathString);
            var list = directory.GetFiles("*.bak");
            if (list.Count() > 0)
            {
                result = list.Where(s => s.Name.StartsWith(databaseName)).OrderByDescending(f => f.Name).First();
            }
            if (result != null)
            {
                return result.Name;
            }
            else
            {
                return string.Empty;
            }
        }

        private void DeleteAllFilesExcept(string fileName, string fullPathString)
        {
            List<string> files = Directory.EnumerateFiles(fullPathString).Where(s => s != fileName).ToList();
            files.ForEach(x => { try { System.IO.File.Delete(x); } catch { } });
        }

        /* Create local directories if not existing as this might be first time running and 
        *  returns false if no file is found and true if one is found */
        private bool MoveRemoteFileToLocalRestoreAndDeleteOtherFiles(ConfiguredDatabaseForMirroring configuredDatabase)
        {
            string databaseNameString = configuredDatabase.DatabaseName.ToString();
            UncPath remoteLocalTransferDirectory = configuredDatabase.RemoteLocalTransferDirectoryWithSubDirectory;
            string remoteLocalTransferDirectoryNewestFileName = string.Empty;
            DirectoryPath localRemoteTransferDirectory = configuredDatabase.LocalRemoteTransferDirectoryWithSubDirectory;
            DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, localRemoteTransferDirectory);
            string localRemoteTransferDirectoryNewestFileName = string.Empty;
            DirectoryPath localRemoteDeliveryDirectory = configuredDatabase.LocalRemoteDeliveryDirectoryWithSubDirectory;
            DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, localRemoteDeliveryDirectory);
            string localRemoteDeliveryDirectoryNewestFileName = string.Empty;
            DirectoryPath localRestoreDirectory = configuredDatabase.LocalRestoreDircetoryWithSubDirectory;
            DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, localRestoreDirectory);
            string localRestoreDirectoryNewestFileName = string.Empty;
            try
            {
                remoteLocalTransferDirectoryNewestFileName = GetNewesteFilename(databaseNameString, remoteLocalTransferDirectory.ToString());
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Could not access remote directory {0}.", remoteLocalTransferDirectory.ToString()), ex);
            }
            localRemoteTransferDirectoryNewestFileName = GetNewesteFilename(databaseNameString, localRemoteTransferDirectory.ToString());
            localRemoteDeliveryDirectoryNewestFileName = GetNewesteFilename(databaseNameString, localRemoteDeliveryDirectory.ToString());
            localRestoreDirectoryNewestFileName = GetNewesteFilename(databaseNameString, localRestoreDirectory.ToString());
            long remoteLocalTransferDirectoryNewestValue = GetFileTimePart(remoteLocalTransferDirectoryNewestFileName);
            long localRemoteTransferDirectoryNewestValue = GetFileTimePart(localRemoteTransferDirectoryNewestFileName);
            long localRemoteDeliveryDirectoryNewestValue = GetFileTimePart(localRemoteDeliveryDirectoryNewestFileName);
            long localRestoreDirectoryNewestValue = GetFileTimePart(localRestoreDirectoryNewestFileName);

            /* Add all to array and select max */
            long[] values = new long[] { remoteLocalTransferDirectoryNewestValue, localRemoteTransferDirectoryNewestValue, localRemoteDeliveryDirectoryNewestValue, localRestoreDirectoryNewestValue };
            long maxValue = values.Max();

            if (maxValue == 0)
            {
                /* No files found so none moved */
                return false;
            }
            else
            {
                /* Match for nearest to home first */
                bool found = false;
                #region Local Restore

                if(localRestoreDirectoryNewestValue == maxValue)
                {
                    found = true;
                    Logger.LogInfo(string.Format("Backup file {0} found in {1}.", localRestoreDirectoryNewestFileName, localRestoreDirectory.ToString()));
                    /* delete all files */
                    DeleteAllFilesExcept(localRestoreDirectoryNewestFileName, localRestoreDirectory.ToString());
                    Logger.LogDebug(string.Format("Deleted all files except {0} in {1}.", localRestoreDirectoryNewestFileName, localRestoreDirectory.ToString()));
                    /* No move action needed */
                }
                else if (localRestoreDirectoryNewestValue == 0)
                {
                    Logger.LogDebug(string.Format("No files found in {0}.", localRestoreDirectory.ToString()));
                }
                else
                {
                    /* delete all files */
                    DeleteAllFilesExcept(string.Empty, localRestoreDirectory.ToString());
                    Logger.LogDebug(string.Format("Deleted all files in {0}.", localRestoreDirectory.ToString()));
                }

                #endregion

                #region Local Remote Delivery

                if (!found && localRemoteDeliveryDirectoryNewestValue == maxValue)
                {
                    found = true;
                    Logger.LogInfo(string.Format("Backup file {0} found in {1}.", localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory.ToString()));
                    /* delete all files */
                    DeleteAllFilesExcept(localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory.ToString());
                    Logger.LogDebug(string.Format("Deleted all files except {0} in {1}.", localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory.ToString()));
                    /* Move actions needed */
                    MoveFileLocal(localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory, localRestoreDirectory);
                }
                else if (localRemoteDeliveryDirectoryNewestValue == 0)
                {
                    Logger.LogDebug(string.Format("No files found in {0}.", localRemoteDeliveryDirectory.ToString()));
                }
                else
                {
                    /* delete all files */
                    DeleteAllFilesExcept(string.Empty, localRemoteDeliveryDirectory.ToString());
                    Logger.LogDebug(string.Format("Deleted all files in {0}.", localRemoteDeliveryDirectory.ToString()));
                }

                #endregion

                #region Local Remote Transfer

                if (!found && localRemoteTransferDirectoryNewestValue == maxValue)
                {
                    found = true;
                    Logger.LogInfo(string.Format("Backup file {0} found in {1}.", localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory.ToString()));
                    /* delete all files */
                    DeleteAllFilesExcept(localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory.ToString());
                    Logger.LogDebug(string.Format("Deleted all files except {0} in {1}.", localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory.ToString()));
                    /* Move actions needed */
                    MoveFileLocal(localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory, localRemoteDeliveryDirectory);
                    MoveFileLocal(localRemoteTransferDirectoryNewestFileName, localRemoteDeliveryDirectory, localRestoreDirectory);
                }
                else if (localRemoteDeliveryDirectoryNewestValue == 0)
                {
                    Logger.LogDebug(string.Format("No files found in {0}.", localRemoteTransferDirectory.ToString()));
                }
                else
                {
                    /* delete all files */
                    DeleteAllFilesExcept(string.Empty, localRemoteTransferDirectory.ToString());
                    Logger.LogDebug(string.Format("Deleted all files in {0}.", localRemoteTransferDirectory.ToString()));
                }

                #endregion

                #region Remote Local Transfer

                if (!found && remoteLocalTransferDirectoryNewestValue == maxValue)
                {
                    found = true;
                    Logger.LogInfo(string.Format("Backup file {0} found in {1}.", remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory.ToString()));
                    /* delete all files */
                    DeleteAllFilesExcept(remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory.ToString());
                    Logger.LogDebug(string.Format("Deleted all files except {0} in {1}.", remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory.ToString()));
                    /* Move actions needed */
                    MoveFileRemoteToLocal(remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory, localRemoteTransferDirectory);
                    MoveFileLocal(remoteLocalTransferDirectoryNewestFileName, localRemoteTransferDirectory, localRemoteDeliveryDirectory);
                    MoveFileLocal(remoteLocalTransferDirectoryNewestFileName, localRemoteDeliveryDirectory, localRestoreDirectory);
                }
                else if (localRemoteDeliveryDirectoryNewestValue == 0)
                {
                    Logger.LogDebug(string.Format("No files found in {0}.", remoteLocalTransferDirectory.ToString()));
                }
                else
                {
                    /* delete all files */
                    DeleteAllFilesExcept(string.Empty, remoteLocalTransferDirectory.ToString());
                    Logger.LogDebug(string.Format("Deleted all files in {0}.", localRemoteTransferDirectory.ToString()));
                }
                #endregion

                return true;
            }
        }

        private void MoveFileLocal(string fileName, DirectoryPath source, DirectoryPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Trying to move file {0} from {1} to {2}.", fileName, source, destination));
                File.Move(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Moved file {0} from {1} to {2}.", fileName, source, destination));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Failed moving file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private void MoveFileRemoteToLocal(string fileName, UncPath source, DirectoryPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Trying to move file {0} from {1} to {2}.", fileName, source, destination));
                File.Move(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Moved file {0} from {1} to {2}.", fileName, source, destination));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Failed moving file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private void MoveFileLocalToRemote(string fileName, DirectoryPath source, UncPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Trying to move file {0} from {1} to {2}.", fileName, source, destination));
                File.Move(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Moved file {0} from {1} to {2}.", fileName, source, destination));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Failed moving file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private void MoveFileRemoteToRemote(string fileName, UncPath source, UncPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Trying to move file {0} from {1} to {2}.", fileName, source, destination));
                File.Move(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Moved file {0} from {1} to {2}.", fileName, source, destination));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Failed moving file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private void CopyFileLocal(string fileName, DirectoryPath source, DirectoryPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Trying to copy file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()));
                File.Move(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Copied file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Failed copying file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private long GetFileTimePart(string fileName)
        {
            if(string.IsNullOrWhiteSpace(fileName))
            {
                return 0;
            }
            Regex regex = new Regex(@"^(?:[\w_][\w_\d]*_)(\d*)(?:\.bak)$");
            Match match = regex.Match(fileName);
            if(match.Success)
            {
                string capture = match.Value;
                long returnValue;
                if(long.TryParse(capture, out returnValue))
                {
                    return returnValue;
                }
            }
            throw new SqlServerMirroringException(string.Format("GetFileTimePart failed to extract time part from {0}.", fileName));
        }

        public bool ResumeMirroringForAllDatabases(Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredDatabases)
        {
            foreach (ConfiguredDatabaseForMirroring configuredDatabase in configuredDatabases.Values)
            {
                try
                {
                    Database database = UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName)).First();

                    database.ChangeMirroringState(MirroringOption.Resume);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Resume failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            return true;
        }

        public bool SuspendMirroringForAllMirrorDatabases(Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredDatabases)
        {
            foreach(ConfiguredDatabaseForMirroring configuredDatabase in configuredDatabases.Values)
            {
                try
                {
                    Database database = UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName)).First();

                    database.ChangeMirroringState(MirroringOption.Suspend);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Suspend failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            return true;
        }

        public bool ForceFailoverWithDataLossForAllMirrorDatabases(Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredDatabases)
        {
            /* TODO Only fail over the first as all others will join if on multi mirror server with witness */
            foreach (ConfiguredDatabaseForMirroring configuredDatabase in configuredDatabases.Values)
            {
                try
                {
                    Database database = UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName)).First();

                    database.ChangeMirroringState(MirroringOption.ForceFailoverAndAllowDataLoss);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("ForceFailoverWithDataLoss failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            return true;
        }

        public bool FailoverForAllMirrorDatabases(Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredDatabases)
        {
            /* TODO Only fail over the fist as all others will join */
            foreach (ConfiguredDatabaseForMirroring configuredDatabase in configuredDatabases.Values)
            {
                try
                {
                    Database database = UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName)).First();
                        database.ChangeMirroringState(MirroringOption.Failover);
                        database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Failover failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            return true;
        }

        public bool BackupForAllMirrorDatabases(Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredDatabases)
        {
            foreach (ConfiguredDatabaseForMirroring configuredDatabase in configuredDatabases.Values)
            {
                try
                {
                    BackupDatabase(configuredDatabase);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Backup failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            return true;
        }

        #endregion
    }
}
