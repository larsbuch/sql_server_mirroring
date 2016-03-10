using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using System.Data.SqlClient;
using HelperFunctions;

namespace SqlServerMirroring
{
    public class SqlServerInstance
    {
        private Server _server;
        private string _connectionString;
        private ILogger _logger;

        public SqlServerInstance(ILogger logger, string connectionString)
        {
            ValidateConnectionStringAndDatabaseConnection(connectionString);
            _server = new Server(new ServerConnection(new SqlConnection(connectionString)));
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

        private Server ServerInstance
        {
            get
            {
                return _server;
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
            return ServerInstance.Status.ToString();
        }

        public List<string> Instance_Endpoints()
        {
            List<string> returnList = new List<string>();
            foreach(Endpoint endpoint in ServerInstance.Endpoints)
            {
                returnList.Add(endpoint.ToString());
            }
            return returnList;
        }

        public Dictionary<string, string> Instance_Information()
        {
            Dictionary<string, string> serverInformation = new Dictionary<string, string>();
            serverInformation.Add("Instance Backup Directory", ServerInstance.BackupDirectory);
            serverInformation.Add("Instance Build Number", ServerInstance.BuildNumber.ToString());
            serverInformation.Add("Instance Collation", ServerInstance.Collation);
            serverInformation.Add("Instance Edition", ServerInstance.Edition);
            serverInformation.Add("Instance Engine Edition", ServerInstance.EngineEdition.ToString());
            serverInformation.Add("Instance Error Log Path", ServerInstance.ErrorLogPath);
            serverInformation.Add("Instance Sql Server Agent Service Start Mode", ServerInstance.JobServer.ServiceStartMode.ToString());
            serverInformation.Add("Instance Sql Server Agent Service Account", ServerInstance.JobServer.ServiceAccount);
            serverInformation.Add("Instance Language", ServerInstance.Language);
            serverInformation.Add("Instance Physical Machine Name", ServerInstance.Name);
            serverInformation.Add("Instance Named Pipes Enabled", ServerInstance.NamedPipesEnabled ? "Yes" : "No");
            serverInformation.Add("Instance OS Version", ServerInstance.OSVersion);
            serverInformation.Add("Instance Platform", ServerInstance.Platform);
            serverInformation.Add("Instance Product", ServerInstance.Product);
            serverInformation.Add("Instance Product Level", ServerInstance.ProductLevel);
            serverInformation.Add("Instance Service Start Mode", ServerInstance.ServiceStartMode.ToString());
            serverInformation.Add("Instance Tcp Enabled", ServerInstance.TcpEnabled ? "Yes" : "No");
            serverInformation.Add("Instance Server Type", ServerInstance.ServerType.ToString());
            serverInformation.Add("Instance Service Account", ServerInstance.ServiceAccount);
            serverInformation.Add("Instance Version", ServerInstance.Version.ToString());

            return serverInformation;
        }

        public bool WindowsAuthentificationActive()
        {
            if(ServerInstance.LoginMode == ServerLoginMode.Integrated || ServerInstance.LoginMode == ServerLoginMode.Mixed)
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
            if (ServerInstance.LoginMode == ServerLoginMode.Normal || ServerInstance.LoginMode == ServerLoginMode.Mixed)
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
        public void SetupInstanceForMirroring()
        {
            // ServiceBroker

        }

        public IEnumerable<Database> UserDatabases
        {
            get
            {
                foreach (Database database in ServerInstance.Databases)
                {
                    if (!database.IsSystemObject)
                    {
                        yield return database;
                    }
                }
            }
        }


        public void StartUpMirrorCheck(List<string> configuredMirrorDatabases, bool serverPrincipal)
        {
            foreach(MirrorState mirrorState in DatabaseMirrorStates)
            {
                if(mirrorState.IsConfiguredForMirroring)
                {
                    if(!configuredMirrorDatabases.Contains(mirrorState.DatabaseName))
                    {
                        Logger.LogWarning(string.Format("Database {0} was set up for mirroring but is not in configuration", mirrorState.DatabaseName));
                        RemoveDatabaseFromMirroring(mirrorState.DatabaseName, serverPrincipal);
                    }
                }
                else
                {
                    if(configuredMirrorDatabases.Contains(mirrorState.DatabaseName))
                    {
                        Logger.LogWarning(string.Format("Database {0} is not set up for mirroring but is in configuration", mirrorState.DatabaseName));
                        AddDatabaseToMirroring(mirrorState.DatabaseName, serverPrincipal);
                    }
                }
            }
        }

        private void AddDatabaseToMirroring(string databaseName, bool serverPrincipal)
        {
            throw new NotImplementedException();
        }

        private void RemoveDatabaseFromMirroring(string databaseName, bool serverPrincipal)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Individual Databases

        public void SetupPrincipalDatabases(List<string> databasesToBeMirrored)
        {
            SetupDatabasesForMirroring(databasesToBeMirrored);
            // Setup mirroring with MirrorDatabase
            // backup database and move to shared drive on remote machine if possible
        }
        public void SetupSecondaryDatabases(List<string> databasesToBeMirrored)
        {
            SetupDatabasesForMirroring(databasesToBeMirrored);
            // Setup mirroring with MirrorDatabase (Database.ChangeMirroringState)
            // restore database
        }

        private void SetupDatabasesForMirroring(List<string> databasesToBeMirrored)
        {
            foreach(string databaseName in databasesToBeMirrored)
            {

            }
            // Full recovery
            // EndPoints Needed for each mirror database ?
        }

        public void RemoveDatabasesFromMirroring(List<string> mirroredDatabasesToBeRemovedFromMirroring)
        {

        }

        // Use MirrorState to find the existing state
        public bool CheckMirrorStateForChangesNeeded(List<string> databasesToBeChecked)
        {
            //SMO: Database.MirroringStatus
            throw new NotImplementedException();
        }

        // Setup Backup with BackupDatabase (daily)
        public bool BackupDatabases(List<string> databasesToBeBackedUp, string localDriveForBackup)
        {
            throw new NotImplementedException();
        }

        // Setup Backup with BackupDatabase (responsible for moving database backup to remote share)
        public bool BackupDatabases(List<string> databasesToBeBackedUp, string localDriveForBackup, string remoteTempFolderForBackup, string remoteDeliveryFolderForBackup)
        {
            throw new NotImplementedException();
        }

        // Setup Restore with RestoreDatabase (responsible for restoring database)
        public bool RestoreDatabases(List<string> databasesToBeRestored, string localDriveForRestore)
        {
            throw new NotImplementedException();
        }



        #endregion
    }
}
