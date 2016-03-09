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
                throw new SqlServerMirroringException(string.Format("Database instance connected with connection string |{0}| failed.", connectionString), ex);
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
        }

        // Setup Backup with BackupDatabase (daily)
        public bool BackupDatabases(List<string> databasesToBeBackedUp, string localDriveForBackup)
        {

        }

        // Setup Backup with BackupDatabase (responsible for moving database backup to remote share)
        public bool BackupDatabases(List<string> databasesToBeBackedUp, string localDriveForBackup, string remoteTempFolderForBackup, string remoteDeliveryFolderForBackup)
        {

        }

        // Setup Restore with RestoreDatabase (responsible for restoring database)
        public bool RestoreDatabases(List<string> databasesToBeRestored, string localDriveForRestore)
        {

        }



        #endregion
    }
}
