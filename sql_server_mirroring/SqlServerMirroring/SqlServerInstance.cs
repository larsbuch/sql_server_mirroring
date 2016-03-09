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
        private ServerConnection _serverConnection;
        private Server _server;
        private SqlConnection _sqlConnection;
        private string _connectionString;

        public SqlServerInstance(ILogger logger, string connectionString)
        {
            ValidateConnectionString(connectionString);
            _sqlConnection = new SqlConnection(connectionString);
            _serverConnection = new ServerConnection(_sqlConnection);
            _server = new Server(_serverConnection);
        }

        private void ValidateConnectionString(string connectionString)
        {
            try
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                }
                    }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format(""), ex);
            }
        }

        #region Properties

        private Server ServerInstance
        {
            get
            {
                switch(_server.Status)
                {
                    case ServerStatus.Online:
                        // Handle Online
                        return _server;
                    default:
                        // Handle other states
                        throw new SqlServerMirroringException(string.Format("Server in {0} state.", _server.Status.ToString()));
                }
            }
        }

        #endregion


        public bool MixedMode()
        {
            if(ServerInstance.LoginMode = ServerLoginMode.
        }

        // MixedMode
        // Databases
        // ServiceBroker
        // EndPoints
        // Setup mirroring with MirrorDatabase
        // Use MirrorState to find the existing state
        // Setup Backup with BackupDatabase (responsible for moving database backup to remote share)
        // Setup Backup with BackupDatabase (daily)
        // Setup Restore with RestoreDatabase (responsible for restoruing 
    }
}
