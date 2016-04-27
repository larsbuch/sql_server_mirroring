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
using System.Data;
using System.Timers;

namespace MirrorLib
{
    public class SqlServerInstance
    {
        private const string DIRECTORY_SPLITTER = "\\";
        private const string MASTER_DATABASE = "master";

        private Server _server;
        private Server _remoteServer;
        private SqlServerLogger _logger;
        private ManagedComputer _managedComputer;
        private ServerStateMonitor _serverStateMonitor;
        private ConfigurationForInstance _instance_Configuration;
        private ServerRoleEnum _activeServerRole = ServerRoleEnum.NotSet;
        private Dictionary<string, ConfigurationForDatabase> _databases_Configuration;
        private Dictionary<string,DatabaseMirrorState> _databaseMirrorStates;
        private Dictionary<string, DatabaseState> _databaseStates;
        private Timer _timedCheckTimer;
        private Timer _backupTimer;
        private SqlServerInstanceSynchronizeInvoke _synchronizeInvoke;
        private bool _remoteServer_HasAccess;
        private Table _localServerStateTable;


        public SqlServerInstance(ILogger logger, string serverName, ConfigurationForInstance instanceConfiguration, Dictionary<string, ConfigurationForDatabase> databasesConfiguration)
        {
            _logger = new SqlServerLogger(logger);
            SqlConnectionStringBuilder connectionBuilder = new SqlConnectionStringBuilder();
            connectionBuilder.DataSource = serverName;
            connectionBuilder.MultipleActiveResultSets = true;
            connectionBuilder.IntegratedSecurity = true;

            _server = new Server(new ServerConnection(new SqlConnection(connectionBuilder.ToString())));

            Instance_Configuration = instanceConfiguration;
            Databases_Configuration = databasesConfiguration;
            _managedComputer = new ManagedComputer("(local)");
            _synchronizeInvoke = new SqlServerInstanceSynchronizeInvoke();

            /* Starts heartbeat */
            _serverStateMonitor = new ServerStateMonitor(this);
        }

        #region Public Properties

        public bool Information_RemoteServer_HasAccess()
        {
            return _remoteServer_HasAccess;
        }

        private void Action_RemoteServer_CheckAccess()
        {
            try
            {
                SqlConnectionStringBuilder connectionBuilder = new SqlConnectionStringBuilder();
                connectionBuilder.DataSource = Instance_Configuration.RemoteServer.ToString();
                connectionBuilder.MultipleActiveResultSets = true;
                connectionBuilder.IntegratedSecurity = true;
                connectionBuilder.ConnectTimeout = Instance_Configuration.RemoteServerAccessTimeoutSeconds;

                SqlConnection sqlConnection = new SqlConnection(connectionBuilder.ToString());

                sqlConnection.Open();
                Logger.LogDebug("Access to remote server.");
                _remoteServer_HasAccess = true;
            }
            catch (Exception)
            {
                Logger.LogInfo("No access to remote server.");
                _remoteServer_HasAccess = false;
            }
        }

        public ServerState Information_ServerState
        {
            get
            {
                if(_activeServerRole == ServerRoleEnum.NotSet)
                {
                    Action_ServerRole_Recheck();
                }
                return ServerStateMonitor.ServerState_Active;
            }
        }

        public ServerStateMonitor ServerStateMonitor
        {
            get
            {
                return _serverStateMonitor;
            }
        }


        private Table LocalServerStateTable
        {
            get
            {
                return _localServerStateTable;
            }
            set
            {
                _localServerStateTable = value;
            }
        }

        public Dictionary<string, ConfigurationForDatabase> Databases_Configuration
        {
            get
            {
                return _databases_Configuration;
            }
            private set
            {
                _databases_Configuration = value;
            }
        }

        public ConfigurationForInstance Instance_Configuration
        {
            get
            {
                return _instance_Configuration;
            }
            private set
            {
                _instance_Configuration = value;
            }
        }

        public IEnumerable<Database> Information_UserDatabases
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

        public bool Information_Instance_IsInDegradedState
        {
            get
            {
                // TODO should probably also check if databases are in correct state
                return ServerStateMonitor.IsInDegradedState;
            }
        }

        public ServerRoleEnum Information_Instance_ServerRole
        {
            get
            {
                return _activeServerRole;
            }
            set
            {
                _activeServerRole = value;
            }
        }

        #endregion

        #region Private Properties

        internal bool Information_Instance_ConfigurationComplete
        {
            get
            {
                if (Instance_Configuration == null || Databases_Configuration == null || Databases_Configuration.Count == 0)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
        }

        private Timer Information_ServerState_TimedCheckTimer
        {
            get
            {
                return _timedCheckTimer;
            }
        }

        private Timer Information_ServerState_BackupTimer
        {
            get
            {
                return _backupTimer;
            }
        }

        private Server DatabaseServerInstance
        {
            get
            {
                return _server;
            }
        }

        private Server RemoteDatabaseServerInstance
        {
            get
            {
                if(_remoteServer == null)
                {
                    string remoteServerName = Instance_Configuration.RemoteServer.ToString();
                    Logger.LogDebug(string.Format("Trying to connect to remote server {0}", remoteServerName));
                    /* Do not validate connection as server might not be installed */
                    SqlConnectionStringBuilder connectionBuilder = new SqlConnectionStringBuilder();
                    connectionBuilder.ConnectTimeout = Instance_Configuration.RemoteServerAccessTimeoutSeconds;
                    connectionBuilder.DataSource = remoteServerName;
                    connectionBuilder.MultipleActiveResultSets = true;
                    connectionBuilder.IntegratedSecurity = true;
                    _remoteServer = new Server(new ServerConnection(new SqlConnection(connectionBuilder.ToString())));
                }
                if(!Information_RemoteServer_HasAccess())
                {
                    Logger.LogWarning("Trying to access without ability to connect.");
                }
                return _remoteServer;
            }
        }

        private ManagedComputer ComputerInstance
        {
            get
            {
                return _managedComputer;
            }
        }

        internal SqlServerLogger Logger
        {
            get
            {
                return _logger;
            }
        }

        private Database RemoteMasterDatabase
        {
            get
            {
                if(Information_RemoteServer_HasAccess())
                {
                    // TODO Caching
                    foreach ( Database database in RemoteDatabaseServerInstance.Databases)
                    {
                        if(database.Name.Equals(MASTER_DATABASE))
                        {
                            Logger.LogDebug("Accessing Remote Master database");
                            return database;
                        }
                    }
                }
                else
                {
                    Logger.LogError("RemoteMasterDatabase: Trying to access without ability to connect to server");
                }
                return null;
            }
        }

        private Database LocalMasterDatabase
        {
            get
            {
                // TODO Caching
                foreach (Database database in DatabaseServerInstance.Databases)
                {
                    if (database.Name.Equals(MASTER_DATABASE))
                    {
                        Logger.LogDebug("Accessing Local Master database");
                        return database;
                    }
                }
                return null;
            }
        }

        private IEnumerable<Service> Information_SqlServerServicesInstalled
        {
            get
            {
                foreach (Service service in ComputerInstance.Services)
                {
                    yield return service;
                }
            }
        }

        private Dictionary<string,DatabaseMirrorState> Information_DatabaseMirrorStates
        {
            get
            {
                return _databaseMirrorStates;
            }
            set
            {
                _databaseMirrorStates = value;
            }
        }

        private Dictionary<string, DatabaseState> Information_DatabaseStates
        {
            get
            {
                return _databaseStates;
            }
            set
            {
                _databaseStates = value;
            }
        }

        #endregion

        #region Public Information Instance methods

        public string Information_Instance_Status()
        {
            return DatabaseServerInstance.Status.ToString();
        }

        public string Information_RemoteServer_Status()
        {
            return RemoteDatabaseServerInstance.Status.ToString();
        }

        public Dictionary<string, string> Information_Instance()
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

        //public Dictionary<string, string> Information_SqlAgent()
        //{
        //    Dictionary<string, string> sqlAgentInformation = new Dictionary<string, string>();
        //    Action_SqlAgent_EnableAgentXps();
        //    try
        //    {
        //        sqlAgentInformation.Add("Sql Server Agent Auto Start", DatabaseServerInstance.JobServer.SqlAgentAutoStart ? "Yes" : "No");
        //        sqlAgentInformation.Add("Sql Server Agent Error Log", DatabaseServerInstance.JobServer.ErrorLogFile);
        //        sqlAgentInformation.Add("Sql Server Agent Log Level", DatabaseServerInstance.JobServer.AgentLogLevel.ToString());
        //        sqlAgentInformation.Add("Sql Server Agent Service Start Mode", DatabaseServerInstance.JobServer.ServiceStartMode.ToString());
        //        sqlAgentInformation.Add("Sql Server Agent Service Account", DatabaseServerInstance.JobServer.ServiceAccount);
        //    }
        //    catch (ExecutionFailureException efe)
        //    {
        //        if (efe.InnerException.Message.StartsWith("SQL Server blocked access to procedure 'dbo.sp_get_sqlagent_properties' of component 'Agent XPs'"))
        //        {
        //            throw new SqlServerMirroringException("'Agent XPs' are disabled in Sql Server seems to be disabled and cannot be started", efe);
        //        }
        //        else
        //        {
        //            throw;
        //        }
        //    }
        //    return sqlAgentInformation;
        //}

        public bool Information_Instance_WindowsAuthentificationActive()
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

        public bool Information_Instance_SqlServerAuthentificationActive()
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

        public bool Information_Instance_CheckForMirroring()
        {
            //Action_SqlAgent_EnableAgentXps();
            if (DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Manual ||
                DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Disabled)
            {
                Logger.LogWarning(string.Format("Sql Server service should be configured to start automatically. It is set to {0}", DatabaseServerInstance.ServiceStartMode.ToString()));
                return false;
            }
            //if (!DatabaseServerInstance.JobServer.SqlAgentAutoStart)
            //{
            //    Logger.LogWarning("Sql Agent not configured to auto start on server restart");
            //    return false;
            //}
            //if (!Information_SqlAgent_CheckRunning())
            //{
            //    Logger.LogWarning("Sql Server Agent service is not running");
            //    return false;
            //}
            if (!Action_IO_TestReadWriteAccessToDirectory(Instance_Configuration.LocalBackupDirectory))
            {
                Logger.LogWarning(string.Format("Not full access to LocalBackupDirectory: {0}", Instance_Configuration.LocalBackupDirectory));
                return false;
            }
            if (!Action_IO_TestReadWriteAccessToDirectory(Instance_Configuration.LocalRestoreDirectory))
            {
                Logger.LogWarning(string.Format("Not full access to LocalRestoreDirectory: {0}", Instance_Configuration.LocalRestoreDirectory));
                return false;
            }
            if (!Action_IO_TestReadWriteAccessToDirectory(Instance_Configuration.LocalShareDirectory))
            {
                Logger.LogWarning(string.Format("Not full access to LocalShareDirectory: {0}", Instance_Configuration.LocalShareDirectory));
                return false;
            }
            if(Information_Endpoint_Exists(Instance_Configuration.Endpoint_Name))
            {
                if(!Information_Endpoint_Started(Instance_Configuration.Endpoint_Name))
                {
                    Logger.LogWarning(string.Format("Endpoint {0} for server not started", Instance_Configuration.Endpoint_Name));
                    return false;
                }
            }
            else
            {
                Logger.LogWarning(string.Format("Endpoint {0} for server does not exist", Instance_Configuration.Endpoint_Name));
                return false;
            }
            if (!Information_Endpoint_HasConnectRights(Instance_Configuration.Endpoint_Name))
            {
                Logger.LogWarning(string.Format("Application is not allowed to connect to Endpoint {0}", Instance_Configuration.Endpoint_Name));
                return false;
            }
            return true;
        }

        #endregion

        #region Public Action Instance methods

        public void Action_Instance_StartPrimary()
        {
            Logger.LogDebug("Started");
            ServerStateMonitor.StartPrimary();
            Logger.LogDebug("Ended");
        }

        public void Action_Instance_StartSecondary()
        {
            Logger.LogDebug("Started");
            ServerStateMonitor.StartSecondary();
            Logger.LogDebug("Ended");
        }

        public void Action_Instance_SetupMonitoring()
        {
            Logger.LogDebug("Started");
            try
            {
                LocalMasterDatabase.ExecuteNonQuery(string.Format("EXEC sys.sp_dbmmonitoraddmonitoring {0}", Instance_Configuration.MirrorMonitoringUpdateMinutes));
                Logger.LogDebug(string.Format("Mirroring monitoring every {0} minutes started with partner endpoint on {1} port {2}"
                    , Instance_Configuration.MirrorMonitoringUpdateMinutes, Instance_Configuration.RemoteServer, Instance_Configuration.Endpoint_ListenerPort));
            }
            catch (Exception ex)
            {
                Logger.LogError("Adding monitoring failed but it might be that is was just done before so it is ignored", ex);
            }

            try
            {
                foreach (Database database in Information_UserDatabases)
                {
                    ConfigurationForDatabase configuredDatabase;
                    if (Databases_Configuration.TryGetValue(database.Name, out configuredDatabase))
                    {

                        //1 Oldest unsent transaction
                        //Specifies the number of minutes worth of transactions that can accumulate in the send queue
                        //before a warning is generated on the principal server instance.This warning helps measure the
                        //potential for data loss in terms of time, and it is particularly relevant for high - performance mode.
                        //However, the warning is also relevant for high - safety mode when mirroring is paused or suspended because the
                        //partners become disconnected.

                        //--Event ID 32042
                        //EXEC sp_dbmmonitorchangealert DatabaseName, 1, 2, 1 ; --OLDEST UNSENT TRANSACTION (set to 2 minutes)
                        database.ExecuteNonQuery(string.Format("EXEC sys.sp_dbmmonitorchangealert {0}, 1, 2, 1", database.Name));
                        Logger.LogDebug(string.Format("sp_dbmmonitorchangealert OLDEST UNSENT TRANSACTION used for {0}", database.Name));


                        //2 Unsent log
                        //Specifies how many kilobytes(KB) of unsent log generate a warning on the principal server instance.This warning
                        //helps measure the potential for data loss in terms of KB, and it is particularly relevant for high - performance mode.
                        //However, the warning is also relevant for high - safety mode when mirroring is paused or suspended because the partners
                        //become disconnected.

                        //--Event ID 32043
                        //EXEC sp_dbmmonitorchangealert DatabaseName, 2, 10000, 1; --UNSENT LOG SIZE ON P(set to 10000K)
                        database.ExecuteNonQuery(string.Format("EXEC sys.sp_dbmmonitorchangealert {0}, 2, 10000, 1", database.Name));
                        Logger.LogDebug(string.Format("sp_dbmmonitorchangealert UNSENT LOG SIZE ON PRINCIPAL used for {0}", database.Name));

                        //3 Unrestored log
                        //Specifies how many KB of unrestored log generate a warning on the mirror server instance.This warning helps measure
                        //failover time.Failover time consists mainly of the time that the former mirror server requires to roll forward any log
                        //remaining in its redo queue, plus a short additional time.

                        //--Event ID 32044
                        //EXEC sp_dbmmonitorchangealert DatabaseName, 3, 10000, 1; --UNRESTORED LOG ON M(set to 10000K)
                        database.ExecuteNonQuery(string.Format("EXEC sys.sp_dbmmonitorchangealert {0}, 3, 10000, 1", database.Name));
                        Logger.LogDebug(string.Format("sp_dbmmonitorchangealert UNRESTORED LOG ON MIRROR used for {0}", database.Name));

                        //4 Mirror commit overhead
                        //Specifies the number of milliseconds of average delay per transaction that are tolerated before a warning is generated
                        //on the principal server.This delay is the amount of overhead incurred while the principal server instance waits for the
                        //mirror server instance to write the transaction's log record into the redo queue. This value is relevant only in high-safety mode.

                        //-- Event ID 32045
                        //EXEC sp_dbmmonitorchangealert DatabaseName, 4, 1000, 1; --MIRRORING COMMIT OVERHEAD (milisecond delay for txn on P_
                        database.ExecuteNonQuery(string.Format("EXEC sys.sp_dbmmonitorchangealert {0}, 4, 1000, 1", database.Name));
                        Logger.LogDebug(string.Format("sp_dbmmonitorchangealert MIRRORING COMMIT OVERHEAD used for {0}", database.Name));

                        //5 Retention period
                        //Metadata that controls how long rows in the database mirroring status table are preserved.
                        //EXEC sp_dbmmonitorchangealert DatabaseName, 5, 48, 1 ;
                        database.ExecuteNonQuery(string.Format("EXEC sys.sp_dbmmonitorchangealert {0}, 5, 48, 1", database.Name));
                        Logger.LogDebug(string.Format("sp_dbmmonitorchangealert DATABASE MIRRORING STATUS RETENTION PERIOD used for {0}", database.Name));
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
            Logger.LogDebug("Ended");
        }

        internal bool Information_RemoteServer_SecondaryRestoreFinished()
        {
            return Information_RemoteServer_CheckLocalWrittenState(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE);
        }

        public void Action_ServerState_TimedCheck()
        {
            Logger.LogDebug("Started");

            Information_ServerState_TimedCheckTimer.Stop();
            Logger.LogDebug("TimedCheckTimer stopped");
            try
            {
                Action_RemoteServer_CheckAccess();
                Action_Instance_CheckDatabaseMirrorStates();
                if (Information_ServerState.MasterDatabaseTablesSafeToAccess)
                {
                    Logger.LogDebug("Server not in Not Set, Initial and Configuration states so it is safe to look at database tables");
                    Action_Instance_CheckDatabaseStates();
                    Action_DatabaseState_TimedCheck();
                }
                Action_ServerRole_Recheck();
                Action_ServerStateMonitor_TimedCheck();
                Action_Instance_SwitchOverCheck();
            }
            finally
            {
                Information_ServerState_TimedCheckTimer.Start();
                Logger.LogDebug("TimedCheckTimer restarted");
                Logger.LogDebug("Ended");
            }
        }

        private void Action_ServerStateMonitor_TimedCheck()
        {
            ServerStateMonitor.TimedCheck();
        }

        private void Action_DatabaseState_TimedCheck()
        {
            bool databaseErrorState = false;
            foreach (DatabaseMirrorState databaseMirrorState in Information_DatabaseMirrorStates.Values.Where(s => s.MirroringState != MirroringStateEnum.NotMirrored))
            {
                if (Information_Instance_ServerRole == ServerRoleEnum.Primary)
                {
                    if (databaseMirrorState.DatabaseState != DatabaseStateEnum.ONLINE)
                    {
                        databaseErrorState = true;
                        Logger.LogError(string.Format("Server Role {0}: Database {1} has error status {2}."
                            , Information_Instance_ServerRole, databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState));
                        Action_DatabaseState_Update(databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState, Information_Instance_ServerRole, true, 1);
                    }
                    else
                    {
                        Logger.LogDebug(string.Format("Server Role {0}: Database {1} has status {2}."
                            , Information_Instance_ServerRole, databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState));
                        Action_DatabaseState_Update(databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState, Information_Instance_ServerRole, false, 0);
                    }
                }
                else if (Information_Instance_ServerRole == ServerRoleEnum.Secondary)
                {
                    if (databaseMirrorState.DatabaseState != DatabaseStateEnum.RESTORING)
                    {
                        databaseErrorState = true;
                        Logger.LogError(string.Format("Server Role {0}: Database {1} has error status {2}."
                            , Information_Instance_ServerRole, databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState));
                        Action_DatabaseState_Update(databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState, Information_Instance_ServerRole, true, 1);
                    }
                    else
                    {
                        Logger.LogDebug(string.Format("Server Role {0}: Database {1} has status {2}."
                            , Information_Instance_ServerRole, databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState));
                        Action_DatabaseState_Update(databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState, Information_Instance_ServerRole, false, 0);
                    }
                }
                else
                {
                    databaseErrorState = true;
                    Logger.LogError(string.Format("Server Role {0}", Information_Instance_ServerRole));
                    Action_DatabaseState_Update(databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState, Information_Instance_ServerRole, true, 1);
                }
            }
            if (databaseErrorState)
            {
                /* Check last state and state count */
                if (Action_DatabaseState_ShiftState())
                {
                    Action_Instance_ForceShutDownMirroringService();
                }
                else
                {
                    Logger.LogDebug(string.Format("Found Server Role {0}, Server State {1} and will shut down after {2} checks."
                        , Information_Instance_ServerRole, Information_ServerState, Instance_Configuration.ShutDownAfterNumberOfChecksForDatabaseState));
                }
            }
            else
            {
                Logger.LogDebug(string.Format("Found Server Role {0} and Server State {1}."
                    , Information_Instance_ServerRole, Information_ServerState));
            }
        }

        public bool Action_Instance_ResumeMirroringForAllDatabases()
        {
            Logger.LogDebug("Started");
            foreach (DatabaseMirrorState databaseMirrorState in Information_DatabaseMirrorStates.Values)
            {
                if (databaseMirrorState.MirroringState == MirroringStateEnum.DisconnectedFromOtherPartner
                    || databaseMirrorState.MirroringState == MirroringStateEnum.Suspended)
                {
                    try
                    {
                        //SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE // TODO Check for state

                        Logger.LogDebug(string.Format("Trying to resume mirroring for {0}"
                            , databaseMirrorState.DatabaseName));
                        string sqlQuery = string.Format("ALTER DATABASE {0} SET PARTNER RESUME", databaseMirrorState.DatabaseName);
                        Logger.LogDebug(string.Format("Run sql query {0}"
                            , sqlQuery));

                        LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                        Logger.LogInfo(string.Format("Resumed mirroring for {0}"
                            , databaseMirrorState.DatabaseName));
                    }
                    catch (Exception ex)
                    {
                        throw new SqlServerMirroringException(string.Format("Resume failed for {0}", databaseMirrorState.DatabaseName), ex);
                    }
                }
            }
            Logger.LogDebug("Ended");
            return true;
        }

        internal bool Information_RemoteServer_BackupFinished()
        {
            return Information_RemoteServer_CheckLocalWrittenState(ServerStateEnum.PRIMARY_CONFIGURATION_WAITING_FOR_SECONDARY_RESTORE_STATE);
        }

        public void Action_Instance_SuspendMirroringForAllMirrorDatabases()
        {
            Logger.LogDebug("Started");

            foreach (DatabaseMirrorState databaseMirrorState in Information_DatabaseMirrorStates.Values)
            {
                if (databaseMirrorState.MirroringState != MirroringStateEnum.DisconnectedFromOtherPartner
                    && databaseMirrorState.MirroringState != MirroringStateEnum.Suspended)
                {
                    try
                    {
                        Logger.LogDebug(string.Format("Trying to suspend mirroring for {0}"
                            , databaseMirrorState.DatabaseName));
                        string sqlQuery = string.Format("ALTER DATABASE {0} SET PARTNER SUSPEND", databaseMirrorState.DatabaseName);
                        Logger.LogDebug(string.Format("Run sql query {0}"
                            , sqlQuery));

                        LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                        Logger.LogInfo(string.Format("Suspended mirroring for {0}"
                            , databaseMirrorState.DatabaseName));
                    }
                    catch (Exception ex)
                    {
                        throw new SqlServerMirroringException(string.Format("Suspend failed for {0}", databaseMirrorState.DatabaseName), ex);
                    }
                }
            }
            Logger.LogDebug("Ended");
        }

        public bool Action_Instance_ForceFailoverWithDataLossForAllMirrorDatabases()
        {
            Logger.LogDebug("Started");

            if (!ServerStateMonitor.ServerState_Active.IsPrimaryRole && ServerStateMonitor.ServerState_Active.ValidTransition(ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE))
            {
                ServerStateMonitor.MakeServerStateChange(ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE);
            }
            else
            {
                Logger.LogWarning(string.Format("Server {0} tried to manually force failover but in invalid state {1} for doing so.", Information_Instance_ServerRole, Information_ServerState));
                return false;
            }
            Logger.LogDebug("Ended");
            return true;
        }

        internal bool Information_RemoteServer_ServeredMirroring()
        {
            throw new NotImplementedException();
            int error;
        }

        public bool Action_Instance_FailoverForAllMirrorDatabases()
        {
            Logger.LogDebug("Started");
            if (ServerStateMonitor.ServerState_Active.IsPrimaryRole && ServerStateMonitor.ServerState_Active.ValidTransition(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE))
            {
                ServerStateMonitor.MakeServerStateChange(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE);
            }
            else if (!ServerStateMonitor.ServerState_Active.IsPrimaryRole && ServerStateMonitor.ServerState_Active.ValidTransition(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE))
            {
                ServerStateMonitor.MakeServerStateChange(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE);
            }
            else
            {
                Logger.LogWarning(string.Format("Server {0} tried to manually failover but in invalid state {1} for doing so.", Information_Instance_ServerRole, Information_ServerState));
                return false;
            }
            Logger.LogDebug("Ended");
            return true;
        }

        internal bool Information_IO_BackupLocatedForAllDatabases()
        {
            throw new NotImplementedException();
            int error;
        }

        public bool Action_Instance_BackupForAllConfiguredDatabasesForMirrorServer()
        {
            Logger.LogDebug("Started");
            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                try
                {
                    Action_IO_DeleteOldFiles(configuredDatabase.LocalBackupDirectoryWithSubDirectory);
                    Action_Databases_BackupDatabaseForMirrorServer(configuredDatabase, true);
                    Action_Databases_BackupDatabaseForMirrorServer(configuredDatabase, false);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Backup failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            Logger.LogDebug("Ended");
            return true;
        }

        public bool Action_Instance_BackupForAllConfiguredDatabases()
        {
            Logger.LogDebug("Started");
            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                try
                {
                    Action_IO_DeleteOldFiles(configuredDatabase.LocalBackupDirectoryWithSubDirectory);
                    Action_Databases_BackupDatabase(configuredDatabase);
                    Action_Databases_BackupDatabaseLog(configuredDatabase);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Backup failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            Logger.LogDebug("Ended");
            return true;
        }

        public bool Action_Instance_ShutDown()
        {
            Logger.LogDebug("Action_ShutDownMirroringService started");
            if (Information_ServerState.IsPrimaryRole && Information_ServerState.ValidTransition(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE))
            {
                ServerStateMonitor.MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else if (!Information_ServerState.IsPrimaryRole && Information_ServerState.ValidTransition(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE))
            {
                ServerStateMonitor.MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else
            {
                Logger.LogWarning(string.Format("Action_ShutDownMirroringService: Shutdown tried done from invalid state {0}. If this is needed make a force shutdown.", Information_ServerState));
                return false;
            }
        }

        public bool Action_Instance_ForceShutDownMirroringService()
        {
            Logger.LogDebug("Action_ForceShutDownMirroringService started");
            if (Information_Instance_ServerRole == ServerRoleEnum.Primary || Information_Instance_ServerRole == ServerRoleEnum.MainlyPrimary)
            {
                Logger.LogWarning(string.Format("Action_ForceShutDownMirroringService: Force shutdown of service with role {0} in state {1}.", Information_Instance_ServerRole, Information_ServerState));
                ServerStateMonitor.MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else if (Information_Instance_ServerRole == ServerRoleEnum.Secondary || Information_Instance_ServerRole == ServerRoleEnum.MainlySecondary)
            {
                Logger.LogWarning(string.Format("Action_ForceShutDownMirroringService: Force shutdown of service with role {0} in state {1}.", Information_Instance_ServerRole, Information_ServerState));
                ServerStateMonitor.MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else
            {
                Logger.LogWarning(string.Format("Action_ForceShutDownMirroringService: Force shutdown of service with role {0} in state {1}.", Information_Instance_ServerRole, Information_ServerState));
                ServerStateMonitor.MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                return true;
            }
        }

        internal void Action_Instance_ShutDownTimer()
        {
            _timedCheckTimer.Stop();
            if (_backupTimer != null)
            {
                _backupTimer.Stop();
            }
        }

        #endregion

        #region Private Instance Methods

        private void Action_IO_DeleteOldFiles(DirectoryPath localBackupDirectoryWithSubDirectory)
        {
            Logger.LogDebug(string.Format("Start deleting {0} days old files from {1}."
                , Instance_Configuration.BackupExpiresAfterDays, localBackupDirectoryWithSubDirectory.PathString));
            if (Directory.Exists(localBackupDirectoryWithSubDirectory.PathString))
            {
                DirectoryInfo directory = new DirectoryInfo(localBackupDirectoryWithSubDirectory.PathString);
                List<FileInfo> oldFilesListList = directory.GetFiles()
                    .Where(x => x.CreationTime.Date < DateTime.Today.AddDays(-Instance_Configuration.BackupExpiresAfterDays))
                    .ToList();
                oldFilesListList.ForEach(x => 
                {
                    try
                    {
                        x.Delete();
                        Logger.LogDebug(string.Format("Deleted file {0}.", x));
                    }
                    catch
                    { }
                });
            }
            Logger.LogDebug(string.Format("Finished deleting {0} days old files from {1}."
                , Instance_Configuration.BackupExpiresAfterDays, localBackupDirectoryWithSubDirectory.PathString));
        }

        private void Action_IO_DeleteOldRemoteFiles(UncPath remoteDirectoryWithSubDirectory)
        {
            Logger.LogDebug(string.Format("Start deleting {0} days old files from {1}."
                , Instance_Configuration.BackupExpiresAfterDays, remoteDirectoryWithSubDirectory.BuildUncPath()));
            if (Directory.Exists(remoteDirectoryWithSubDirectory.BuildUncPath()))
            {
                DirectoryInfo directory = new DirectoryInfo(remoteDirectoryWithSubDirectory.BuildUncPath());
                List<FileInfo> oldFilesListList = directory.GetFiles()
                    .Where(x => x.CreationTime.Date < DateTime.Today.AddDays(-Instance_Configuration.BackupExpiresAfterDays))
                    .ToList();
                oldFilesListList.ForEach(x => 
                {
                    try {
                        x.Delete();
                        Logger.LogDebug(string.Format("Deleted file {0}.", x));
                    }
                    catch
                    { }
                });
            }
            Logger.LogDebug(string.Format("Finished deleting {0} days old files from {1}."
                , Instance_Configuration.BackupExpiresAfterDays, remoteDirectoryWithSubDirectory.BuildUncPath()));
        }

        internal bool Information_Instance_AllConfiguredDatabasesMirrored()
        {
            bool allConfigured = true;
            foreach (ConfigurationForDatabase configuration in Databases_Configuration.Values)
            {
                DatabaseMirrorState databaseMirrorState;
                if (Information_DatabaseMirrorStates.TryGetValue(configuration.DatabaseName, out databaseMirrorState))
                {
                    if (databaseMirrorState.MirroringRole == MirroringRoleEnum.NotMirrored)
                    {
                        allConfigured = false;
                        break;
                    }
                }
                else
                {
                    allConfigured = false;
                    break;
                }
            }
            return allConfigured;
        }

        private void Action_Instance_SwitchOverCheck()
        {
            if (Information_ServerState.IsPrimaryRole)
            {
                if (Information_Instance_ServerRole == ServerRoleEnum.MainlyPrimary || Information_Instance_ServerRole == ServerRoleEnum.MainlySecondary)
                {
                    Logger.LogError(string.Format("Server Role {0} but Server State {1}. Switching over all principal databases. Shutting down to signal shift.", Information_Instance_ServerRole, Information_ServerState));
                    Action_Instance_SwitchOverAllPrincipalDatabasesIfPossible(false);
                    Action_Instance_ForceShutDownMirroringService();
                }
                else if (Information_Instance_ServerRole == ServerRoleEnum.Secondary)
                {
                    Logger.LogError(string.Format("Server Role {0} but Server State {1}. Trying to switch all mirror databases.", Information_Instance_ServerRole, Information_ServerState));
                    Action_Instance_SwitchOverAllMirrorDatabasesForcedIfNeeded();
                }
            }
            else
            {
                if (Information_Instance_ServerRole == ServerRoleEnum.MainlyPrimary || Information_Instance_ServerRole == ServerRoleEnum.MainlySecondary)
                {
                    Logger.LogError(string.Format("Server Role {0} but Server State {1}. Switching over all mirror databases. Shutting down to signal shift.", Information_Instance_ServerRole, Information_ServerState));
                    Action_Instance_SwitchOverAllMirrorDatabasesIfPossible(false);
                    Action_Instance_ForceShutDownMirroringService();
                }
                else if (Information_Instance_ServerRole == ServerRoleEnum.Primary)
                {
                    Logger.LogError(string.Format("Server Role {0} but Server State {1}. Shutting down to signal shift.", Information_Instance_ServerRole, Information_ServerState));
                    Action_Instance_ForceShutDownMirroringService();
                }
            }
        }

        private bool Action_IO_TestReadWriteAccessToDirectory(DirectoryPath directoryPath)
        {
            try
            {
                DirectoryHelper.TestReadWriteAccessToDirectory(Logger.InternalLogger, directoryPath);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public void Action_Instance_RestoreDatabases()
        {
            Logger.LogDebug("Action_Instance_RestoreNeeded started");
            foreach (ConfigurationForDatabase configurationDatabase in Databases_Configuration.Values)
            {
                DatabaseState databaseState;
                if (Information_DatabaseStates.TryGetValue(configurationDatabase.DatabaseName, out databaseState))
                {
                    if (Action_Databases_RestoreDatabase(configurationDatabase))
                    {
                        Logger.LogInfo("Action_Instance_RestoreNeeded: Restored backup");
                    }
                    else
                    {
                        Logger.LogInfo("Action_Instance_RestoreNeeded: Moved database from remote share and restored Backup");
                    }
                    if (Action_Databases_RestoreDatabaseLog(configurationDatabase))
                    {
                        Logger.LogInfo("Action_Instance_RestoreNeeded: Restored log backup");
                    }
                    else
                    {
                        Logger.LogInfo("Action_Instance_RestoreNeeded: Moved database from remote share and restored log Backup");
                    }
                }
            }
            Logger.LogDebug("Action_Instance_RestoreNeeded ended");
        }

        internal void Action_Instance_SetupForMirroring()
        {
            Logger.LogDebug("Started");
            //Action_SqlAgent_EnableAgentXps();
            //if (DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Manual ||
            //    DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Disabled)
            //{
            //    Logger.LogDebug("Bug/SecurityIssue: Might not be able to change to automatic start");
            //    Action_Instance_ChangeDatabaseServiceToAutomaticStart();
            //    Logger.LogInfo("Sql Server was set to Automatic start");
            //}
            //if (!DatabaseServerInstance.JobServer.SqlAgentAutoStart)
            //{
            //    Logger.LogDebug("Bug/SecurityIssue: Might not be able to change to automatic start");
            //    Action_Instance_ChangeSqlAgentServiceToAutomaticStart();
            //    Logger.LogInfo("Sql Agent was set to Automatic start");
            //}
            //if (Information_SqlAgent_CheckRunning())
            //{
            //    Logger.LogDebug("Bug/SecurityIssue: Might not be able to stop service for enabling service broker on msdb");
            //    Action_SqlAgent_Stop();
            //    Logger.LogInfo("Sql Agent service was stopped");

            //    Logger.LogDebug("Action_SetupInstanceForMirroring: Try to enable service broker on msdb");
            //    foreach (Database database in DatabaseServerInstance.Databases)
            //    {
            //        if (database.Name.Equals("msdb") && !database.BrokerEnabled)
            //        {
            //            database.BrokerEnabled = true;
            //            database.Alter(TerminationClause.RollbackTransactionsImmediately);
            //            Logger.LogDebug(string.Format("Action_SetupInstanceForMirroring: Database {0} has enabled service broker", database.Name));
            //        }
            //    }
            //}
            //if (!Information_SqlAgent_CheckRunning())
            //{
            //    Logger.LogDebug("Bug/SecurityIssue: Might not be able to start service");
            //    Action_SqlAgent_Start();
            //    Logger.LogInfo("Sql Agent service was started");
            //}
            if (Information_Endpoint_Exists(Instance_Configuration.Endpoint_Name))
            {
                if (Information_Endpoint_Started(Instance_Configuration.Endpoint_Name))
                {
                    Logger.LogDebug(string.Format("Mirroring endpoint {0} exists", Instance_Configuration.Endpoint_Name));
                }
                else
                {
                    Action_Endpoint_Start(Instance_Configuration.Endpoint_Name);
                }
            }
            else
            {
                Action_Endpoint_Create();
            }
            if (!Information_Endpoint_HasConnectRights(Instance_Configuration.Endpoint_Name))
            {
                Logger.LogInfo(string.Format("No connect rights identified for endpoint {0}", Instance_Configuration.Endpoint_Name));
                Action_Endpoint_GrantConnectRights(Instance_Configuration.Endpoint_Name);
            }
        }

        internal bool Information_ServerState_CheckLocalMasterTable()
        {
            return Information_ServerState_CheckMasterTable(LocalMasterDatabase);
        }

        internal bool Information_Instance_Configured()
        {
            if( Information_ServerState_CheckLocalMasterTable()
                && Information_DatabaseState_CheckLocalMasterTableExists()
                && Information_Instance_CheckForMirroring()
                && Information_IO_DirectoriesAndShareExists()
                && Information_Instance_AllConfiguredDatabasesMirrored()
                )
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        internal void Action_ServerState_CreateMasterTable()
        {
            Logger.LogDebug("Action_ServerState_CreateMasterTable started.");

            try
            {
                Table localServerStateTable = new Table(LocalMasterDatabase, "ServerState");
                Column column1 = new Column(localServerStateTable, "Updater", DataType.Char(6));
                column1.Nullable = false;
                localServerStateTable.Columns.Add(column1);
                Column column2 = new Column(localServerStateTable, "About", DataType.Char(6));
                column2.Nullable = false;
                localServerStateTable.Columns.Add(column2);
                Column column3 = new Column(localServerStateTable, "Connected", DataType.Bit);
                column3.Nullable = false;
                localServerStateTable.Columns.Add(column3);
                Column column4 = new Column(localServerStateTable, "LastRole", DataType.NVarChar(50));
                column4.Nullable = false;
                localServerStateTable.Columns.Add(column4);
                Column column5 = new Column(localServerStateTable, "LastState", DataType.NVarChar(50));
                column5.Nullable = false;
                localServerStateTable.Columns.Add(column5);
                Column column6 = new Column(localServerStateTable, "LastWriteDate", DataType.DateTime2(7));
                column6.Nullable = false;
                localServerStateTable.Columns.Add(column6);
                Column column7 = new Column(localServerStateTable, "StateCount", DataType.Int);
                column7.Nullable = false;
                localServerStateTable.Columns.Add(column7);

                localServerStateTable.Create();
                LocalServerStateTable = localServerStateTable;
                Logger.LogDebug("Action_ServerState_CreateMasterTable ended.");
                Action_ServerState_InsertBaseState();
            }
            catch (SqlServerMirroringException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private void Action_ServerState_InsertBaseState()
        {
            try
            {
                Logger.LogDebug("Started.");

                string sqlQuery = "INSERT INTO ServerState (Updater, About, Connected, LastRole, LastState, LastWriteDate, StateCount) ";
                sqlQuery += "VALUES ";
                sqlQuery += string.Format("('{0}','{1}',0,'NotSet','INITIAL_STATE',SYSDATETIME(),0),", ServerPlacement.Local, ServerPlacement.Local);
                sqlQuery += string.Format("('{0}','{1}',0,'NotSet','INITIAL_STATE',SYSDATETIME(),0)", ServerPlacement.Remote, ServerPlacement.Remote);

                Logger.LogDebug(string.Format("SqlQuery {0}", sqlQuery));
                LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                Logger.LogDebug("Ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private void Action_ServerState_Update(Database databaseToUpdate, ServerPlacement updater, ServerPlacement about, bool connected, ServerRoleEnum activeServerRole, ServerState activeServerState, int increaseCount)
        {
            // TODO rewrite to MERGE
            try
            {
                Logger.LogDebug("Started.");

                string sqlQuery = "UPDATE ServerState ";
                sqlQuery += string.Format("SET Connected = {0} ", connected ? "1" : "0");
                sqlQuery += string.Format(", LastRole = '{0}' ", activeServerRole.ToString());
                sqlQuery += string.Format(", LastState = '{0}' ", activeServerState.ToString());
                sqlQuery += ", LastWriteDate = SYSDATETIME() ";
                sqlQuery += string.Format(", StateCount = {0} ", increaseCount == 0 ? "0" : "StateCount + " + increaseCount.ToString());
                sqlQuery += string.Format("WHERE Updater = '{0}' ", updater);
                sqlQuery += string.Format("AND About = '{0}' ", about);

                Logger.LogDebug(string.Format("SqlQuery {0}.", sqlQuery));
                databaseToUpdate.ExecuteNonQuery(sqlQuery);
                Logger.LogDebug("Ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private void Action_ServerRole_Recheck()
        {
            Logger.LogDebug("Started");
            int isPrincipal = 0;
            int isMirror = 0;
            foreach (DatabaseMirrorState databaseMirrorState in Information_DatabaseMirrorStates.Values)
            {
                if (databaseMirrorState.MirroringRole == MirroringRoleEnum.Principal)
                {
                    isPrincipal += 1;
                }
                if (databaseMirrorState.MirroringRole == MirroringRoleEnum.Mirror)
                {
                    isMirror += 1;
                }
            }
            if (isMirror == 0 && isPrincipal > 0)
            {
                /* Sure primary */
                _activeServerRole = ServerRoleEnum.Primary;
            }
            else if (isPrincipal == 0 && isMirror > 0)
            {
                /* Sure secondary */
                _activeServerRole = ServerRoleEnum.Secondary;
            }
            else if (isPrincipal > isMirror)
            {
                /* Mainly primary */
                Logger.LogWarning("Server is mainly in primary role but has mirror databases. Make switchover to bring to correct state.");
                _activeServerRole = ServerRoleEnum.MainlyPrimary;
            }
            else if (isMirror < isPrincipal)
            {
                /* Mainly secondary */
                Logger.LogWarning("Server is mainly in secondary role but has principal databases. Make switchover to bring to correct state.");
                _activeServerRole = ServerRoleEnum.MainlySecondary;
            }
            else
            {
                /* Neither */
                Logger.LogWarning("Server has no principal or mirror database. Investigate what went wrong.");
                _activeServerRole = ServerRoleEnum.Neither;
            }
            Logger.LogDebug("Ended");
        }

        private void Action_Instance_SwitchOverAllDatabasesIfPossible(bool failoverPrincipal, bool failIfNotSwitchingOver)
        {
            MirroringRoleEnum failoverRole;
            MirroringRoleEnum ignoreRole;
            Database databaseToUse;
            if(failoverPrincipal)
            {
                failoverRole = MirroringRoleEnum.Principal;
                ignoreRole = MirroringRoleEnum.Mirror;
                databaseToUse = LocalMasterDatabase;
            }
            else
            {
                failoverRole = MirroringRoleEnum.Mirror;
                ignoreRole = MirroringRoleEnum.Principal;
                databaseToUse = LocalMasterDatabase;
            }
            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                    if (database == null)
                    {
                        if (failIfNotSwitchingOver)
                        {
                            throw new SqlServerMirroringException(string.Format(
                                "Did not switch database {0} as it is unknown."
                                , configuredDatabase.DatabaseName));
                        }
                    }
                    else
                    {
                        MirroringRoleEnum databaseRole = Information_Databases_GetDatabaseMirroringRole(database.Name);
                        
                        if (databaseRole == failoverRole)
                        {
                            Logger.LogDebug(string.Format(
                                "Trying to switch {0} as it in {1}."
                                , database.Name, databaseRole));

                            string sqlQuery = string.Format("ALTER DATABASE {0} SET PARTNER FAILOVER", database.Name);
                            databaseToUse.ExecuteNonQuery(sqlQuery);
                            Logger.LogDebug(string.Format(
                                "Database {0} switched from {1} to {2}."
                                , database.Name, databaseRole, failoverRole));
                        }
                        else if (databaseRole == ignoreRole)
                        {
                            Logger.LogDebug(string.Format(
                                "Did not switch {0} as it is already in {1}."
                                , database.Name, databaseRole));
                        }
                        else
                        {
                            if (failIfNotSwitchingOver)
                            {
                                throw new SqlServerMirroringException(string.Format(
                                    "Did not switch {0} as role {1} is unknown."
                                    , database.Name, databaseRole));
                            }
                            else
                            {
                                Logger.LogWarning(string.Format(
                                    "Did not switch {0} as role {1} is unknown."
                                    , database.Name, databaseRole));
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (failIfNotSwitchingOver)
                    {
                        throw new SqlServerMirroringException(string.Format(
                            "Failover failed for {0}"
                            , configuredDatabase.DatabaseName), ex);
                    }
                }
            }
            Action_ServerRole_Recheck();
        }

        private void Action_Instance_SwitchOverAllDatabasesForcedIfNeeded(bool failoverPrincipal)
        {
            MirroringRoleEnum failoverRole;
            MirroringRoleEnum ignoreRole;
            Database databaseToUse;
            if (failoverPrincipal)
            {
                failoverRole = MirroringRoleEnum.Principal;
                ignoreRole = MirroringRoleEnum.Mirror;
                databaseToUse = LocalMasterDatabase;
            }
            else
            {
                failoverRole = MirroringRoleEnum.Mirror;
                ignoreRole = MirroringRoleEnum.Principal;
                databaseToUse = RemoteMasterDatabase;
            }
            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                    if (database == null)
                    {
                        throw new SqlServerMirroringException(string.Format(
                            "Did not switch database {0} as it is unknown."
                            , configuredDatabase.DatabaseName));
                    }
                    else
                    {
                        MirroringRoleEnum databaseRole = Information_Databases_GetDatabaseMirroringRole(database.Name);

                        if (databaseRole == failoverRole)
                        {
                            try
                            {
                                Logger.LogDebug(string.Format(
                                    "Trying to switch {0} as it in {1}."
                                    , database.Name, databaseRole));
                                string sqlQuery = string.Format("ALTER DATABASE {0} SET PARTNER FAILOVER", database.Name);
                                databaseToUse.ExecuteNonQuery(sqlQuery);
                                Logger.LogDebug(string.Format(
                                    "Database {0} switched from {1} to {2}."
                                    , database.Name, databaseRole, failoverRole));
                            }
                            catch (Exception ex)
                            {
                                Logger.LogError("Switch without dataloss failed", ex);
                                Logger.LogDebug(string.Format(
                                    "Force Failover with dataloss switch {0} as it in {1}."
                                    , database.Name, databaseRole));
                                string sqlQuery = string.Format("ALTER DATABASE {0} SET PARTNER FORCE_SERVICE_ALLOW_DATA_LOSS", database.Name);
                                LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                                Logger.LogDebug(string.Format(
                                    "Database {0} switched with dataloss from {1} to {2}."
                                    , database.Name, databaseRole, failoverRole));
                            }
                        }
                        else if (databaseRole == ignoreRole)
                        {
                            Logger.LogDebug(string.Format("Did not switch {0} as it is already in {1}.", database.Name, databaseRole));
                        }
                        else
                        {
                            throw new SqlServerMirroringException(string.Format(
                                "Did not switch {0} as role {1} is unknown."
                                , database.Name, databaseRole));
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format(
                        "Failover failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            Action_ServerRole_Recheck();
        }

        private void Action_Instance_CheckDatabaseMirrorStates()
        {
            try
            {
                Information_DatabaseMirrorStates = Information_Instance_CheckDatabaseMirrorStates(LocalMasterDatabase);
                Logger.LogDebug("Ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private Dictionary<string, DatabaseMirrorState> Information_Instance_CheckDatabaseMirrorStates(Database databaseToCheck)
        {
            try
            {
                Dictionary<string, DatabaseMirrorState> databaseMirrorStates = new Dictionary<string, DatabaseMirrorState>();

                Logger.LogDebug("Started");

                string sqlQuery = "SELECT databases.name database_name ";
                sqlQuery += "   , databases.database_id ";
                sqlQuery += "	, databases.compatibility_level ";
                sqlQuery += "	, databases.state database_state ";
                sqlQuery += "   , databases.recovery_model ";
                sqlQuery += "	, databases.user_access ";
                sqlQuery += "	, databases.is_in_standby ";
                sqlQuery += "	, mirroring_guid ";
                sqlQuery += "	, mirroring_state ";
                sqlQuery += "	, mirroring_role ";
                sqlQuery += "	, mirroring_safety_level ";
                sqlQuery += "	, mirroring_partner_instance ";
                sqlQuery += "FROM sys.database_mirroring ";
                sqlQuery += "    INNER JOIN sys.databases ";
                sqlQuery += "        ON database_mirroring.database_id = databases.database_id ";
                sqlQuery += "WHERE databases.database_id > 4 ";

                Logger.LogDebug(string.Format("SqlQuery: {0}", sqlQuery));
                DataSet dataSet = databaseToCheck.ExecuteWithResults(sqlQuery);
                if (dataSet == null || dataSet.Tables.Count == 0)
                {
                    Logger.LogWarning("Found no rows returning NoDatabase as it might not be an error");
                }
                else
                {
                    DataTable table = dataSet.Tables[0];
                    foreach (DataRow row in table.Rows)
                    {
                        DatabaseMirrorState databaseMirrorState = new DatabaseMirrorState();
                        foreach (DataColumn column in row.Table.Columns)
                        {
                            switch (column.ColumnName)
                            {
                                case "database_name":
                                    if (row[column] is DBNull)
                                    {
                                        databaseMirrorState.SetDatabaseName("NotSet");
                                    }
                                    else
                                    {
                                        databaseMirrorState.SetDatabaseName((string)row[column]);
                                    }
                                    break; // NVARCHAR(128)
                                case "database_id":
                                    databaseMirrorState.SetDatabaseId((int?)row[column]);
                                    break; // INT
                                case "database_state":
                                    databaseMirrorState.SetDatabaseState((byte?)row[column]);
                                    break; // TINYINT
                                case "compatibility_level":
                                    databaseMirrorState.SetCompatibilityLevel((byte?)row[column]);
                                    break; // TINYINT
                                case "recovery_model":
                                    databaseMirrorState.SetDatabaseRecoveryModel((byte?)row[column]);
                                    break; // TINYINT
                                case "user_access":
                                    databaseMirrorState.SetDatabaseUserAccess((byte?)row[column]);
                                    break; // TINYINT
                                case "is_in_standby":
                                    databaseMirrorState.SetDatabaseIsInStandby((bool?)row[column]);
                                    break; // BIT
                                case "mirroring_guid":
                                    if (row[column] is DBNull)
                                    {
                                        databaseMirrorState.SetMirroringGuid(Guid.Empty);
                                    }
                                    else
                                    {
                                        databaseMirrorState.SetMirroringGuid((Guid)row[column]);
                                    }
                                    break; // UNIQUEIDENTIFIER
                                case "mirroring_state":
                                    if (row[column] is DBNull)
                                    {
                                        databaseMirrorState.SetMirroringState(null);
                                    }
                                    else
                                    {
                                        databaseMirrorState.SetMirroringState((byte)row[column]);
                                    }
                                    break; // TINYINT
                                case "mirroring_role":
                                    if (row[column] is DBNull)
                                    {
                                        databaseMirrorState.SetMirroringRole(null);
                                    }
                                    else
                                    {
                                        databaseMirrorState.SetMirroringRole((byte)row[column]);
                                    }
                                    break; // TINYINT
                                case "mirroring_safety_level":
                                    if (row[column] is DBNull)
                                    {
                                        databaseMirrorState.SetMirroringSafetyLevel(null);
                                    }
                                    else
                                    {
                                        databaseMirrorState.SetMirroringSafetyLevel((byte)row[column]);
                                    }
                                    break; // TINYINT
                                case "mirroring_partner_instance":
                                    if (row[column] is DBNull)
                                    {
                                        databaseMirrorState.SetMirroringParnerInstance("NotSet");
                                    }
                                    else
                                    {
                                        databaseMirrorState.SetMirroringParnerInstance((string)row[column]);
                                    }
                                    break; // NVARCHAR(128)
                            }
                        }
                        databaseMirrorStates.Add(databaseMirrorState.DatabaseName, databaseMirrorState);
                    }
                }
                
                Logger.LogDebug("Ended.");
                return databaseMirrorStates;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private void Action_Instance_SwitchOverAllPrincipalDatabasesIfPossible(bool failIfNotSwitchingOver)
        {
            Action_Instance_SwitchOverAllDatabasesIfPossible(true, failIfNotSwitchingOver);
        }

        private void Action_Instance_SwitchOverAllMirrorDatabasesIfPossible(bool failIfNotSwitchingOver)
        {
            Action_Instance_SwitchOverAllDatabasesIfPossible(false, failIfNotSwitchingOver);
        }

        //private void Action_Instance_SwitchOverAllPrincipalDatabasesForcedIfNeeded()
        //{
        //    Action_Instance_SwitchOverAllDatabasesForcedIfNeeded(true);
        //}

        private void Action_Instance_SwitchOverAllMirrorDatabasesForcedIfNeeded()
        {
            Action_Instance_SwitchOverAllDatabasesForcedIfNeeded(false);
        }

        //private void Action_Instance_ChangeSqlAgentServiceToAutomaticStart()
        //{
        //    /* TODO: Check that it is the correct instance sql server */
        //    foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
        //    {
        //        Logger.LogDebug(string.Format("Checking Sql Agent {0}({1}) in startup state {2} before change", service.Name, service.DisplayName, service.StartMode));
        //        //ServiceController sc = new ServiceController(service.Name);
        //        service.StartMode = Microsoft.SqlServer.Management.Smo.Wmi.ServiceStartMode.Auto;
        //        service.Alter();
        //        Logger.LogInfo(string.Format("Checking Sql Agent {0}({1}) in startup state {2} after change", service.Name, service.DisplayName, service.StartMode));
        //    }
        //}

        //private void Action_Instance_ChangeDatabaseServiceToAutomaticStart()
        //{
        //    /* TODO: Check that it is the correct instance sql server */
        //    foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlServer))
        //    {
        //        Logger.LogDebug(string.Format("Checking Sql Server {0}({1}) in startup state {2} before change", service.Name, service.DisplayName, service.StartMode));
        //        service.StartMode = Microsoft.SqlServer.Management.Smo.Wmi.ServiceStartMode.Auto;
        //        service.Alter();
        //        Logger.LogInfo(string.Format("Checking Sql Server {0}({1}) in startup state {2} after change", service.Name, service.DisplayName, service.StartMode));
        //    }
        //}

        //private void Action_SqlAgent_Start()
        //{
        //    /* TODO: Check that it is the correct instance sql agent */
        //    foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
        //    {
        //        Logger.LogDebug(string.Format("Action_StartSqlAgent: Sql Agent {0}({1}) in state {2}", service.Name, service.DisplayName, service.ServiceState));
        //        if (service.ServiceState != ServiceState.Running)
        //        {
        //            int timeoutCounter = 0;
        //            service.Start();
        //            while (service.ServiceState != ServiceState.Running && timeoutCounter > Instance_Configuration.ServiceStartTimeout)
        //            {
        //                timeoutCounter += Instance_Configuration.ServiceStartTimeoutStep;
        //                System.Threading.Thread.Sleep(Instance_Configuration.ServiceStartTimeoutStep);
        //                Console.WriteLine(string.Format("Action_StartSqlAgent: Waited {0} seconds for Sql Agent {1}({2}) starting: {3}", (timeoutCounter / 1000), service.Name, service.DisplayName, service.ServiceState));
        //                service.Refresh();
        //            }
        //            if (timeoutCounter > Instance_Configuration.ServiceStartTimeout)
        //            {
        //                throw new SqlServerMirroringException(string.Format("Action_StartSqlAgent: Timed out waiting for Sql Agent {1}({2}) starting", service.Name, service.DisplayName));
        //            }
        //        }
        //    }
        //}

        //private void Action_SqlAgent_Stop()
        //{
        //    /* TODO: Check that it is the correct instance sql agent */
        //    foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
        //    {
        //        Logger.LogDebug(string.Format("Action_StopSqlAgent: Sql Agent {0}({1}) in state {2}", service.Name, service.DisplayName, service.ServiceState));
        //        if (service.ServiceState != ServiceState.Stopped)
        //        {
        //            int timeoutCounter = 0;
        //            service.Stop();
        //            while (service.ServiceState != ServiceState.Stopped && timeoutCounter > Instance_Configuration.ServiceStartTimeout)
        //            {
        //                timeoutCounter += Instance_Configuration.ServiceStartTimeoutStep;
        //                System.Threading.Thread.Sleep(Instance_Configuration.ServiceStartTimeoutStep);
        //                Console.WriteLine(string.Format("Action_StopSqlAgent: Waited {0} seconds for Sql Agent {1}({2}) stopping: {3}", (timeoutCounter / 1000), service.Name, service.DisplayName, service.ServiceState));
        //                service.Refresh();
        //            }
        //            if (timeoutCounter > Instance_Configuration.ServiceStartTimeout)
        //            {
        //                throw new SqlServerMirroringException(string.Format("Action_StopSqlAgent: Timed out waiting for Sql Agent {1}({2}) stopping", service.Name, service.DisplayName));
        //            }
        //        }
        //    }
        //}

        //private bool Information_SqlAgent_CheckRunning()
        //{
        //    /* TODO: Check that it is the correct instance sql agent */
        //    foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
        //    {
        //        Logger.LogDebug(string.Format("Checking Sql Agent {0} with display name {1}", service.Name, service.DisplayName));
        //        if (service.ServiceState == ServiceState.Running)
        //        {
        //            return true;
        //        }
        //        else
        //        {
        //            return false;
        //        }
        //    }
        //    throw new SqlServerMirroringException("No Sql Agent installed on the server. Install Sql Agent to set up mirroring.");
        //}

        //private void Action_SqlAgent_EnableAgentXps()
        //{
        //    Configuration configuration = DatabaseServerInstance.Configuration;
        //    if (configuration.AgentXPsEnabled.RunValue == 0)
        //    {
        //        try
        //        {
        //            configuration.AgentXPsEnabled.ConfigValue = 1;
        //            configuration.Alter();
        //            Logger.LogInfo("Enabling 'Agent XPs' succeeded");
        //        }
        //        catch (Exception e)
        //        {
        //            throw new SqlServerMirroringException("Enabling 'Agent XPs' failed", e);
        //        }
        //    }
        //}

        internal void Action_ServerState_StartTimedCheckTimer()
        {
            _timedCheckTimer = new System.Timers.Timer();
            _timedCheckTimer.Interval = 1000 * Instance_Configuration.CheckMirroringStateSecondInterval;
            _timedCheckTimer.SynchronizingObject = _synchronizeInvoke;
            _timedCheckTimer.Elapsed += new ElapsedEventHandler(OnTimedCheckEvent);
            _timedCheckTimer.AutoReset = true;
            _timedCheckTimer.Enabled = true;
            _timedCheckTimer.Start();
        }

        /* This runs as a main thread task */
        private void OnTimedCheckEvent(object source, ElapsedEventArgs e)
        {
            try
            {
                /* Disables timer to avoid two running at the same time */
                Action_ServerState_TimedCheck();
            }
            catch (Exception ex)
            {
                Logger.LogError("Action_ServerState_TimedCheck failed", ex);
                throw new SqlServerMirroringException("Action_ServerState_TimedCheck failed", ex);
            }
        }

        internal void Action_Instance_StartDelayedBackupTimer()
        {
            Timer delayedBackupTimer = new System.Timers.Timer();
            delayedBackupTimer.Interval = Instance_Configuration.BackupTime.CalculateIntervalUntil;
            delayedBackupTimer.Elapsed += new ElapsedEventHandler(OnStartBackupEvent);
            delayedBackupTimer.SynchronizingObject = _synchronizeInvoke;
            delayedBackupTimer.AutoReset = false;
            delayedBackupTimer.Enabled = true;
            delayedBackupTimer.Start();
        }

        /* This runs as a main thread task */
        private void OnStartBackupEvent(object sender, ElapsedEventArgs e)
        {
            try
            {
                Action_Instance_StartBackupTimer();
            }
            catch (Exception ex)
            {
                Logger.LogError("Action_Instance_StartBackupTimer failed", ex);
                throw new SqlServerMirroringException("Action_Instance_StartBackupTimer failed", ex);
            }
        }

        private void Action_Instance_StartBackupTimer()
        {
            _backupTimer = new System.Timers.Timer();
            _backupTimer.Interval = 1000 * 60 * 60 * Instance_Configuration.BackupHourInterval;
            _backupTimer.Elapsed += new ElapsedEventHandler(OnBackupEvent);
            _backupTimer.AutoReset = true;
            _backupTimer.Enabled = true;
            _backupTimer.Start();
        }

        /* This runs as a background task */
        private void OnBackupEvent(object source, ElapsedEventArgs e)
        {
            try
            {
                if (Instance_Configuration.BackupToMirrorServer)
                {
                    Action_Instance_BackupForAllConfiguredDatabasesForMirrorServer();
                }
                else
                {
                    Action_Instance_BackupForAllConfiguredDatabases();
                }
            }
            catch (Exception ex)
            {
                string error;
                if(Instance_Configuration.BackupToMirrorServer)
                {
                    error = "Action_Instance_BackupForAllConfiguredDatabasesForMirrorServer failed";
                }
                else
                {
                    error = "Action_Instance_BackupForAllConfiguredDatabases failed";
                }

                Logger.LogError(error, ex);
                throw new SqlServerMirroringException(error, ex);
            } 
        }

        internal void Action_Instance_StartEmergencyBackupTimer()
        {
            Timer emergencyBackupTimer = new System.Timers.Timer();
            emergencyBackupTimer.Interval = 1000 * 60 * Instance_Configuration.BackupDelayEmergencyBackupMin;
            emergencyBackupTimer.Elapsed += new ElapsedEventHandler(OnEmergencyBackupEvent);
            emergencyBackupTimer.AutoReset = false;
            emergencyBackupTimer.Enabled = true;
            emergencyBackupTimer.Start();
        }

        /* This runs as a background task */
        private void OnEmergencyBackupEvent(object source, ElapsedEventArgs e)
        {
            try
            {
                Action_Instance_BackupForAllConfiguredDatabasesForMirrorServer();
            }
            catch (Exception ex)
            {
                Logger.LogError("Action_Instance_BackupForAllConfiguredDatabasesForMirrorServer failed", ex);
                throw new SqlServerMirroringException("Action_Instance_BackupForAllConfiguredDatabasesForMirrorServer failed", ex);
            }
        }

        internal void Action_IO_CreateDirectoryAndShare()
        {
            Logger.LogDebug(string.Format("Started"));
            try
            {
                foreach (ConfigurationForDatabase configurationForDatabase in Databases_Configuration.Values)
                {
                    DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger.InternalLogger, configurationForDatabase.LocalBackupDirectoryWithSubDirectory);
                    DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger.InternalLogger, configurationForDatabase.LocalRestoreDirectoryWithSubDirectory);
                    DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger.InternalLogger, configurationForDatabase.LocalRemoteTransferDirectoryWithSubDirectory);
                    DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger.InternalLogger, configurationForDatabase.LocalRemoteDeliveryDirectoryWithSubDirectory);
                    DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger.InternalLogger, configurationForDatabase.LocalLocalTransferDirectoryWithSubDirectory);
                }
                Logger.LogDebug(string.Format("Creating LocalShare {0} in LocalShareDirectory {1}", Instance_Configuration.LocalShareName, Instance_Configuration.LocalRestoreDirectory));
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(Logger.InternalLogger, Instance_Configuration.LocalShareDirectory, Instance_Configuration.LocalShareName);
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
            Logger.LogDebug(string.Format("Ended"));
        }

        internal bool Information_IO_DirectoriesAndShareExists()
        {
            if(!Instance_Configuration.LocalBackupDirectory.Exists
                || !Instance_Configuration.LocalRestoreDirectory.Exists
                || !Instance_Configuration.LocalShareDirectory.Exists
                || !Information_IO_LocalShareExists()
                )
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        private bool Information_IO_LocalShareExists()
        {
            try
            {
                ShareHelper.TestReadWriteAccessToShare(Logger.InternalLogger, new UncPath(Instance_Configuration.LocalServer, Instance_Configuration.LocalShareName));
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        internal bool Information_DatabaseState_CheckLocalMasterTableExists()
        {
            return Information_DatabaseState_CheckTableExists(LocalMasterDatabase);
        }

        //private bool Information_DatabaseState_CheckRemoteMasterTableExists()
        //{
        //    return Information_DatabaseState_CheckTableExists(RemoteMasterDatabase);
        //}

        private bool Information_DatabaseState_CheckTableExists(Database masterDatabaseToCheck)
        {
            foreach (Table serverStateTable in masterDatabaseToCheck.Tables)
            {
                if (serverStateTable.Name == "DatabaseState")
                {
                    return true;
                }
            }
            return false;
        }

        internal void Action_DatabaseState_CreateMasterTable()
        {
            Logger.LogDebug("Started.");
            
            try
            {
                Table localServerStateTable = new Table(LocalMasterDatabase, "DatabaseState");
                Column column1 = new Column(localServerStateTable, "DatabaseName", DataType.NVarChar(128));
                column1.Nullable = false;
                localServerStateTable.Columns.Add(column1);
                Column column2 = new Column(localServerStateTable, "DatabaseState", DataType.NVarChar(50));
                column2.Nullable = false;
                localServerStateTable.Columns.Add(column2);
                Column column3 = new Column(localServerStateTable, "ServerRole", DataType.NVarChar(50));
                column3.Nullable = false;
                localServerStateTable.Columns.Add(column3);
                Column column4 = new Column(localServerStateTable, "LastWriteDate", DataType.DateTime2(7));
                column4.Nullable = false;
                localServerStateTable.Columns.Add(column4);
                Column column5 = new Column(localServerStateTable, "ErrorDatabaseState", DataType.Bit);
                column5.Nullable = false;
                localServerStateTable.Columns.Add(column5);
                Column column6 = new Column(localServerStateTable, "ErrorCount", DataType.Int);
                column6.Nullable = false;
                localServerStateTable.Columns.Add(column6);

                localServerStateTable.Create();
                LocalServerStateTable = localServerStateTable;
                Logger.LogDebug("Ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private void Action_DatabaseState_Update(string databaseName, DatabaseStateEnum databaseState, ServerRoleEnum activeServerRole, bool errorDatabaseState, int errorCount)
        {
            Action_DatabaseState_Update(LocalMasterDatabase, databaseName, databaseState, activeServerRole, errorDatabaseState, errorCount);
        }

        private void Action_DatabaseState_Update(Database databaseToUpdate, string databaseName, DatabaseStateEnum databaseState, ServerRoleEnum activeServerRole, bool errorDatabaseState, int errorCount)
        {
            try
            {
                Logger.LogDebug("Started.");

                string sqlQuery = "MERGE INTO DatabaseState AS Target ";
                sqlQuery += string.Format("USING (VALUES('{0}', '{1}', '{2}', SYSDATETIME(), {3}, {4})) "
                    ,databaseName, databaseState.ToString(), activeServerRole.ToString(), errorDatabaseState?"1":"0", errorCount.ToString());
                sqlQuery += "AS Source(DatabaseName, DatabaseState, ServerRole, LastWriteDate, ErrorDatabaseState, ErrorCount) ";
                sqlQuery += "ON(Target.DatabaseName = Source.DatabaseName) ";
                sqlQuery += "WHEN MATCHED THEN ";
                sqlQuery += "UPDATE SET DatabaseState = Source.DatabaseState ";
                sqlQuery += "    , ServerRole = Source.ServerRole ";
                sqlQuery += "    , LastWriteDate = Source.LastWriteDate ";
                sqlQuery += "    , ErrorDatabaseState = Source.ErrorDatabaseState ";
                sqlQuery += "	 , ErrorCount = CASE WHEN Source.ErrorCount = 0 THEN 0 ELSE Target.ErrorCount + Source.ErrorCount END ";
                sqlQuery += "WHEN NOT MATCHED BY TARGET THEN ";
                sqlQuery += "INSERT(DatabaseName, DatabaseState, ServerRole, LastWriteDate, ErrorDatabaseState, ErrorCount) ";
                sqlQuery += "VALUES(DatabaseName, DatabaseState, ServerRole, LastWriteDate, ErrorDatabaseState, ErrorCount); ";

                Logger.LogDebug(string.Format("Action_DatabaseState_Update sqlQuery {0}",sqlQuery));
                databaseToUpdate.ExecuteNonQuery(sqlQuery);
                Logger.LogDebug("Action_DatabaseState_Update ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_DatabaseState_Update failed", ex);
            }
        }

        private void Action_Instance_CheckDatabaseStates()
        {
            Information_DatabaseStates = Information_DatabaseState_Check(LocalMasterDatabase);
        }

        private Dictionary<string, DatabaseState> Information_DatabaseState_Check(Database databaseToCheck)
        {
            try
            {
                Logger.LogDebug("Started");

                Dictionary<string, DatabaseState> databaseStates = new Dictionary<string, DatabaseState>();
                string sqlQuery = "SELECT DatabaseName, DatabaseState, ServerRole, LastWriteDate, ErrorDatabaseState, ErrorCount FROM DatabaseState";

                Logger.LogDebug(string.Format("SqlQuery {0}", sqlQuery));
                DataSet dataSet = databaseToCheck.ExecuteWithResults(sqlQuery);
                if (dataSet == null || dataSet.Tables.Count == 0)
                {
                    Logger.LogWarning("Found no rows returning NoDatabase as it might not be an error");
                }
                else
                {
                    DataTable table = dataSet.Tables[0];
                    foreach (DataRow row in table.Rows)
                    {
                        DatabaseState databaseState = new DatabaseState();
                        foreach (DataColumn column in row.Table.Columns)
                        {
                            switch (column.ColumnName)
                            {
                                case "DatabaseName":
                                    databaseState.SetDatabaseName((string)row[column]);
                                    break; // NVARCHAR(128)
                                case "DatabaseState":
                                    string databaseStateString = (string)row[column];
                                    DatabaseStateEnum databaseStateEnum;
                                    if(!Enum.TryParse(databaseStateString, out databaseStateEnum))
                                    {
                                        throw new SqlServerMirroringException(string.Format("Could not parse {0} for DatabaseStateEnum", databaseStateString));
                                    }
                                    databaseState.SetDatabaseState(databaseStateEnum);
                                    break; // NVARCHAR(50)
                                case "ServerRole":
                                    string serverRoleString = (string)row[column];
                                    ServerRoleEnum serverRoleEnum;
                                    if (!Enum.TryParse(serverRoleString, out serverRoleEnum))
                                    {
                                        throw new SqlServerMirroringException(string.Format("Could not parse {0} for ServerRoleEnum", serverRoleString));
                                    }
                                    databaseState.SetServerRole(serverRoleEnum);
                                    break; // NVARCHAR(50)
                                case "LastWriteDate":
                                    databaseState.SetLastWriteDate((DateTime)row[column]);
                                    break; // DATETIME2(7)
                                case "ErrorDatabaseState":
                                    databaseState.SetErrorDatabaseState((bool)row[column]);
                                    break; // BIT
                                case "ErrorCount":
                                        databaseState.SetErrorCount((int)row[column]);
                                    break; // INT
                            }
                        }
                        databaseStates.Add(databaseState.DatabaseName, databaseState);
                    }
                }
                
                Logger.LogDebug("Ended.");
                return databaseStates;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private bool Action_DatabaseState_ShiftState()
        {
            Logger.LogDebug("Started.");
            int checksToShutDown = Instance_Configuration.ShutDownAfterNumberOfChecksForDatabaseState;
            if (checksToShutDown == 0)
            {
                return false;
            }
            else
            {
                int countOfChecks = Information_DatabaseState_GetErrorCount();
                if (countOfChecks > checksToShutDown)
                {
                    Logger.LogWarning(string.Format("Will shut down from state {0} because of Database State Error as count {1} is above {2}."
                        , Information_ServerState, countOfChecks, checksToShutDown));
                    Action_DatabaseState_ResetErrorStates();
                    return true;
                }
                else
                {
                    Logger.LogDebug(string.Format("Should not shut down because of Database State Error as count {0} is not above {1}.", countOfChecks, checksToShutDown));
                    return false;
                }
            }
        }

        private void Action_DatabaseState_ResetErrorStates()
        {
            try
            {
                Logger.LogDebug("Started.");

                string sqlQuery = "UPDATE DatabaseState SET ErrorCount = 0 ";

                Logger.LogDebug(string.Format("SqlQuery {0}.", sqlQuery));
                LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                Logger.LogDebug("Ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private int Information_DatabaseState_GetErrorCount()
        {
            try
            {
                Logger.LogDebug("Started");

                int maxCount = 0;

                foreach(DatabaseState databaseState in Information_DatabaseStates.Values)
                {
                    if(databaseState.ErrorDatabaseState)
                    {
                        if(databaseState.ErrorCount > maxCount)
                        {
                            maxCount = databaseState.ErrorCount;
                        }
                    }
                }
                Logger.LogDebug(string.Format("Ended with error count {0}", maxCount.ToString()));
                return maxCount;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private MirroringRoleEnum Information_Databases_GetDatabaseMirroringRole(string databaseName)
        {
            Logger.LogDebug("Started");
            DatabaseMirrorState databaseMirrorState;
            if (Information_DatabaseMirrorStates.TryGetValue(databaseName, out databaseMirrorState))
            {
                Logger.LogDebug(string.Format("Found {0}", databaseMirrorState.MirroringRole));
                return databaseMirrorState.MirroringRole;
            }
            else
            {
                throw new SqlServerMirroringException("Failed");
            }
        }

        #endregion

        #region Individual Databases

        internal bool Action_Instance_CheckStartMirroring()
        {
            if (Information_RemoteServer_HasAccess())
            {
                if (Information_RemoteServer_ReadyForMirroring())
                {
                    return Action_Instance_StartMirroring();
                }
                else
                {
                    Logger.LogInfo(string.Format("Remote server {0} not ready for mirroring.", Instance_Configuration.RemoteServer));
                    return false;
                }
            }
            else
            {
                Logger.LogWarning("Could not start mirroring as remote server is it not possible to connect to.");
                return false;
            }
        }

        private bool Information_RemoteServer_ReadyForMirroring()
        {
            return Information_RemoteServer_CheckLocalWrittenState(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE);
        }

        private bool Information_RemoteServer_CheckLocalWrittenState(ServerStateEnum serverStateToLookFor)
        {
            try
            {
                string sqlQuery = string.Format("SELECT TOP (1) StateCount FROM ServerState WHERE LastState = '{0}' AND Updater = '{1}' AND About = '{2}' ", serverStateToLookFor, ServerPlacement.Remote, ServerPlacement.Remote);

                Logger.LogDebug(string.Format("SqlQuery {0}", sqlQuery));
                DataSet dataSet = LocalMasterDatabase.ExecuteWithResults(sqlQuery);
                foreach (DataTable table in dataSet.Tables)
                {
                    foreach (DataRow row in table.Rows)
                    {
                        foreach (DataColumn column in row.Table.Columns)
                        {
                            if (column.DataType == typeof(Int32))
                            {
                                int? returnValue = (int?)row[column];
                                Logger.LogDebug(string.Format("For {0} ended with value {1}", returnValue));
                                if (returnValue.HasValue)
                                {
                                    return true;
                                }
                            }
                        }
                    }
                }
                return false;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private void Action_Endpoint_Start(string endpoint_Name)
        {
            foreach (Endpoint endpoint in DatabaseServerInstance.Endpoints)
            {
                if (endpoint.Name == endpoint_Name)
                {
                    if (endpoint.EndpointState != EndpointState.Started)
                    {
                        endpoint.Start();
                        Logger.LogInfo(string.Format("Started endpoint {0}", endpoint_Name));
                        break;
                    }
                    else
                    {
                        Logger.LogDebug(string.Format("Endpoint {0} was already started so no action taken", endpoint_Name));
                        break;
                    }
                }
            }
        }

        private bool Information_Endpoint_Exists(string endpoint_Name)
        {
            bool found = false;
            foreach (Endpoint endpoint in DatabaseServerInstance.Endpoints)
            {
                if (endpoint.Name.Equals(endpoint_Name))
                {
                    found = true;
                    break;
                }
            }
            return found;
        }

        private bool Information_Endpoint_Started(string endpoint_Name)
        {
            bool started = false;
            foreach (Endpoint endpoint in DatabaseServerInstance.Endpoints)
            {
                if (endpoint.Name == endpoint_Name)
                {
                    if (endpoint.EndpointState == EndpointState.Started)
                    {
                        started = true;
                        break;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            return started;
        }

        private bool Action_Instance_StartMirroring()
        {
            bool mirroringStarted = true;
            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                if (!Action_Databases_StartMirroring(configuredDatabase))
                {
                    mirroringStarted = false;
                }
            }
            if (mirroringStarted)
            {
                Logger.LogDebug("Setup monitoring");
                Action_Instance_SetupMonitoring();
            }
            return mirroringStarted;
        }

        private bool Action_Databases_StartMirroring(ConfigurationForDatabase configuredDatabase)
        {
            DatabaseMirrorState databaseMirrorState;
            if (Information_DatabaseMirrorStates.TryGetValue(configuredDatabase.DatabaseName, out databaseMirrorState))
            {
                if (databaseMirrorState.MirroringRole == MirroringRoleEnum.NotMirrored)
                {
                    try
                    {
                        Logger.LogDebug(string.Format("Getting ready to start mirroring {0} with partner endpoint on {1} port {2}"
                            , configuredDatabase.DatabaseName, Instance_Configuration.RemoteServer, Instance_Configuration.Endpoint_ListenerPort));

                        //string sqlQueryLogin = "CREATE LOGIN {0} FROM WINDOWS WITH DEFAULT_DATABASE = [master]";
                        //string sqlQueryConnect = "GRANT CONNECT IN ENDPOINT::{0} TO {1}";
                        string sqlQueryMirroring = "ALTER DATABASE {0} SET PARTNER = 'TCP://{1}:{2}'";
                        //string serviceAccount = DatabaseServerInstance.ServiceAccount;
                        
                        #region RemoteMaster

                        //string sqlQuery = string.Format(sqlQueryLogin, serviceAccount);
                        //Logger.LogDebug(string.Format("Action_Databases_StartMirroring: Executing {0} on {1}.", sqlQuery, Instance_Configuration.RemoteServer));
                        //RemoteMasterDatabase.ExecuteNonQuery(sqlQuery);
                        
                        //sqlQuery = string.Format(sqlQueryConnect, Instance_Configuration.Endpoint_Name, serviceAccount);
                        //Logger.LogDebug(string.Format("Action_Databases_StartMirroring: Executing {0} on {1}.", sqlQuery, Instance_Configuration.RemoteServer));
                        //RemoteMasterDatabase.ExecuteNonQuery(sqlQuery);
                        
                        string sqlQuery = string.Format(sqlQueryMirroring
                            , configuredDatabase.DatabaseName, Instance_Configuration.LocalServer, Instance_Configuration.Endpoint_ListenerPort);
                        Logger.LogDebug(string.Format("Executing {0} on {1}.", sqlQuery, Instance_Configuration.RemoteServer));
                        RemoteMasterDatabase.ExecuteNonQuery(sqlQuery);

                        #endregion

                        #region LocalMaster
                        
                        //string sqlQuery = string.Format(sqlQueryLogin, serviceAccount);
                        //Logger.LogDebug(string.Format("Action_Databases_StartMirroring: Executing {0} on {1}.", sqlQuery, Instance_Configuration.LocalServer));
                        //LocalMasterDatabase.ExecuteNonQuery(sqlQuery);

                        //sqlQuery = string.Format(sqlQueryConnect, Instance_Configuration.Endpoint_Name, serviceAccount);
                        //Logger.LogDebug(string.Format("Action_Databases_StartMirroring: Executing {0} on {1}.", sqlQuery, Instance_Configuration.LocalServer));
                        //LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                        
                        sqlQuery = string.Format(sqlQueryMirroring
                            , configuredDatabase.DatabaseName, Instance_Configuration.RemoteServer, Instance_Configuration.Endpoint_ListenerPort);
                        Logger.LogDebug(string.Format("Executing {0} on {1}.", sqlQuery, Instance_Configuration.LocalServer));
                        LocalMasterDatabase.ExecuteNonQuery(sqlQuery);

                        #endregion

                        Logger.LogDebug(string.Format("Mirroring started {0} with partner endpoint on {1} port {2}"
                            , configuredDatabase.DatabaseName, Instance_Configuration.RemoteServer, Instance_Configuration.Endpoint_ListenerPort));
                        return true;
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(string.Format("Creation of mirroring failed for {0}", configuredDatabase.DatabaseName), ex);
                        return false;
                    }
                }
                else
                {
                    Logger.LogDebug(string.Format("Database {0} is already in {1}.", configuredDatabase.DatabaseName, databaseMirrorState.MirroringRole));
                    return true;
                }
            }
            else
            {
                Logger.LogDebug(string.Format("Could not get configuration for {0}", configuredDatabase.DatabaseName));
                return false;
            }
        }

        private void Action_Endpoint_Create()
        {
            try
            {
                Endpoint endpoint = default(Endpoint);
                endpoint = new Endpoint(DatabaseServerInstance, Instance_Configuration.Endpoint_Name);
                endpoint.ProtocolType = ProtocolType.Tcp;
                endpoint.EndpointType = EndpointType.DatabaseMirroring;
                endpoint.Protocol.Tcp.ListenerPort = Instance_Configuration.Endpoint_ListenerPort;
                endpoint.Payload.DatabaseMirroring.ServerMirroringRole = ServerMirroringRole.All;
                Logger.LogDebug(string.Format("Creates endpoint {0}", Instance_Configuration.Endpoint_Name));
                endpoint.Create();
                Logger.LogDebug(string.Format("Starts endpoint {0}", Instance_Configuration.Endpoint_Name));
                endpoint.Start();
                Logger.LogDebug(string.Format("Created endpoint and started. Endpoint in state {1}.", endpoint.EndpointState));
            }
            catch (Exception ex)
            {
                if (!DatabaseServerInstance.Endpoints.Contains(Instance_Configuration.Endpoint_Name))
                {
                    throw new SqlServerMirroringException(string.Format("Creation of endpoint for {0} failed", Instance_Configuration.Endpoint_Name), ex);
                }
            }
        }

        private bool Information_Endpoint_HasConnectRights(string endpointName)
        {
            string runnerOfSqlServer = DatabaseServerInstance.ServiceAccount;
            try
            {
                string sqlQuery = "SELECT endpoints.name ";
                sqlQuery += "FROM sys.server_permissions ";
                sqlQuery += "    INNER JOIN sys.endpoints ";
                sqlQuery += "        ON server_permissions.major_id = endpoints.endpoint_id ";
                sqlQuery += "    INNER JOIN sys.server_principals GranteePrincipals ";
                sqlQuery += "        ON server_permissions.grantee_principal_id = GranteePrincipals.principal_id ";
                sqlQuery += "WHERE server_permissions.class_desc = 'ENDPOINT' ";
                sqlQuery += string.Format("AND endpoints.name = '{0}' ", endpointName);
                sqlQuery += string.Format("AND GranteePrincipals.name = '{0}' ", runnerOfSqlServer);

                Logger.LogDebug(string.Format("SqlQuery {0}", sqlQuery));
                DataSet dataSet = LocalMasterDatabase.ExecuteWithResults(sqlQuery);
                if (dataSet.Tables.Count == 0)
                {
                    Logger.LogDebug(string.Format("{0} has no connect for {1}", endpointName, runnerOfSqlServer));
                    return false;
                }
                else
                {
                    Logger.LogDebug(string.Format("{0} has connect for {1}", endpointName, runnerOfSqlServer));
                    return true;
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("{0} failed for {1}", endpointName, runnerOfSqlServer), ex);
            }
        }

        private void Action_Endpoint_GrantConnectRights(string endpointName)
        {
            string runnerOfSqlServer = DatabaseServerInstance.ServiceAccount;
            try
            {
                string sqlQuery = string.Format("GRANT CONNECT ON ENDPOINT::{0} TO [{1}]", endpointName, runnerOfSqlServer);
                Logger.LogDebug(string.Format("SqlQuery {0}", sqlQuery));
                LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                Logger.LogDebug(string.Format("{0} failed for {1}", endpointName, runnerOfSqlServer));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("{0} failed for {1}", endpointName, runnerOfSqlServer), ex);
            }
        }

        private bool Information_ServerState_CheckMasterTable(Database databaseToCheck)
        {
            foreach (Table serverStateTable in databaseToCheck.Tables)
            {
                if (serverStateTable.Name == "ServerState")
                {
                    return true;
                }
            }
            return false;
        }

        private string Action_Databases_BackupDatabaseLog(ConfigurationForDatabase configuredDatabase)
        {
            try
            {
                DirectoryPath localDirectoryForBackup = configuredDatabase.LocalBackupDirectoryWithSubDirectory;
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger.InternalLogger, localDirectoryForBackup);

                Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                if (database == null)
                {
                    throw new SqlServerMirroringException(string.Format("Could not find database {0}", configuredDatabase.DatabaseName));
                }

                string fileName = configuredDatabase.DatabaseName + "_" + DateTime.Now.ToFileTime() + "." + Instance_Configuration.DatabaseLogBackupFileEnd;
                string fullFileName = localDirectoryForBackup.PathString + DIRECTORY_SPLITTER + fileName;

                // Define a Backup object variable. 
                Backup logBackup = new Backup();

                // Specify the type of backup, the description, the name, and the database to be backed up. 
                logBackup.Action = BackupActionType.Log;
                logBackup.BackupSetDescription = "Transaction log backup of " + configuredDatabase.DatabaseName.ToString();
                logBackup.BackupSetName = configuredDatabase.DatabaseName.ToString() + " Log Backup";
                logBackup.Database = configuredDatabase.DatabaseName.ToString();

                // Declare a BackupDeviceItem by supplying the backup device file name in the constructor, and the type of device is a file. 
                BackupDeviceItem bdi = default(BackupDeviceItem);
                bdi = new BackupDeviceItem(fullFileName, DeviceType.File);

                // Add the device to the Backup object. 
                logBackup.Devices.Add(bdi);
                // Set the Incremental property to False to specify that this is a full database backup. 
                logBackup.Incremental = false;

                // Set the expiration date. 
                System.DateTime backupdate = System.DateTime.Now.AddDays(Instance_Configuration.BackupExpiresAfterDays);
                logBackup.ExpirationDate = backupdate;

                // Specify that the log must be truncated after the backup is complete. 
                logBackup.LogTruncation = BackupTruncateLogType.Truncate;

                logBackup.Initialize = true;

                logBackup.PercentComplete += Action_Databases_BackupDatabaseLog_CompletionStatusInPercent;

                // Run SqlBackup to perform the full database backup on the instance of SQL Server. 
                logBackup.SqlBackup(DatabaseServerInstance);

                // Inform the user that the backup has been completed. 
                Logger.LogInfo(string.Format("Action_BackupDatabaseLog: Log backup of {0} done", configuredDatabase.DatabaseName));

                // Remove the backup device from the Backup object. 
                logBackup.Devices.Remove(bdi);

                return fileName;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_BackupDatabaseLog: Log backup of database {0} failed", configuredDatabase.DatabaseName), ex);
            }
        }

        private void Action_Databases_BackupDatabaseLog_CompletionStatusInPercent(object sender, PercentCompleteEventArgs args)
        {
            Logger.LogInfo(string.Format("Action_BackupDatabaseLog: Percent completed: {0}%.", args.Percent));
        }

        private string Action_Databases_BackupDatabase(ConfigurationForDatabase configuredDatabase)
        {
            try
            {
                DirectoryPath localDirectoryForBackup = configuredDatabase.LocalBackupDirectoryWithSubDirectory;
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger.InternalLogger, localDirectoryForBackup);

                Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                if (database == null)
                {
                    throw new SqlServerMirroringException(string.Format("Action_BackupDatabase: Could not find database {0}", configuredDatabase.DatabaseName));
                }

                string fileName = configuredDatabase.DatabaseName + "_" + DateTime.Now.ToFileTime() + "." + Instance_Configuration.DatabaseBackupFileEnd;
                string fullFileName = localDirectoryForBackup.PathString + DIRECTORY_SPLITTER + fileName;

                // Define a Backup object variable. 
                Backup backup = new Backup();

                // Specify the type of backup, the description, the name, and the database to be backed up. 
                backup.Action = BackupActionType.Database;
                backup.BackupSetDescription = "Full backup of " + configuredDatabase.DatabaseName.ToString();
                backup.BackupSetName = configuredDatabase.DatabaseName.ToString() + " Backup";
                backup.Database = configuredDatabase.DatabaseName.ToString();

                // Declare a BackupDeviceItem by supplying the backup device file name in the constructor, and the type of device is a file. 
                BackupDeviceItem bdi = default(BackupDeviceItem);
                bdi = new BackupDeviceItem(fullFileName, DeviceType.File);

                // Add the device to the Backup object. 
                backup.Devices.Add(bdi);
                // Set the Incremental property to False to specify that this is a full database backup. 
                backup.Incremental = false;

                // Set the expiration date. 
                System.DateTime backupdate = System.DateTime.Now.AddDays(Instance_Configuration.BackupExpiresAfterDays);
                backup.ExpirationDate = backupdate;

                // Specify that the log must be truncated after the backup is complete. 
                backup.LogTruncation = BackupTruncateLogType.Truncate;

                backup.Initialize = true;
                backup.CompressionOption = BackupCompressionOptions.On;
                backup.PercentComplete += Action_Databases_BackupDatabase_CompletionStatusInPercent;

                // Run SqlBackup to perform the full database backup on the instance of SQL Server. 
                backup.SqlBackup(DatabaseServerInstance);

                // Inform the user that the backup has been completed. 
                Logger.LogInfo(string.Format("Action_BackupDatabase: Full backup of {0} done", configuredDatabase.DatabaseName));

                // Remove the backup device from the Backup object. 
                backup.Devices.Remove(bdi);

                return fileName;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_BackupDatabase: Backup of database {0} failed", configuredDatabase.DatabaseName), ex);
            }
        }

        private void Action_Databases_BackupDatabase_CompletionStatusInPercent(object sender, PercentCompleteEventArgs args)
        {
            Logger.LogInfo(string.Format("Action_BackupDatabase: Percent completed: {0}%.", args.Percent));
        }

        // Setup Backup with BackupDatabase (responsible for moving database backup to remote share)
        private bool Action_Databases_BackupDatabaseForMirrorServer(ConfigurationForDatabase configuredDatabase, bool isMainDatabase)
        {
            string fileName;
            if (isMainDatabase)
            {
                fileName = Action_Databases_BackupDatabase(configuredDatabase);
            }
            else
            {
                fileName = Action_Databases_BackupDatabaseLog(configuredDatabase);
            }
            DirectoryPath localBackupDirectoryWithSubDirectory = configuredDatabase.LocalBackupDirectoryWithSubDirectory;
            DirectoryHelper.TestReadWriteAccessToDirectory(Logger.InternalLogger, localBackupDirectoryWithSubDirectory);
            DirectoryPath localLocalTransferDirectoryWithSubDirectory = configuredDatabase.LocalLocalTransferDirectoryWithSubDirectory;
            ShareName localShareName = configuredDatabase.LocalShareName;
            try
            {
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(Logger.InternalLogger, localLocalTransferDirectoryWithSubDirectory, localShareName);
                Action_IO_DeleteOldFiles(localBackupDirectoryWithSubDirectory);
                Action_IO_CopyFileLocal(fileName, localBackupDirectoryWithSubDirectory, localLocalTransferDirectoryWithSubDirectory);
            }
            catch (Exception e)
            {
                throw new SqlServerMirroringException(string.Format("Failed copying backup file {0} locally", fileName),e);
            }
            if (Information_RemoteServer_HasAccess())
            {
                try
                {
                    UncPath remoteRemoteTransferDirectoryWithSubDirectory = configuredDatabase.RemoteRemoteTransferDirectoryWithSubDirectory;
                    UncPath remoteRemoteDeliveryDirectoryWithSubDirectory = configuredDatabase.RemoteRemoteDeliveryDirectoryWithSubDirectory;
                    Action_IO_DeleteOldRemoteFiles(remoteRemoteTransferDirectoryWithSubDirectory);
                    Action_IO_MoveFileLocalToRemote(fileName, localLocalTransferDirectoryWithSubDirectory, remoteRemoteTransferDirectoryWithSubDirectory);
                    Action_IO_DeleteOldRemoteFiles(remoteRemoteDeliveryDirectoryWithSubDirectory);
                    Action_IO_MoveFileRemoteToRemote(fileName, remoteRemoteTransferDirectoryWithSubDirectory, remoteRemoteDeliveryDirectoryWithSubDirectory);
                    return true; // return true if moved to remote directory
                }
                catch (Exception)
                {
                    /* Ignore the reason for the failure */
                    return false; // return false if moved to remote directory
                }
            }
            else
            {
                Logger.LogDebug("No access to remote server");
                return false;
            }
        }

        // Setup Restore with RestoreDatabase (responsible for restoring database)
        private bool Action_Databases_RestoreDatabaseLog(ConfigurationForDatabase configuredDatabase)
        {
            Logger.LogDebug(string.Format("Started for {0}", configuredDatabase.DatabaseName));

            string fileName;
            try
            {
                if (Action_IO_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles(configuredDatabase, Instance_Configuration.DatabaseLogBackupSearchPattern))
                {

                    DirectoryPath localRestoreDircetoryWithSubDirectory = configuredDatabase.LocalRestoreDirectoryWithSubDirectory;

                    fileName = Information_IO_GetNewesteFilename(configuredDatabase.DatabaseName.ToString(), localRestoreDircetoryWithSubDirectory.ToString(), Instance_Configuration.DatabaseLogBackupSearchPattern);
                    string fullFileName = localRestoreDircetoryWithSubDirectory + DIRECTORY_SPLITTER + fileName;
                    // Define a Restore object variable.
                    Logger.LogDebug(string.Format("For {0} from {1}", configuredDatabase.DatabaseName, fullFileName));
                    Restore rs = new Restore();

                    // Set the NoRecovery property to true, so the transactions are not recovered. 
                    rs.NoRecovery = true;
                    rs.ReplaceDatabase = true;

                    // Declare a BackupDeviceItem by supplying the backup device file name in the constructor, and the type of device is a file. 
                    BackupDeviceItem bdi = default(BackupDeviceItem);
                    bdi = new BackupDeviceItem(fullFileName, DeviceType.File);

                    // Add the device that contains the full database backup to the Restore object. 
                    rs.Devices.Add(bdi);

                    // Specify the database name. 
                    rs.Database = configuredDatabase.DatabaseName.ToString();

                    // Restore the full database backup with no recovery. 
                    rs.SqlRestore(DatabaseServerInstance);

                    // Inform the user that the Full Database Restore is complete. 
                    Logger.LogDebug(string.Format("For {0} complete", configuredDatabase.DatabaseName));

                    return true;
                }
                else
                {
                    if (Information_Databases_DatabaseExists(configuredDatabase.DatabaseName.ToString()))
                    {
                        Logger.LogInfo(string.Format("No log backup to restore for {0}.", configuredDatabase.DatabaseName.ToString()));
                        return false;
                    }
                    else
                    {
                        throw new SqlServerMirroringException(string.Format("Could not find database log to restore for {0}.", configuredDatabase.DatabaseName.ToString()));
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Restore failed for {0}", configuredDatabase.DatabaseName), ex);
            }
        }

        private bool Action_Databases_RestoreDatabase(ConfigurationForDatabase configuredDatabase)
        {
            Logger.LogDebug(string.Format("Started for {0}", configuredDatabase.DatabaseName));

            string fileName;
            try {
                if (Action_IO_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles(configuredDatabase, Instance_Configuration.DatabaseBackupSearchPattern))
                {

                    DirectoryPath localRestoreDircetoryWithSubDirectory = configuredDatabase.LocalRestoreDirectoryWithSubDirectory;

                    fileName = Information_IO_GetNewesteFilename(configuredDatabase.DatabaseName.ToString(), localRestoreDircetoryWithSubDirectory.ToString(), Instance_Configuration.DatabaseBackupSearchPattern);
                    string fullFileName = localRestoreDircetoryWithSubDirectory + DIRECTORY_SPLITTER + fileName;
                    // Define a Restore object variable.
                    Logger.LogDebug(string.Format("For {0} from {1}", configuredDatabase.DatabaseName, fullFileName));
                    Restore rs = new Restore();

                    // Set the NoRecovery property to true, so the transactions are not recovered. 
                    rs.NoRecovery = true;
                    rs.ReplaceDatabase = true;

                    // Declare a BackupDeviceItem by supplying the backup device file name in the constructor, and the type of device is a file. 
                    BackupDeviceItem bdi = default(BackupDeviceItem);
                    bdi = new BackupDeviceItem(fullFileName, DeviceType.File);

                    // Add the device that contains the full database backup to the Restore object. 
                    rs.Devices.Add(bdi);

                    // Specify the database name. 
                    rs.Database = configuredDatabase.DatabaseName.ToString();

                    // Restore the full database backup with no recovery. 
                    rs.SqlRestore(DatabaseServerInstance);

                    // Inform the user that the Full Database Restore is complete. 
                    Logger.LogDebug(string.Format("For {0} complete", configuredDatabase.DatabaseName));

                    return true;
                }
                else
                {
                    if (Information_Databases_DatabaseExists(configuredDatabase.DatabaseName.ToString()))
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

        private bool Information_Databases_DatabaseExists(string databaseName)
        {
            Database database = Information_UserDatabases.Where(s => s.Name.Equals(databaseName)).FirstOrDefault();
            if(database == null)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        private string Information_IO_GetNewesteFilename(string databaseName, string fullPathString, string searchPattern)
        {
            FileInfo result = null;
            var directory = new DirectoryInfo(fullPathString);
            var list = directory.GetFiles(searchPattern);
            if (list.Count() > 0)
            {
                result = list.Where(s => s.Name.StartsWith(databaseName)).OrderByDescending(f => f.Name).FirstOrDefault();
            }
            if (result != null)
            {
                Logger.LogDebug(string.Format("Found {0} in {1} searching for {2}(...){3}", result.Name, fullPathString, databaseName, searchPattern.Replace("*.", ".")));
                return result.Name;
            }
            else
            {
                Logger.LogDebug(string.Format("Found nothing in {0} searching for {1}(...){2}", fullPathString, databaseName, searchPattern.Replace("*.", ".")));
                return string.Empty;
            }
        }

        private void Action_IO_DeleteAllFilesExcept(string fileName, string fullPathString, string searchPattern)
        {
            List<string> files;
            if (string.IsNullOrWhiteSpace(fileName))
            {
                files = Directory.EnumerateFiles(fullPathString,searchPattern).ToList();
            }
            else
            {
                files = Directory.EnumerateFiles(fullPathString, searchPattern).Where(s => !s.EndsWith(fileName)).ToList();
            }
            files.ForEach(x => { try { System.IO.File.Delete(x); Logger.LogDebug(string.Format("Deleted file {0}.", x)); } catch { } });
            Logger.LogDebug(string.Format("For {0} except {1} in pattern {2}.", fullPathString, fileName, searchPattern));
        }

        /* Create local directories if not existing as this might be first time running and 
        *  returns false if no file is found and true if one is found */
        private bool Action_IO_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles(ConfigurationForDatabase configuredDatabase, string searchPattern)
        {
            Logger.LogDebug(string.Format("Started for {0}", configuredDatabase.DatabaseName));

            try
            {
                string databaseNameString = configuredDatabase.DatabaseName.ToString();
                UncPath remoteLocalTransferDirectory = configuredDatabase.RemoteLocalTransferDirectoryWithSubDirectory;
                string remoteLocalTransferDirectoryNewestFileName = string.Empty;
                DirectoryPath localRemoteTransferDirectory = configuredDatabase.LocalRemoteTransferDirectoryWithSubDirectory;
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger.InternalLogger, localRemoteTransferDirectory);
                string localRemoteTransferDirectoryNewestFileName = string.Empty;
                DirectoryPath localRemoteDeliveryDirectory = configuredDatabase.LocalRemoteDeliveryDirectoryWithSubDirectory;
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger.InternalLogger, localRemoteDeliveryDirectory);
                string localRemoteDeliveryDirectoryNewestFileName = string.Empty;
                DirectoryPath localRestoreDirectory = configuredDatabase.LocalRestoreDirectoryWithSubDirectory;
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger.InternalLogger, localRestoreDirectory);
                string localRestoreDirectoryNewestFileName = string.Empty;
                if (Information_RemoteServer_HasAccess())
                {
                    try
                    {
                        remoteLocalTransferDirectoryNewestFileName = Information_IO_GetNewesteFilename(databaseNameString, remoteLocalTransferDirectory.ToString(), searchPattern);
                    }
                    catch (Exception)
                    {
                        Logger.LogWarning(string.Format("Could not access remote directory {0} but have access to server.", remoteLocalTransferDirectory.ToString()));
                    }
                }
                localRemoteTransferDirectoryNewestFileName = Information_IO_GetNewesteFilename(databaseNameString, localRemoteTransferDirectory.ToString(),searchPattern);
                localRemoteDeliveryDirectoryNewestFileName = Information_IO_GetNewesteFilename(databaseNameString, localRemoteDeliveryDirectory.ToString(), searchPattern);
                localRestoreDirectoryNewestFileName = Information_IO_GetNewesteFilename(databaseNameString, localRestoreDirectory.ToString(),searchPattern);
                long remoteLocalTransferDirectoryNewestValue = Information_IO_GetFileTimePart(remoteLocalTransferDirectoryNewestFileName);
                long localRemoteTransferDirectoryNewestValue = Information_IO_GetFileTimePart(localRemoteTransferDirectoryNewestFileName);
                long localRemoteDeliveryDirectoryNewestValue = Information_IO_GetFileTimePart(localRemoteDeliveryDirectoryNewestFileName);
                long localRestoreDirectoryNewestValue = Information_IO_GetFileTimePart(localRestoreDirectoryNewestFileName);

                /* Add all to array and select max */
                long[] values = new long[] { remoteLocalTransferDirectoryNewestValue, localRemoteTransferDirectoryNewestValue, localRemoteDeliveryDirectoryNewestValue, localRestoreDirectoryNewestValue };
                long maxValue = values.Max();

                if (maxValue == 0)
                {
                    /* No files found so none moved */
                    Logger.LogDebug("No files found so nothing was moved.");
                    return false;
                }
                else
                {
                    /* Match for nearest to home first */
                    bool found = false;
                    #region Local Restore

                    if (localRestoreDirectoryNewestValue == maxValue)
                    {
                        found = true;
                        Logger.LogInfo(string.Format("Backup file {0} found in {1}.", localRestoreDirectoryNewestFileName, localRestoreDirectory.ToString()));
                        /* delete all files */
                        Action_IO_DeleteAllFilesExcept(localRestoreDirectoryNewestFileName, localRestoreDirectory.ToString(),searchPattern);
                        Logger.LogDebug(string.Format("Deleted all files except {0} in {1} of pattern {2}.", localRestoreDirectoryNewestFileName, localRestoreDirectory.ToString(), searchPattern));
                        /* No move action needed */
                    }
                    else if (localRestoreDirectoryNewestValue == 0)
                    {
                        Logger.LogDebug(string.Format("No files found in {0} of pattern {1}.", localRestoreDirectory.ToString(), searchPattern));
                    }
                    else
                    {
                        /* delete all files */
                        Action_IO_DeleteAllFilesExcept(string.Empty, localRestoreDirectory.ToString(), searchPattern);
                        Logger.LogDebug(string.Format("Deleted all files in {0} of pattern {1}.", localRestoreDirectory.ToString(), searchPattern));
                    }

                    #endregion

                    #region Local Remote Delivery

                    if (!found && localRemoteDeliveryDirectoryNewestValue == maxValue)
                    {
                        found = true;
                        Logger.LogInfo(string.Format("Backup file {0} found in {1}.", localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory.ToString()));
                        /* delete all files */
                        Action_IO_DeleteAllFilesExcept(localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory.ToString(), searchPattern);
                        Logger.LogDebug(string.Format("Deleted all files except {0} in {1} of pattern {2}.", localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory.ToString(), searchPattern));
                        /* Move actions needed */
                        Action_IO_MoveFileLocal(localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory, localRestoreDirectory);
                    }
                    else if (localRemoteDeliveryDirectoryNewestValue == 0)
                    {
                        Logger.LogDebug(string.Format("No files found in {0} of pattern {1}.", localRemoteDeliveryDirectory.ToString(), searchPattern));
                    }
                    else
                    {
                        /* delete all files */
                        Action_IO_DeleteAllFilesExcept(string.Empty, localRemoteDeliveryDirectory.ToString(), searchPattern);
                        Logger.LogDebug(string.Format("Deleted all files in {0} of pattern {1}.", localRemoteDeliveryDirectory.ToString(), searchPattern));
                    }

                    #endregion

                    #region Local Remote Transfer

                    if (!found && localRemoteTransferDirectoryNewestValue == maxValue)
                    {
                        found = true;
                        Logger.LogInfo(string.Format("Backup file {0} found in {1}.", localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory.ToString()));
                        /* delete all files */
                        Action_IO_DeleteAllFilesExcept(localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory.ToString(), searchPattern);
                        Logger.LogDebug(string.Format("Deleted all files except {0} in {1} of pattern {2}.", localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory.ToString(), searchPattern));
                        /* Move actions needed */
                        Action_IO_MoveFileLocal(localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory, localRemoteDeliveryDirectory);
                        Action_IO_MoveFileLocal(localRemoteTransferDirectoryNewestFileName, localRemoteDeliveryDirectory, localRestoreDirectory);
                    }
                    else if (localRemoteDeliveryDirectoryNewestValue == 0)
                    {
                        Logger.LogDebug(string.Format("No files found in {0} of pattern {1}.", localRemoteTransferDirectory.ToString(), searchPattern));
                    }
                    else
                    {
                        /* delete all files */
                        Action_IO_DeleteAllFilesExcept(string.Empty, localRemoteTransferDirectory.ToString(), searchPattern);
                        Logger.LogDebug(string.Format("Deleted all files in {0} of pattern {1}.", localRemoteTransferDirectory.ToString(), searchPattern));
                    }

                    #endregion

                    #region Remote Local Transfer

                    if (!found && remoteLocalTransferDirectoryNewestValue == maxValue)
                    {
                        found = true;
                        Logger.LogInfo(string.Format("Backup file {0} found in {1}.", remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory.ToString()));
                        /* delete all files */
                        Action_IO_DeleteAllFilesExcept(remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory.ToString(), searchPattern);
                        Logger.LogDebug(string.Format("Deleted all files except {0} in {1} of pattern {2}.", remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory.ToString(), searchPattern));
                        /* Move actions needed */
                        Action_IO_MoveFileRemoteToLocal(remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory, localRemoteTransferDirectory);
                        Action_IO_MoveFileLocal(remoteLocalTransferDirectoryNewestFileName, localRemoteTransferDirectory, localRemoteDeliveryDirectory);
                        Action_IO_MoveFileLocal(remoteLocalTransferDirectoryNewestFileName, localRemoteDeliveryDirectory, localRestoreDirectory);
                    }
                    else if (localRemoteDeliveryDirectoryNewestValue == 0)
                    {
                        Logger.LogDebug(string.Format("No files found in {0} of pattern {1}.", remoteLocalTransferDirectory.ToString(), searchPattern));
                    }
                    else
                    {
                        /* delete all files */
                        Action_IO_DeleteAllFilesExcept(string.Empty, remoteLocalTransferDirectory.ToString(), searchPattern);
                        Logger.LogDebug(string.Format("Deleted all files in {0} of pattern {1}.", localRemoteTransferDirectory.ToString(), searchPattern));
                    }
                    #endregion

                    return true;
                }
            }
            catch(Exception ex)
            {
                throw new SqlServerMirroringException("Failed", ex);
            }
        }

        private void Action_IO_MoveFileLocal(string fileName, DirectoryPath source, DirectoryPath destination)
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

        private void Action_IO_MoveFileRemoteToLocal(string fileName, UncPath source, DirectoryPath destination)
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

        private void Action_IO_MoveFileLocalToRemote(string fileName, DirectoryPath source, UncPath destination)
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

        private void Action_IO_MoveFileRemoteToRemote(string fileName, UncPath source, UncPath destination)
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

        private void Action_IO_CopyFileLocal(string fileName, DirectoryPath source, DirectoryPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Trying to copy file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()));
                File.Copy(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Copied file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Failed copying file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private long Information_IO_GetFileTimePart(string fileName)
        {
            if(string.IsNullOrWhiteSpace(fileName))
            {
                Logger.LogDebug("FileName empty");
                return 0;
            }
            string preparedFileName = Path.GetFileNameWithoutExtension(fileName);
            Regex regex = new Regex(@"^(?:[\w_][\w_\d]*_)(\d*)$");
            Match match = regex.Match(preparedFileName);
            if(match.Success)
            {
                string capture = match.Groups[1].Value;
                long returnValue;
                if(long.TryParse(capture, out returnValue))
                {
                    Logger.LogDebug(string.Format("Found filename {0} datepart {1}", fileName, returnValue));
                    return returnValue;
                }
            }
            throw new SqlServerMirroringException(string.Format("Failed to extract time part from {0}.", fileName));
        }

        internal void Action_ServerState_Update(ServerState serverState_Active)
        {
            try
            {
                Logger.LogDebug("Started.");
                if (Information_ServerState.MasterDatabaseTablesSafeToAccess)
                {
                    Action_ServerState_Update(LocalMasterDatabase, ServerPlacement.Local, ServerPlacement.Local, Information_RemoteServer_HasAccess(), Information_Instance_ServerRole, Information_ServerState, serverState_Active.ServerStateCount);
                    if (Information_RemoteServer_HasAccess())
                    {
                        Action_ServerState_Update(RemoteMasterDatabase, ServerPlacement.Remote, ServerPlacement.Remote, true, Information_Instance_ServerRole, Information_ServerState, serverState_Active.ServerStateCount);
                    }
                }
                Logger.LogDebug("Ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Failed", ex);
            }
        }

        #endregion
    }
}
