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

namespace MirrorLib
{
    public class SqlServerInstance
    {
        private const string DIRECTORY_SPLITTER = "\\";
        private const string URI_SPLITTER = "\\";
        private const string MASTER_DATABASE = "master";
        private const string DATABASE_ROLE_PRINCIPAL = "Principal";
        private const string DATABASE_ROLE_MIRROR = "Mirror";

        private Server _server;
        private Server _remoteServer;
        private ILogger _logger;
        private ManagedComputer _managedComputer;
        private Dictionary<ServerStateEnum, ServerState> _serverStates;
        private ServerState _activeServerState;
        private ConfigurationForInstance _configurationForInstance;
        private Dictionary<string, ConfigurationForDatabase> _configurationForDatabases;
        private ServerRole _activeServerRole = ServerRole.NotSet;

        public SqlServerInstance(ILogger logger, string connectionString)
        {
            _logger = logger;
            Action_ValidateConnectionStringAndDatabaseConnection(connectionString);
            _server = new Server(new ServerConnection(new SqlConnection(connectionString)));
            _configurationForDatabases = new Dictionary<string, ConfigurationForDatabase>();
            _managedComputer = new ManagedComputer("(local)");
            Action_BuildServerStates();
        }

        #region Public Properties

        public bool Information_HasAccessToRemoteServer()
        {
            try
            {
                if (Information_RemoteServer_InstanceStatus().Equals("Online"))
                {
                    Logger.LogDebug("Access to remote server.");
                    return true;
                }
                else
                {
                    Logger.LogInfo("No access to remote server.");
                    return false;
                }
            }
            catch (Exception)
            {
                Logger.LogInfo("No access to remote server.");
                return false;
            }
        }

        public ServerState Information_ServerState
        {
            get
            {
                if(_activeServerRole == ServerRole.NotSet)
                {
                    Action_RecheckServerRole();
                }
                return _activeServerState;
            }
            set
            {
                _activeServerState = value;
                Logger.LogDebug(string.Format("Active state set to {0}.", _activeServerState));
            }
        }

        private Table LocalServerStateTable
        {
            // TODO nicify
            get;
            set;
        }

        public Dictionary<string, ConfigurationForDatabase> ConfigurationForDatabases
        {
            get
            {
                return _configurationForDatabases;
            }
            set
            {
                _configurationForDatabases = value;
            }
        }

        public ConfigurationForInstance ConfigurationForInstance
        {
            get
            {
                return _configurationForInstance;
            }
            set
            {
                _configurationForInstance = value;
            }
        }

        /* Lists the current MirrorStates of the databases on the instance */
        public IEnumerable<MirrorDatabase> Information_MirrorDatabases
        {
            get
            {
                foreach (Database database in Information_UserDatabases)
                {
                    string mirrorRoleDesc = Information_GetDatabaseMirringRole(new DatabaseName(database.Name));
                    yield return new MirrorDatabase(database, mirrorRoleDesc);
                }
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

        public int Information_ServiceStartTimeout
        {
            get
            {
                /* 120 sec */
                return 120000;
            }
        }

        public int Information_ServiceStartTimeoutStep
        {
            get
            {
                /* 5 sec */
                return 5000;
            }
        }

        public bool Information_IsInDegradedState
        {
            get
            {
                // TODO should probably also check if databases are in correct state
                return Information_ServerState.IsDegradedState;
            }
        }

        public ServerRole Information_ServerRole
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
                    string remoteServerName = ConfigurationForInstance.RemoteServer.ToString();
                    Logger.LogDebug(string.Format("Trying to connect to remote server {0}", remoteServerName));
                    /* Do not validate connection as server might not be installed */
                    SqlConnectionStringBuilder connectionBuilder = new SqlConnectionStringBuilder();
                    connectionBuilder.ConnectTimeout = ConfigurationForInstance.RemoteServerAccessTimeoutSeconds;
                    connectionBuilder.DataSource = remoteServerName;
                    connectionBuilder.IntegratedSecurity = true;
                    _remoteServer = new Server(new ServerConnection(new SqlConnection(connectionBuilder.ToString())));
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

        private ILogger Logger
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
                // TODO Caching
                foreach( Database database in RemoteDatabaseServerInstance.Databases)
                {
                    if(database.Name.Equals(MASTER_DATABASE))
                    {
                        return database;
                    }
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

        #endregion

        #region Public Information Instance methods

        public string Information_InstanceStatus()
        {
            return DatabaseServerInstance.Status.ToString();
        }

        public string Information_RemoteServer_InstanceStatus()
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

        public Dictionary<string, string> Information_SqlAgent()
        {
            Dictionary<string, string> sqlAgentInformation = new Dictionary<string, string>();
            Action_EnableAgentXps();
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

        public bool Information_WindowsAuthentificationActive()
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

        public bool Information_SqlServerAuthentificationActive()
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

        public bool Information_CheckInstanceForMirroring()
        {
            Action_EnableAgentXps();
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
            if (!Action_CheckSqlAgentRunning())
            {
                Logger.LogWarning("Sql Server Agent service is not running");
                return false;
            }
            if (!Action_TestReadWriteAccessToDirectory(ConfigurationForInstance.LocalBackupDirectory))
            {
                Logger.LogWarning(string.Format("Not full access to LocalBackupDirectory: {0}", ConfigurationForInstance.LocalBackupDirectory));
                return false;
            }
            if (!Action_TestReadWriteAccessToDirectory(ConfigurationForInstance.LocalRestoreDirectory))
            {
                Logger.LogWarning(string.Format("Not full access to LocalRestoreDirectory: {0}", ConfigurationForInstance.LocalRestoreDirectory));
                return false;
            }
            if (!Action_TestReadWriteAccessToDirectory(ConfigurationForInstance.LocalShareDirectory))
            {
                Logger.LogWarning(string.Format("Not full access to LocalShareDirectory: {0}", ConfigurationForInstance.LocalShareDirectory));
                return false;
            }
            return true;
        }

        #endregion

        #region Public Action Instance methods

        public void Action_StartPrimary()
        {
            Logger.LogDebug("Action_StartPrimary started");
            Action_StartInitialServerState();
            Information_ServerRole = ServerRole.Primary;
            Action_SwitchOverAllMirrorDatabasesIfPossible(false);
            if (Information_IsValidServerStateChange(ServerStateEnum.PRIMARY_STARTUP_STATE))
            {
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_STARTUP_STATE);
            }
            Logger.LogDebug("Action_StartPrimary ended");
        }

        public void Action_StartSecondary()
        {
            Logger.LogDebug("Action_StartSecondary started");
            Action_StartInitialServerState();
            Information_ServerRole = ServerRole.Secondary;
            Action_SwitchOverAllPrincipalDatabasesIfPossible(false);

            if (Information_IsValidServerStateChange(ServerStateEnum.SECONDARY_STARTUP_STATE))
            {
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_STARTUP_STATE);
            }
            Logger.LogDebug("Action_StartSecondary ended");
        }

        public void Action_SetupMonitoring()
        {
            Logger.LogDebug("Action_SetupMonitoring started");
            foreach (Database database in Information_UserDatabases)
            {
                ConfigurationForDatabase configuredDatabase;
                if (ConfigurationForDatabases.TryGetValue(database.Name, out configuredDatabase))
                {
                    try
                    {
                        throw new NotImplementedException();
                        database.ExecuteNonQuery("EXEC sys.sp_dbmmonitoraddmonitoring " + ConfigurationForInstance.MirrorMonitoringUpdateMinutes);
                        Logger.LogDebug(string.Format("Mirroring monitoring every {0} minutes started {1} with partner endpoint on {2} port {3}"
                            , ConfigurationForInstance.MirrorMonitoringUpdateMinutes, configuredDatabase.DatabaseName, configuredDatabase.RemoteServer, ConfigurationForInstance.Endpoint_ListenerPort));


                        //1 Oldest unsent transaction
                        //Specifies the number of minutes worth of transactions that can accumulate in the send queue
                        //before a warning is generated on the principal server instance.This warning helps measure the
                        //potential for data loss in terms of time, and it is particularly relevant for high - performance mode.
                        //However, the warning is also relevant for high - safety mode when mirroring is paused or suspended because the
                        //partners become disconnected.

                        //--Event ID 32042
                        //EXEC sp_dbmmonitorchangealert DatabaseName, 1, 2, 1 ; --OLDEST UNSENT TRANSACTION (set to 2 minutes)


                        //2 Unsent log
                        //Specifies how many kilobytes(KB) of unsent log generate a warning on the principal server instance.This warning
                        //helps measure the potential for data loss in terms of KB, and it is particularly relevant for high - performance mode.
                        //However, the warning is also relevant for high - safety mode when mirroring is paused or suspended because the partners
                        //become disconnected.

                        //--Event ID 32043
                        //EXEC sp_dbmmonitorchangealert DatabaseName, 2, 10000, 1; --UNSENT LOG SIZE ON P(set to 10000K)

                        //3 Unrestored log
                        //Specifies how many KB of unrestored log generate a warning on the mirror server instance.This warning helps measure
                        //failover time.Failover time consists mainly of the time that the former mirror server requires to roll forward any log
                        //remaining in its redo queue, plus a short additional time.

                        //--Event ID 32044
                        //EXEC sp_dbmmonitorchangealert DatabaseName, 3, 10000, 1; --UNRESTORED LOG ON M(set to 10000K)

                        //4 Mirror commit overhead
                        //Specifies the number of milliseconds of average delay per transaction that are tolerated before a warning is generated
                        //on the principal server.This delay is the amount of overhead incurred while the principal server instance waits for the
                        //mirror server instance to write the transaction's log record into the redo queue. This value is relevant only in high-safety mode.

                        //-- Event ID 32045
                        //EXEC sp_dbmmonitorchangealert DatabaseName, 4, 1000, 1; --MIRRORING COMMIT OVERHEAD (milisecond delay for txn on P_

                        //5 Retention period
                        //Metadata that controls how long rows in the database mirroring status table are preserved.

                        //EXEC sp_dbmmonitorchangealert DatabaseName, 5, 48, 1 ;

                    }
                    catch (Exception ex)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_SetupMonitoring failed for {0}", configuredDatabase.DatabaseName.ToString()), ex);
                    }
                }
            }
            Logger.LogDebug("Action_SetupMonitoring ended");
        }

        public void Action_CheckServerState()
        {
            Logger.LogDebug("Action_CheckServerState started");

            if (!Information_ServerState.IgnoreMirrorStateCheck)
            {
                if (!Information_CheckLocalMasterServerStateTable())
                {
                    Action_CreateMasterServerStateTable();
                }

                bool databaseErrorState = false;
                foreach (MirrorDatabase mirrorDatabase in Information_MirrorDatabases.Where(s => s.IsMirroringEnabled))
                {
                    if (Information_ServerRole == ServerRole.Primary)
                    {
                        if (mirrorDatabase.Status != DatabaseStatus.Normal)
                        {
                            databaseErrorState = true;
                            Logger.LogError(string.Format("Database {0} has error status {1}.", mirrorDatabase.DatabaseName, mirrorDatabase.Status));
                        }
                        else
                        {
                            Logger.LogDebug(string.Format("Database {0} has status {1}.", mirrorDatabase.DatabaseName, mirrorDatabase.Status));
                        }
                    }
                    else if (Information_ServerRole == ServerRole.Secondary)
                    {
                        if (mirrorDatabase.Status != DatabaseStatus.Restoring)
                        {
                            databaseErrorState = true;
                            Logger.LogError(string.Format("Database {0} has error status {1}.", mirrorDatabase.DatabaseName, mirrorDatabase.Status));
                        }
                        else
                        {
                            Logger.LogDebug(string.Format("Database {0} has status {1}.", mirrorDatabase.DatabaseName, mirrorDatabase.Status));
                        }
                    }
                }
                if (databaseErrorState)
                {
                    /* Check last state and state count */
                    if (Action_UpdateLastDatabaseStateErrorAndCount_ShiftState())
                    {
                        Action_ForceShutDownMirroringService();
                    }
                    else
                    {
                        Logger.LogDebug(string.Format("Found Server Role {0}, Server State {1} and will shut down after {2} checks.", Information_ServerRole, Information_ServerState, ConfigurationForInstance.ShutDownAfterNumberOfChecksForDatabaseState));
                    }
                }
                else
                {
                    Action_ResetLastDatabaseStateErrorAndCount();
                }

                /* Check remote server access */
                if (Information_HasAccessToRemoteServer())
                {
                    Action_UpdateLocalServerState_ConnectedRemoteServer();
                    Action_UpdateRemoteServerState_ConnectedRemoteServer();
                    if (Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE)
                    {
                        Action_UpdateServerState(LocalMasterDatabase, true, true, false, Information_ServerRole, Information_ServerState, 0);
                        Action_MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_STATE);
                    }
                    else if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE)
                    {
                        Action_UpdateServerState(LocalMasterDatabase, true, true, false, Information_ServerRole, Information_ServerState, 0);
                        Action_MakeServerStateChange(ServerStateEnum.SECONDARY_RUNNING_STATE);
                    }
                }
                else
                {
                    Action_UpdateLocalServerState_MissingRemoteServer();
                    if (Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_STATE)
                    {
                        Action_MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE);
                    }
                    else if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_STATE)
                    {
                        Action_MakeServerStateChange(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE);
                    }
                    else if(Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE)
                    {
                        if(Action_UpdatePrimaryRunningNoSecondaryCount_ShiftState())
                        {
                            Action_MakeServerStateChange(ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE);
                        }
                    }
                    else if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE)
                    {
                        if(Action_UpdateSecondaryRunningNoPrimaryCount_ShiftState())
                        {
                            Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                        }
                    }

                }
            }
            else
            {
                Logger.LogDebug(string.Format("Ignores Action_CheckMirrorState because server state is {0}.", Information_ServerState));
            }
            Logger.LogDebug("Action_CheckServerState ended");
        }

        public bool Action_ResumeMirroringForAllDatabases()
        {
            Logger.LogDebug("Action_ResumeMirroringForAllDatabases started");
            foreach (ConfigurationForDatabase configuredDatabase in ConfigurationForDatabases.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).First();

                    database.ChangeMirroringState(MirroringOption.Resume);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Resume failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            Logger.LogDebug("Action_ResumeMirroringForAllDatabases ended");
            return true;
        }

        public bool Action_SuspendMirroringForAllMirrorDatabases()
        {
            Logger.LogDebug("Action_SuspendMirroringForAllMirrorDatabases started");

            foreach (ConfigurationForDatabase configuredDatabase in ConfigurationForDatabases.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).First();

                    database.ChangeMirroringState(MirroringOption.Suspend);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Suspend failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            Logger.LogDebug("Action_SuspendMirroringForAllMirrorDatabases ended");
            return true;
        }

        public bool Action_ForceFailoverWithDataLossForAllMirrorDatabases()
        {
            Logger.LogDebug("Action_ForceFailoverWithDataLossForAllMirrorDatabases started");

            if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_STATE ||
            Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE ||
            Information_ServerState.State == ServerStateEnum.SECONDARY_MAINTENANCE_STATE)
            {
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE);
            }
            else
            {
                Logger.LogWarning(string.Format("Server {0} triet to manually shut down but in invalid state {1} for doing so.", Information_ServerRole, Information_ServerState));
                return false;
            }
            /* TODO Only fail over the first as all others will join if on multi mirror server with witness */
            foreach (ConfigurationForDatabase configuredDatabase in ConfigurationForDatabases.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).First();

                    database.ChangeMirroringState(MirroringOption.ForceFailoverAndAllowDataLoss);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("ForceFailoverWithDataLoss failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            if (Information_ServerState.State == ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE)
            {
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
            Logger.LogDebug("Action_ForceFailoverWithDataLossForAllMirrorDatabases ended");
            return true;
        }

        public bool Action_FailoverForAllMirrorDatabases()
        {
            Logger.LogDebug("Action_FailoverForAllMirrorDatabases started");
            if (Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_STATE ||
                Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE ||
                Information_ServerState.State == ServerStateEnum.PRIMARY_MAINTENANCE_STATE)
            {
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE);
            }
            else if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_STATE ||
            Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE ||
            Information_ServerState.State == ServerStateEnum.SECONDARY_MAINTENANCE_STATE)
            {
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE);
            }
            else
            {
                Logger.LogWarning(string.Format("Server {0} triet to manually shut down but in invalid state {1} for doing so.", Information_ServerRole, Information_ServerState));
                return false;
            }


            /* TODO Only fail over the fist as all others will join if in witness mode */
            foreach (ConfigurationForDatabase configuredDatabase in ConfigurationForDatabases.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).First();
                    database.ChangeMirroringState(MirroringOption.Failover);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Failover failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            if (Information_ServerState.State == ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE)
            {
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
            else if (Information_ServerState.State == ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE)
            {
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
            Logger.LogDebug("Action_FailoverForAllMirrorDatabases ended");

            return true;
        }

        public bool Action_BackupForAllMirrorDatabases()
        {
            Logger.LogDebug("Action_BackupForAllMirrorDatabases started");
            foreach (ConfigurationForDatabase configuredDatabase in ConfigurationForDatabases.Values)
            {
                try
                {
                    Action_BackupDatabaseForMirrorServer(configuredDatabase);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Backup failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            Logger.LogDebug("Action_BackupForAllMirrorDatabases ended");
            return true;
        }

        public bool Action_ShutDownMirroringService()
        {
            Logger.LogDebug("Action_ShutDownMirroringService started");
            if (Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_STATE ||
                Information_ServerState.State == ServerStateEnum.PRIMARY_MAINTENANCE_STATE ||
                Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE ||
                Information_ServerState.State == ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE)
            {
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_STATE ||
                Information_ServerState.State == ServerStateEnum.SECONDARY_MAINTENANCE_STATE ||
                Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE)
            {
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else
            {
                Logger.LogWarning(string.Format("Shutdown tried done from invalid state {0}. If this is needed make a force shutdown.", Information_ServerState));
                return false;
            }
        }

        public bool Action_ForceShutDownMirroringService()
        {
            Logger.LogDebug("Action_ForceShutDownMirroringService started");
            if (Information_ServerRole == ServerRole.Primary || Information_ServerRole == ServerRole.MainlyPrimary)
            {
                Logger.LogWarning(string.Format("Force shutdown of service with role {0} in state {1}.", Information_ServerRole, Information_ServerState));
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else if (Information_ServerRole == ServerRole.Secondary || Information_ServerRole == ServerRole.MainlySecondary)
            {
                Logger.LogWarning(string.Format("Force shutdown of service with role {0} in state {1}.", Information_ServerRole, Information_ServerState));
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else
            {
                Logger.LogWarning(string.Format("Force shutdown of service with role {0} in state {1}.", Information_ServerRole, Information_ServerState));
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                return true;
            }
        }

        #endregion

        #region Private Instance Methods

        private bool Action_TestReadWriteAccessToDirectory(DirectoryPath directoryPath)
        {
            try
            {
                DirectoryHelper.TestReadWriteAccessToDirectory(Logger, directoryPath);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private void Action_StartUpMirrorCheck(Dictionary<string, ConfigurationForDatabase> configuredMirrorDatabases, bool serverPrimary)
        {
            Logger.LogDebug(string.Format("Action_StartUpMirrorCheck check if databases mirrored but not configured"));
            foreach (MirrorDatabase mirrorState in Information_MirrorDatabases)
            {
                Logger.LogDebug(string.Format("Checking database {0}", mirrorState.DatabaseName));
                if (mirrorState.IsMirroringEnabled)
                {
                    if (!configuredMirrorDatabases.ContainsKey(mirrorState.DatabaseName.ToString()))
                    {
                        Logger.LogWarning(string.Format("Database {0} was set up for mirroring but is not in configuration", mirrorState.DatabaseName));
                        Action_RemoveDatabaseFromMirroring(mirrorState, serverPrimary);
                    }
                    else
                    {
                        Logger.LogDebug(string.Format("Database {0} was setup for mirroring and enabled.", mirrorState.DatabaseName));
                    }
                }
                else
                {
                    Logger.LogDebug(string.Format("Database {0} was not setup for mirroring and not configured for it.", mirrorState.DatabaseName));
                }

            }
            Logger.LogDebug(string.Format("Action_StartUpMirrorCheck check if databases needs to be set up"));
            /* Check databases setup */
            foreach (ConfigurationForDatabase configurationDatabase in ConfigurationForDatabases.Values)
            {
                Logger.LogDebug(string.Format("Checking database {0}", configurationDatabase.DatabaseName));
                MirrorDatabase mirrorDatabase = Information_MirrorDatabases.Where(s => s.DatabaseName.ToString().Equals(configurationDatabase.DatabaseName.ToString())).FirstOrDefault();
                if(mirrorDatabase == null || !mirrorDatabase.IsMirroringEnabled)
                {
                    Logger.LogWarning(string.Format("Database {0} is not set up for mirroring but is in configuration", configurationDatabase.DatabaseName));
                    Action_AddDatabaseToMirroring(configurationDatabase, serverPrimary);
                }
                else
                {
                    Logger.LogDebug(string.Format("Database {0} is set up for mirroring and is in configuration", configurationDatabase.DatabaseName));
                }
            }
            Logger.LogDebug(string.Format("Action_StartUpMirrorCheck ended"));
        }

        private bool Information_IsValidServerStateChange(ServerStateEnum newState)
        {
            return Information_ServerState.ValidTransition(newState);
        }

        private void Action_SetupInstanceForMirroring()
        {
            Action_EnableAgentXps();
            if (DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Manual ||
                DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Disabled)
            {
                // TODO fix security issue (guess)
                Logger.LogDebug("Bug: Cannot change to automatic start");
                Action_ChangeDatabaseServiceToAutomaticStart();
                Logger.LogInfo("Sql Server was set to Automatic start");
            }
            if (!DatabaseServerInstance.JobServer.SqlAgentAutoStart)
            {
                // TODO fix security issue (guess)
                Logger.LogDebug("Bug: Cannot change to automatic start");
                Action_ChangeSqlAgentServiceToAutomaticStart();
                Logger.LogInfo("Sql Agent was set to Automatic start");
            }
            if (!Action_CheckSqlAgentRunning())
            {
                // TODO fix security issue (guess)
                Logger.LogDebug("Bug: Cannot start service");
                Action_StartSqlAgent();
                Logger.LogInfo("Sql Agent service was started");
            }
            if (Information_EndpointExists(ConfigurationForInstance.Endpoint_Name))
            {
                if (Information_EndpointStarted(ConfigurationForInstance.Endpoint_Name))
                {
                    Logger.LogDebug(string.Format("Mirroring endpoint {0} exists", ConfigurationForInstance.Endpoint_Name));
                }
                else
                {
                    Action_StartEndpoint(ConfigurationForInstance.Endpoint_Name);
                }
            }
            else
            {
                Action_CreateEndpoint();
            }


        }

        private void Action_ResetServerRole()
        {
            _activeServerRole = ServerRole.NotSet;
        }

        private void Action_RecheckServerRole()
        {
            int isPrincipal = 0;
            int isMirror = 0;
            foreach (MirrorDatabase mirrorState in Information_MirrorDatabases.Where(s => s.IsMirroringEnabled))
            {
                if (mirrorState.IsPrincipal)
                {
                    isPrincipal += 1;
                }
                if (mirrorState.IsMirror)
                {
                    isMirror += 1;
                }
            }
            if (isMirror == 0 && isPrincipal > 0)
            {
                /* Sure primary */
                _activeServerRole = ServerRole.Primary;
            }
            else if (isPrincipal == 0 && isMirror > 0)
            {
                /* Sure secondary */
                _activeServerRole = ServerRole.Secondary;
            }
            else if (isPrincipal > isMirror)
            {
                /* Mainly primary */
                Logger.LogWarning("Server is mainly in primary role but has mirror databases. Make switchover to bring to correct state.");
                _activeServerRole = ServerRole.MainlyPrimary;
            }
            else if (isMirror < isPrincipal)
            {
                /* Mainly secondary */
                Logger.LogWarning("Server is mainly in secondary role but has principal databases. Make switchover to bring to correct state.");
                _activeServerRole = ServerRole.MainlySecondary;
            }
            else
            {
                /* Neither */
                Logger.LogWarning("Server has no principal or mirror database. Investigate what went wrong.");
                _activeServerRole = ServerRole.Neither;
            }
        }

        private void Action_SwitchOverAllDatabasesIfPossible(bool failoverPrincipal, bool failIfNotSwitchingOver)
        {
            string failoverRole;
            string ignoreRole;
            if(failoverPrincipal)
            {
                failoverRole = DATABASE_ROLE_PRINCIPAL;
                ignoreRole = DATABASE_ROLE_MIRROR;
            }
            else
            {
                failoverRole = DATABASE_ROLE_MIRROR;
                ignoreRole = DATABASE_ROLE_PRINCIPAL;
            }
            foreach (ConfigurationForDatabase configuredDatabase in ConfigurationForDatabases.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                    if (database == null)
                    {
                        if (failIfNotSwitchingOver)
                        {
                            throw new SqlServerMirroringException(string.Format("Did not switch database {0} as it is unknown.", configuredDatabase.DatabaseName));
                        }
                    }
                    else
                    {
                        string databaseRole = Information_GetDatabaseMirringRole(new DatabaseName(database.Name));
                        if (databaseRole.Equals(failoverRole))
                        {
                            Logger.LogDebug(string.Format("Trying to switch {0} as it in {1}.", database.Name, databaseRole));
                            database.ChangeMirroringState(MirroringOption.Failover);
                            database.Alter(TerminationClause.RollbackTransactionsImmediately);
                            Logger.LogDebug(string.Format("Database {0} switched from {1} to {2}.", database.Name, databaseRole, failoverRole));
                        }
                        else if (databaseRole.Equals(ignoreRole))
                        {
                            Logger.LogDebug(string.Format("Did not switch {0} as it is already in {1}.", database.Name, databaseRole));
                        }
                        else
                        {
                            if (failIfNotSwitchingOver)
                            {
                                throw new SqlServerMirroringException(string.Format("Did not switch {0} as role {1} is unknown.", database.Name, databaseRole));
                            }
                            else
                            {
                                Logger.LogWarning(string.Format("Did not switch {0} as role {1} is unknown.", database.Name, databaseRole));
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (failIfNotSwitchingOver)
                    {
                        throw new SqlServerMirroringException(string.Format("Failover failed for {0}", configuredDatabase.DatabaseName), ex);
                    }
                }
            }
            Action_RecheckServerRole();
        }

        private void Action_SwitchOverAllPrincipalDatabasesIfPossible(bool failIfNotSwitchingOver)
        {
            Action_SwitchOverAllDatabasesIfPossible(true, failIfNotSwitchingOver);
        }

        private void Action_SwitchOverAllMirrorDatabasesIfPossible(bool failIfNotSwitchingOver)
        {
            Action_SwitchOverAllDatabasesIfPossible(false, failIfNotSwitchingOver);
        }

        private void Action_MakeServerStateChange(ServerStateEnum newState)
        {
            if (Information_IsValidServerStateChange(newState))
            {
                ServerState newServerState;
                if (_serverStates.TryGetValue(newState, out newServerState))
                {
                    Logger.LogInfo(string.Format("Server state changed from {0} to {1}.", Information_ServerState, newServerState));
                    Information_ServerState = newServerState;
                    Action_StartNewServerState(newState);
                    Logger.LogDebug(string.Format("Server in new state {0}.", newServerState));
                }
                else
                {
                    throw new SqlServerMirroringException(string.Format("Server in state {0} could not get new state {1}.", Information_ServerState, newState));
                }
            }
            else
            {
                throw new SqlServerMirroringException(string.Format("Server in state {0} does not allow state shange to {1}.", Information_ServerState, newState));
            }
        }

        private void Action_ChangeSqlAgentServiceToAutomaticStart()
        {
            /* TODO: Check that it is the correct instance sql server */
            foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
            {
                Logger.LogDebug(string.Format("Checking Sql Agent {0}({1}) in startup state {2} before change", service.Name, service.DisplayName, service.StartMode));
                //ServiceController sc = new ServiceController(service.Name);
                service.StartMode = Microsoft.SqlServer.Management.Smo.Wmi.ServiceStartMode.Auto;
                service.Alter();
                Logger.LogInfo(string.Format("Checking Sql Agent {0}({1}) in startup state {2} after change", service.Name, service.DisplayName, service.StartMode));
            }
        }

        private void Action_ChangeDatabaseServiceToAutomaticStart()
        {
            /* TODO: Check that it is the correct instance sql server */
            foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlServer))
            {
                Logger.LogDebug(string.Format("Checking Sql Server {0}({1}) in startup state {2} before change", service.Name, service.DisplayName, service.StartMode));
                service.StartMode = Microsoft.SqlServer.Management.Smo.Wmi.ServiceStartMode.Auto;
                service.Alter();
                Logger.LogInfo(string.Format("Checking Sql Server {0}({1}) in startup state {2} after change", service.Name, service.DisplayName, service.StartMode));
            }
        }

        private void Action_StartSqlAgent()
        {
            /* TODO: Check that it is the correct instance sql agent */
            foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
            {
                Logger.LogDebug(string.Format("Checking Sql Agent {0}({1}) in state {2}", service.Name, service.DisplayName, service.ServiceState));
                if (service.ServiceState != ServiceState.Running)
                {
                    int timeoutCounter = 0;
                    service.Start();
                    while (service.ServiceState != ServiceState.Running && timeoutCounter > Information_ServiceStartTimeout)
                    {
                        timeoutCounter += Information_ServiceStartTimeoutStep;
                        System.Threading.Thread.Sleep(Information_ServiceStartTimeoutStep);
                        Console.WriteLine(string.Format("Waited {0} seconds for Sql Agent {1}({2}) starting: {3}", (timeoutCounter / 1000), service.Name, service.DisplayName, service.ServiceState));
                        service.Refresh();
                    }
                    if (timeoutCounter > Information_ServiceStartTimeout)
                    {
                        throw new SqlServerMirroringException(string.Format("Timed out waiting for Sql Agent {1}({2}) starting", service.Name, service.DisplayName));
                    }
                }
            }
        }

        private bool Action_CheckSqlAgentRunning()
        {
            /* TODO: Check that it is the correct instance sql agent */
            foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
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

        private void Action_EnableAgentXps()
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

        private void Action_StartNewServerState(ServerStateEnum newState)
        {
            switch (newState)
            {
                case ServerStateEnum.PRIMARY_STARTUP_STATE:
                    Action_StartPrimaryStartupState();
                    break;
                case ServerStateEnum.PRIMARY_RUNNING_STATE:
                    Action_StartPrimaryRunningState();
                    break;
                case ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE:
                    Action_StartPrimaryForcedRunningState();
                    break;
                case ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE:
                    Action_StartPrimaryShuttingDownState();
                    break;
                case ServerStateEnum.PRIMARY_SHUTDOWN_STATE:
                    Action_StartPrimaryShutdownState();
                    break;
                case ServerStateEnum.PRIMARY_MAINTENANCE_STATE:
                    Action_StartPrimaryMaintenanceState();
                    break;
                case ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE:
                    Action_StartPrimaryManualFailoverState();
                    break;
                case ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE:
                    Action_StartPrimaryRunningNoSecondaryState();
                    break;
                case ServerStateEnum.SECONDARY_STARTUP_STATE:
                    Action_StartSecondaryStartupState();
                    break;
                case ServerStateEnum.SECONDARY_RUNNING_STATE:
                    Action_StartSecondaryRunningState();
                    break;
                case ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE:
                    Action_StartSecondaryRunningNoPrimaryState();
                    break;
                case ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE:
                    Action_StartSecondaryShuttingDownState();
                    break;
                case ServerStateEnum.SECONDARY_SHUTDOWN_STATE:
                    Action_StartSecondaryShutdownState();
                    break;
                case ServerStateEnum.SECONDARY_MAINTENANCE_STATE:
                    Action_StartSecondaryMaintenanceState();
                    break;
                case ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE:
                    Action_StartSecondaryManualFailoverState();
                    break;
                case ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE:
                    Action_StartSecondaryForcedManualFailoverState();
                    break;
                default:
                    throw new SqlServerMirroringException(string.Format("Unknown state {0}.", newState.ToString()));
            }
        }

        private void Action_StartSecondaryRunningNoPrimaryState()
        {
            Logger.LogDebug("Action_StartSecondaryRunningNoPrimaryState starting");
            try
            {
                Logger.LogDebug("Action_StartSecondaryRunningNoPrimaryState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Running State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartSecondaryShuttingDownState()
        {
            Logger.LogDebug("StartSecondaryShuttingDownState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartSecondaryShuttingDownState ended");
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTDOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTDOWN_STATE);
            }
        }

        private void Action_StartSecondaryShutdownState()
        {
            Logger.LogDebug("Action_StartSecondaryShutdownState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("Action_StartSecondaryShutdownState ended");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutdown State could not be ended.", ex);
            }
        }

        private void Action_StartSecondaryMaintenanceState()
        {
            Logger.LogDebug("Action_StartSecondaryMaintenanceState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("Action_StartSecondaryMaintenanceState starting");
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartSecondaryManualFailoverState()
        {
            Logger.LogDebug("Action_StartSecondaryManualFailoverState starting");
            try
            {
                Action_FailoverForAllMirrorDatabases();
                Logger.LogDebug("Action_StartSecondaryManualFailoverState ended");
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Secondary Manual Failover State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartSecondaryRunningState()
        {
            Logger.LogDebug("StartSecondaryRunningState starting");
            try
            {
                if (!(Information_ServerRole == ServerRole.Secondary))
                {
                    Action_MakeServerStateChange(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE);
                }
                Logger.LogDebug("StartSecondaryRunningState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Running State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartSecondaryStartupState()
        {
            Logger.LogDebug("StartStartupState starting");
            try
            {
                if (!Information_CheckInstanceForMirroring())
                {
                    Action_SetupInstanceForMirroring();
                }
                Action_StartUpMirrorCheck(ConfigurationForDatabases, false);
                Action_SwitchOverAllPrincipalDatabasesIfPossible(false);
                Logger.LogDebug("StartStartupState ended");
                if (Information_HasAccessToRemoteServer())
                {
                    Action_MakeServerStateChange(ServerStateEnum.SECONDARY_RUNNING_STATE);
                }
                else
                {
                    Action_MakeServerStateChange(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError("Secondary Startup State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartPrimaryRunningNoSecondaryState()
        {
            Logger.LogDebug("Action_StartPrimaryRunningNoSecondaryState starting");
            try
            {
                Logger.LogDebug("Action_StartPrimaryRunningNoSecondaryState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Running State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartPrimaryStartupState()
        {
            Logger.LogDebug("StartStartupState starting");
            try
            {
                if (!Information_CheckInstanceForMirroring())
                {
                    Action_SetupInstanceForMirroring();
                }
                Action_StartUpMirrorCheck(ConfigurationForDatabases, true);
                Action_SwitchOverAllMirrorDatabasesIfPossible(true);
                Logger.LogDebug("StartStartupState ended");
                if (Information_HasAccessToRemoteServer())
                {
                    Action_MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_STATE);
                }
                else
                {
                    Action_MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError("Primary Startup State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartPrimaryRunningState()
        {
            Logger.LogDebug("StartRunningState starting");
            try
            {
                if (!(Information_ServerRole == ServerRole.Primary))
                {
                    Action_MakeServerStateChange(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE);
                }
                Logger.LogDebug("StartRunningState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Running State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartPrimaryForcedRunningState()
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
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartPrimaryShuttingDownState()
        {
            Logger.LogDebug("StartShuttingDownState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartShuttingDownState ended");
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTDOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTDOWN_STATE);
            }
        }

        private void Action_StartPrimaryShutdownState()
        {
            Logger.LogDebug("StartShutdownState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartShutdownState ended");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutdown State could not be ended.", ex);
            }
        }

        private void Action_StartPrimaryMaintenanceState()
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
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartPrimaryManualFailoverState()
        {
            Logger.LogDebug("StartPrimaryManualFailoverState starting");
            try
            {
                Action_FailoverForAllMirrorDatabases();
                Logger.LogDebug("StartPrimaryManualFailoverState ended");
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Primary Manual Failover State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_StartSecondaryForcedManualFailoverState()
        {
            Logger.LogDebug("StartForcedManualFailoverState starting");
            try
            {
                Action_ForceFailoverWithDataLossForAllMirrorDatabases();
                Logger.LogDebug("StartForcedManualFailoverState ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                Action_MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ValidateConnectionStringAndDatabaseConnection(string connectionString)
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

        private void Action_BuildServerStates()
        {
            _serverStates = new Dictionary<ServerStateEnum, ServerState>();

            /* Add Initial State */
            _serverStates.Add(ServerStateEnum.INITIAL_STATE
                , new ServerState(ServerStateEnum.INITIAL_STATE, true, true, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_STARTUP_STATE, ServerStateEnum.SECONDARY_STARTUP_STATE}));

            /* Add Primary Role server states */
            _serverStates.Add(ServerStateEnum.PRIMARY_STARTUP_STATE
                , new ServerState(ServerStateEnum.PRIMARY_STARTUP_STATE, true, true, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE, ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_RUNNING_STATE
                , new ServerState(ServerStateEnum.PRIMARY_RUNNING_STATE, false, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_MAINTENANCE_STATE, ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE
                , ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE, ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE }));

            _serverStates.Add(ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE
                , new ServerState(ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE, true, false, new List<ServerStateEnum>()
                {ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_MAINTENANCE_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE }));

            _serverStates.Add(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE
                , new ServerState(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, true, true, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTDOWN_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_SHUTDOWN_STATE
                , new ServerState(ServerStateEnum.PRIMARY_SHUTDOWN_STATE, true, true, new List<ServerStateEnum>()
                { }));

            _serverStates.Add(ServerStateEnum.PRIMARY_MAINTENANCE_STATE
                , new ServerState(ServerStateEnum.PRIMARY_MAINTENANCE_STATE, false, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE, ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE
                , ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE, false, true, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE
                , new ServerState(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE, false, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE, ServerStateEnum.PRIMARY_MAINTENANCE_STATE
                , ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE, ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE}));


            /* Add Secondary Role server states */
            _serverStates.Add(ServerStateEnum.SECONDARY_STARTUP_STATE
                , new ServerState(ServerStateEnum.SECONDARY_STARTUP_STATE, true, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_RUNNING_STATE, ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_RUNNING_STATE
                , new ServerState(ServerStateEnum.SECONDARY_RUNNING_STATE, false, false, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_MAINTENANCE_STATE, ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE
                , ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE }));

            _serverStates.Add(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE
                , new ServerState(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE, true, false, new List<ServerStateEnum>()
                {ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE
                , ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, ServerStateEnum.SECONDARY_RUNNING_STATE }));

            _serverStates.Add(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE
                , new ServerState(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, true, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTDOWN_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_SHUTDOWN_STATE
                , new ServerState(ServerStateEnum.SECONDARY_SHUTDOWN_STATE, true, true, new List<ServerStateEnum>()
                { }));

            _serverStates.Add(ServerStateEnum.SECONDARY_MAINTENANCE_STATE
                , new ServerState(ServerStateEnum.SECONDARY_MAINTENANCE_STATE, false, false, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_RUNNING_STATE
                , ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, false, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE, true, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE}));

            /* Set default state */
            if (!_serverStates.TryGetValue(ServerStateEnum.INITIAL_STATE, out _activeServerState))
            {
                throw new SqlServerMirroringException("Could not get Initial Server State.");
            }
        }

        private void Action_StartInitialServerState()
        {
            if (ConfigurationForInstance == null ||ConfigurationForDatabases == null || ConfigurationForDatabases.Count == 0)
            {
                throw new SqlServerMirroringException("Configuration not set before Initial server state");
            }

            /* Create Master Database ServerState */
            if (!Information_CheckLocalMasterServerStateTable())
            {
                Action_CreateMasterServerStateTable();
            }

            if (!Information_CheckLocalMasterDatabaseStateErrorTable())
            {
                Action_CreateMasterDatabaseStateErrorTable();
            }
            Action_CreateDirectoryAndShare();
            Logger.LogDebug("Action_StartInitialServerState ended.");
        }

        private void Action_CreateDirectoryAndShare()
        {
            Logger.LogDebug(string.Format("Action_CreateDirectoryAndShare started"));
            try
            {
                Logger.LogDebug(string.Format("Creating LocalBackupDirectory: {0}", ConfigurationForInstance.LocalBackupDirectory));
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, ConfigurationForInstance.LocalBackupDirectory);
                Logger.LogDebug(string.Format("Creating LocalRestoreDirectory: {0}", ConfigurationForInstance.LocalRestoreDirectory));
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, ConfigurationForInstance.LocalRestoreDirectory);
                Logger.LogDebug(string.Format("Creating LocalShareDirectory {0} and LocalShare {1}", ConfigurationForInstance.LocalRestoreDirectory, ConfigurationForInstance.LocalShareName));
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(Logger, ConfigurationForInstance.LocalShareDirectory, ConfigurationForInstance.LocalShareName);
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_CreateDirectoryAndShare failed", ex);
            }
            Logger.LogDebug(string.Format("Action_CreateDirectoryAndShare ended"));
        }

        private bool Information_CheckLocalMasterDatabaseStateErrorTable()
        {
            foreach (Table serverStateTable in LocalMasterDatabase.Tables)
            {
                if (serverStateTable.Name == "DatabaseStateError")
                {
                    return true;
                }
            }
            return false;
        }

        private void Action_CreateMasterDatabaseStateErrorTable()
        {
            Logger.LogDebug("Action_CreateMasterDatabaseStateErrorTable started.");

            try
            {
                Table localServerStateTable = new Table(LocalMasterDatabase, "DatabaseStateError");
                Column column1 = new Column(localServerStateTable, "LastRole", DataType.NVarChar(50));
                column1.Nullable = false;
                localServerStateTable.Columns.Add(column1);
                Column column2 = new Column(localServerStateTable, "LastWriteDate", DataType.DateTime2(7));
                column2.Nullable = false;
                localServerStateTable.Columns.Add(column2);
                Column column3 = new Column(localServerStateTable, "ErrorCount", DataType.Int);
                column3.Nullable = false;
                localServerStateTable.Columns.Add(column3);

                localServerStateTable.Create();
                LocalServerStateTable = localServerStateTable;
                Logger.LogDebug("Action_CreateMasterDatabaseStateErrorTable ended.");
                Action_InsertDatabaseStateErrorBaseState();
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_CreateMasterDatabaseStateErrorTable failed", ex);
            }
        }

        private void Action_InsertDatabaseStateErrorBaseState()
        {
            try
            {
                Logger.LogDebug("Action_InsertDatabaseStateErrorBaseState started.");

                string sqlQuery = "INSERT INTO DatabaseStateError (LastRole, LastWriteDate, ErrorCount) ";
                sqlQuery += "VALUES ";
                sqlQuery += "('NotSet',SYSDATETIME(),0)";

                LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                Logger.LogDebug("Action_InsertDatabaseStateErrorBaseState ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_InsertDatabaseStateErrorBaseState failed", ex);
            }
        }

        private void Action_UpdateDatabaseStateError(ServerRole activeServerRole, int increaseCount)
        {
            try
            {
                Logger.LogDebug("Action_UpdateDatabaseStateError started.");

                string sqlQuery = "UPDATE DatabaseStateError (LastRole, LastWriteDate, ErrorCount) ";
                sqlQuery += "SET LastRole = " + activeServerRole.ToString() + " ";
                sqlQuery += ", LastWriteDate = SYSDATETIME() ";
                sqlQuery += ", ErrorCount = " + (increaseCount == 0 ? "0 " : "ErrorCount + " + increaseCount.ToString() + " ");

                LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                Logger.LogDebug("Action_UpdateDatabaseStateError ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_UpdateDatabaseStateError failed", ex);
            }
        }

        private bool Action_UpdateLastDatabaseStateErrorAndCount_ShiftState()
        {
            Logger.LogDebug("Action_CheckLastDatabaseStateErrorAndCount_ShiftState started.");
            int checksToShutDown = ConfigurationForInstance.ShutDownAfterNumberOfChecksForDatabaseState;
            Action_UpdateDatabaseStateError(Information_ServerRole, 1);

            int countOfChecks = Information_GetDatabaseStateErrorCount();
            if (countOfChecks > checksToShutDown)
            {
                Logger.LogWarning(string.Format("Will shut down from state {0} because of Database State Error as count {1} is above {2}.", Information_ServerState, countOfChecks, checksToShutDown));
                return true;
            }
            else
            {
                Logger.LogDebug(string.Format("Should not shut down because of Database State Error as count {0} is not above {1}.", countOfChecks, checksToShutDown));
                return false;
            }
        }

        private bool Action_UpdateSecondaryRunningNoPrimaryCount_ShiftState()
        {
            Logger.LogDebug("Action_CheckLastDatabaseStateErrorAndCount_ShiftState started.");
            int checksToShutDown = ConfigurationForInstance.ShutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState;
            Action_UpdateServerState(LocalMasterDatabase, true, true, false, Information_ServerRole, Information_ServerState, 1);

            int countOfChecks = Information_GetServerStateSecondaryRunningNoPrimaryStateCount();
            if (countOfChecks > checksToShutDown)
            {
                Logger.LogWarning(string.Format("Will shut down from state {0} because of Server State is not allowed for longer as count {1} is above {2}.", Information_ServerState, countOfChecks, checksToShutDown));
                return true;
            }
            else
            {
                Logger.LogDebug(string.Format("Should not shut down because of Server State Error is allowed as count {0} is not above {1}.", countOfChecks, checksToShutDown));
                return false;
            }
        }

        private bool Action_UpdatePrimaryRunningNoSecondaryCount_ShiftState()
        {
            Logger.LogDebug("Action_CheckLastDatabaseStateErrorAndCount_ShiftState started.");
            int checksToSwitch = ConfigurationForInstance.SwitchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState;
            Action_UpdateServerState(LocalMasterDatabase, true, true, false, Information_ServerRole, Information_ServerState, 1);

            int countOfChecks = Information_GetServerStatePrimaryRunningNoSecondaryStateCount();
            if (countOfChecks > checksToSwitch)
            {
                Logger.LogWarning(string.Format("Will shift from state {0} because of Server State in not allowed for longer as count {1} is above {2}.", Information_ServerState, countOfChecks, checksToSwitch));
                return true;
            }
            else
            {
                Logger.LogDebug(string.Format("Should not shift state because of Server State is allowed as count {0} is not above {1}.", countOfChecks, checksToSwitch));
                return false;
            }
        }

        private int Information_GetServerStatePrimaryRunningNoSecondaryStateCount()
        {
            return Information_GetServerStateCount(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE);
        }

        private int Information_GetServerStateSecondaryRunningNoPrimaryStateCount()
        {
            return Information_GetServerStateCount(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE);
        }

        private int Information_GetServerStateCount(ServerStateEnum serverState)
        {
            try
            {
                Logger.LogDebug(string.Format("Information_GetServerStateCount for {0} started",serverState));
                string sqlQuery = "SELECT TOP (1) StateCount FROM ServerState WHERE LastState = " + serverState + " AND UpdaterLocal = 1 AND AboutLocal = 1 ";

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
                                Logger.LogDebug(string.Format("Information_GetServerStateCount for {0} ended with value {1}", serverState, returnValue));
                                if (returnValue.HasValue)
                                {
                                    return returnValue.Value;
                                }
                            }
                        }
                    }
                }
                return 0;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Information_GetServerStateCount failed", ex);
            }

        }

        private int Information_GetDatabaseStateErrorCount()
        {
            try
            {
                Logger.LogDebug("Information_GetDatabaseStateErrorCount started");
                string sqlQuery = "SELECT TOP (1) ErrorCount FROM DatabaseStateError";

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
                                Logger.LogDebug(string.Format("Information_GetDatabaseStateErrorCount ended with value {0}", returnValue));
                                if (returnValue.HasValue)
                                {
                                    return returnValue.Value;
                                }
                            }
                        }
                    }
                }
                throw new SqlServerMirroringException("Information_GetDatabaseStateErrorCount could not find a value in table DatabaseStateError");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Information_GetDatabaseStateErrorCount failed", ex);
            }
        }

        private string Information_GetDatabaseMirringRole(DatabaseName databaseName)
        {
            try
            {
                Logger.LogDebug("Information_GetDatabaseMirringRole started");
                string sqlQuery = "SELECT TOP (1) mirroring_role_desc FROM sys.database_mirroring WHERE database_id = DB_ID('" + databaseName + "')";

                DataSet dataSet = LocalMasterDatabase.ExecuteWithResults(sqlQuery);
                if (dataSet == null || dataSet.Tables.Count == 0)
                {
                    Logger.LogWarning("Information_GetDatabaseMirringRole found no rows returning NoDatabase as it might not be an error");
                    return "NoDatabase";
                }
                else
                {
                    foreach (DataTable table in dataSet.Tables)
                    {
                        foreach (DataRow row in table.Rows)
                        {
                            foreach (DataColumn column in row.Table.Columns)
                            {
                                if (column.DataType == typeof(string))
                                {
                                    Logger.LogDebug(string.Format("Information_GetDatabaseMirringRole ended with value {0}", (row[column] is DBNull) ? "DBNull" : row[column].ToString()));
                                    if (row[column] is DBNull)
                                    {
                                        return "NotSet";
                                    }
                                    else
                                    {
                                        return (string)row[column];
                                    }
                                }
                            }
                        }
                    }
                }
                throw new SqlServerMirroringException("Information_GetDatabaseMirringRole could not find a value in table sys.database_mirroring for " + databaseName);
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Information_GetDatabaseMirringRole failed", ex);
            }
        }

        private void Action_ResetLastDatabaseStateErrorAndCount()
        {
            Logger.LogDebug("Action_ResetLastDatabaseStateErrorAndCount started.");
            Action_UpdateDatabaseStateError(Information_ServerRole, 0);
            Logger.LogDebug("Action_ResetLastDatabaseStateErrorAndCount ended.");
        }

        private void Action_UpdateLocalServerState_MissingRemoteServer()
        {
            Logger.LogDebug("Action_UpdateRemoteServerState_ConnectedRemoteServer started.");
            Action_UpdateServerState(LocalMasterDatabase, true, false, false, Information_ServerRole, Information_ServerState, 1);
            Logger.LogDebug("Action_UpdateRemoteServerState_ConnectedRemoteServer ended.");
        }

        private void Action_UpdateRemoteServerState_ConnectedRemoteServer()
        {
            Logger.LogDebug("Action_UpdateRemoteServerState_ConnectedRemoteServer started.");
            Action_UpdateServerState(RemoteMasterDatabase, false, true, true, Information_ServerRole, Information_ServerState, 0);
            Logger.LogDebug("Action_UpdateRemoteServerState_ConnectedRemoteServer ended.");
        }

        private void Action_UpdateLocalServerState_ConnectedRemoteServer()
        {
            Logger.LogDebug("Action_UpdateLocalServerState_ConnectedRemoteServer started.");
            Action_UpdateServerState(LocalMasterDatabase, true, false, true, Information_ServerRole, Information_ServerState, 0);
            Logger.LogDebug("Action_UpdateLocalServerState_ConnectedRemoteServer ended.");
        }

        private void Action_CreateMasterServerStateTable()
        {
            Logger.LogDebug("Action_CreateMasterServerStateTable started.");

            try
            {
                Table localServerStateTable = new Table(LocalMasterDatabase, "ServerState");
                Column column1 = new Column(localServerStateTable, "UpdaterLocal", DataType.Bit);
                column1.Nullable = false;
                localServerStateTable.Columns.Add(column1);
                Column column2 = new Column(localServerStateTable, "AboutLocal", DataType.Bit);
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
                Logger.LogDebug("Action_CreateMasterServerStateTable ended.");
                Action_InsertServerStateBaseState();
            }
            catch (SqlServerMirroringException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_CreateMasterServerStateTable failed", ex);
            }
        }

        private void Action_InsertServerStateBaseState()
        {
            try
            {
                Logger.LogDebug("Action_InsertServerStateBaseState started.");

                string sqlQuery = "INSERT INTO ServerState (UpdaterLocal, AboutLocal, Connected, LastRole, LastState, LastWriteDate, StateCount) ";
                sqlQuery += "VALUES ";
                sqlQuery += "(1,1,0,'NotSet','INITIAL_STATE',SYSDATETIME(),0),";
                sqlQuery += "(1,0,0,'NotSet','INITIAL_STATE',SYSDATETIME(),0),";
                sqlQuery += "(0,1,0,'NotSet','INITIAL_STATE',SYSDATETIME(),0)";

                LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                Logger.LogDebug("Action_InsertServerStateBaseState ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_InsertServerStateBaseState failed", ex);
            }
        }

        private void Action_UpdateServerState(Database databaseToUpdate, bool updaterLocal, bool aboutLocal, bool connected, ServerRole activeServerRole, ServerState activeServerState, int increaseCount)
        {
            try
            {
                Logger.LogDebug("Action_UpdateServerState started.");

                string sqlQuery = "UPDATE ServerState (UpdaterLocal, AboutLocal, Connected, LastState, LastWriteDate, StateCount) ";
                sqlQuery += "SET Connected = " + (connected ? "1 " : "0 ");
                sqlQuery += ", LastRole = " + activeServerRole.ToString() + " ";
                sqlQuery += ", LastState = " + activeServerState.ToString() + " ";
                sqlQuery += ", LastWriteDate = SYSDATETIME() ";
                sqlQuery += ", StateCount = " + (increaseCount == 0 ? "0 " : "StateCount + " + increaseCount.ToString() + " ");
                sqlQuery += "WHERE UpdaterLocal = " + (updaterLocal ? "1 " : "0 ");
                sqlQuery += "AND AboutLocal = " + (aboutLocal ? "1 " : "0 ");

                databaseToUpdate.ExecuteNonQuery(sqlQuery);
                Logger.LogDebug("Action_UpdateServerState ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_UpdateServerState failed", ex);
            }
        }

        private bool Information_CheckLocalMasterServerStateTable()
        {
            return Information_CheckMasterServerStateTable(LocalMasterDatabase);
        }

        private bool Information_CheckRemoteMasterServerStateTable()
        {
            return Information_CheckMasterServerStateTable(RemoteMasterDatabase);
        }

        #endregion

        #region Individual Databases

        private void Action_AddDatabaseToMirroring(ConfigurationForDatabase configuredDatabase, bool serverPrimary)
        {
            if (!serverPrimary)
            {
                if (Action_RestoreDatabase(configuredDatabase))
                {
                    Logger.LogInfo("Restored backup");
                }
                else
                {
                    Logger.LogInfo("Moved database from remote share and restored Backup");
                }
            }
            Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).First();
            if (database.RecoveryModel != RecoveryModel.Full)
            {
                try
                {
                    database.RecoveryModel = RecoveryModel.Full;
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                    Logger.LogDebug(string.Format("Database {0} has been set to full recovery", database.Name));
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
                Logger.LogDebug(string.Format("Database {0} has enabled service broker", database.Name));
            }
            if (serverPrimary)
            {
                if (Action_BackupDatabaseForMirrorServer(configuredDatabase))
                {
                    Logger.LogInfo("Backup created and moved to remote share");
                }
                else
                {
                    Logger.LogInfo("Backup created and moved to local share due to missing access to remote share");
                }
            }

            if (Information_HasAccessToRemoteServer())
            {
                Action_CreateMirroring(configuredDatabase, serverPrimary);
            }
            else
            {
                Logger.LogWarning("Could not start mirroring as remote server is it not possible to connect to. If this is first run on primary without a secondary set up this should happen.");
            }
        }

        private void Action_StartEndpoint(string endpoint_Name)
        {
            foreach (Endpoint endpoint in DatabaseServerInstance.Endpoints)
            {
                if (endpoint.Name == endpoint_Name)
                {
                    if (endpoint.EndpointState != EndpointState.Started)
                    {
                        endpoint.Start();
                        Logger.LogInfo(string.Format("Action_StartEndpoint: Started endpoint {0}", endpoint_Name));
                        break;
                    }
                    else
                    {
                        Logger.LogDebug(string.Format("Action_StartEndpoint: Endpoint {0} was already started so no action taken", endpoint_Name));
                        break;
                    }
                }
            }
        }

        private bool Information_EndpointExists(string endpoint_Name)
        {
            bool found = false;
            foreach (Endpoint endpoint in DatabaseServerInstance.Endpoints)
            {
                if (endpoint.Name == endpoint_Name)
                {
                    found = true;
                    break;
                }
            }
            return found;
        }

        private bool Information_EndpointStarted(string endpoint_Name)
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

        private void Action_CreateMirroring(ConfigurationForDatabase configuredDatabase, bool serverPrimary)
        {
            Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).First();
            try
            {
                Logger.LogDebug(string.Format("Getting ready to start mirroring {0} with partner endpoint on {1} port {2}"
                    , configuredDatabase.DatabaseName, ConfigurationForInstance.RemoteServer, ConfigurationForInstance.Endpoint_ListenerPort));

                database.MirroringPartner = "TCP://" + ConfigurationForInstance.RemoteServer + ":" + ConfigurationForInstance.Endpoint_ListenerPort;
                database.Alter(TerminationClause.RollbackTransactionsImmediately);
                Logger.LogDebug(string.Format("Mirroring started {0} with partner endpoint on {1} port {2}", configuredDatabase.DatabaseName, ConfigurationForInstance.RemoteServer, ConfigurationForInstance.Endpoint_ListenerPort));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Creation of mirroring failed for {0}", configuredDatabase.DatabaseName.ToString()), ex);
            }
        }

        private void Action_CreateEndpoint()
        {
            // EndPoints Needed for each mirror database
            try
            {
                //Set up a database mirroring endpoint on the server before 
                //setting up a database mirror. 
                //Define an Endpoint object variable for database mirroring. 
                Endpoint ep = default(Endpoint);
                ep = new Endpoint(DatabaseServerInstance, ConfigurationForInstance.Endpoint_Name);
                ep.ProtocolType = ProtocolType.Tcp;
                ep.EndpointType = EndpointType.DatabaseMirroring;
                //Specify the protocol ports. 
                ep.Protocol.Tcp.ListenerPort = ConfigurationForInstance.Endpoint_ListenerPort;
                //Specify the role of the payload. 
                ep.Payload.DatabaseMirroring.ServerMirroringRole = ServerMirroringRole.All;
                //Create the endpoint on the instance of SQL Server. 
                ep.Create();
                //Start the endpoint. 
                ep.Start();
                Logger.LogDebug(string.Format("Created endpoint and started. Endpoint in state {1}.", ep.EndpointState));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Creation of endpoint for {0} failed", ConfigurationForInstance.Endpoint_Name), ex);
            }
        }

        private void Action_RemoveDatabaseFromMirroring(MirrorDatabase mirrorState, bool serverPrimary)
        {
            Database database = Information_UserDatabases.Where(s => s.Name.Equals(mirrorState.DatabaseName.ToString())).First();
            try
            {
                database.ChangeMirroringState(MirroringOption.Off);
                database.Alter(TerminationClause.RollbackTransactionsImmediately);
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Removing mirroring failed for {0}", mirrorState.DatabaseName), ex);
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


        private bool Information_CheckMasterServerStateTable(Database databaseToCheck)
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

        private string Action_BackupDatabase(ConfigurationForDatabase configuredDatabase)
        {
            try
            {
                DirectoryPath localDirectoryForBackup = configuredDatabase.LocalBackupDirectoryWithSubDirectory;
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, localDirectoryForBackup);

                Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).First();

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
                System.DateTime backupdate = System.DateTime.Now.AddDays(ConfigurationForInstance.BackupExpiresAfterDays);
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
        private bool Action_BackupDatabaseForMirrorServer(ConfigurationForDatabase configuredDatabase)
        {
            string fileName = Action_BackupDatabase(configuredDatabase);
            DirectoryPath localBackupDirectoryWithSubDirectory = configuredDatabase.LocalBackupDirectoryWithSubDirectory;
            DirectoryHelper.TestReadWriteAccessToDirectory(Logger, localBackupDirectoryWithSubDirectory);
            DirectoryPath localLocalTransferDirectoryWithSubDirectory = configuredDatabase.LocalLocalTransferDirectoryWithSubDirectory;
            ShareName localShareName = configuredDatabase.LocalShareName;
            try
            {
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(Logger, localLocalTransferDirectoryWithSubDirectory, localShareName);
                Action_CopyFileLocal(fileName, localBackupDirectoryWithSubDirectory, localLocalTransferDirectoryWithSubDirectory);
            }
            catch (Exception e)
            {
                throw new SqlServerMirroringException(string.Format("Failed copying backup file {0} locally", fileName),e);
            }
            if (Information_HasAccessToRemoteServer())
            {
                try
                {
                    UncPath remoteRemoteTransferDirectoryWithSubDirectory = configuredDatabase.RemoteRemoteTransferDirectoryWithSubDirectory;
                    UncPath remoteRemoteDeliveryDirectoryWithSubDirectory = configuredDatabase.RemoteRemoteDeliveryDirectoryWithSubDirectory;
                    Action_MoveFileLocalToRemote(fileName, localLocalTransferDirectoryWithSubDirectory, remoteRemoteTransferDirectoryWithSubDirectory);
                    Action_MoveFileRemoteToRemote(fileName, remoteRemoteTransferDirectoryWithSubDirectory, remoteRemoteDeliveryDirectoryWithSubDirectory);
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
        private bool Action_RestoreDatabase(ConfigurationForDatabase configuredDatabase)
        {
            Logger.LogDebug(string.Format("Action_RestoreDatabase started for {0}", configuredDatabase.DatabaseName));

            string fileName;
            try {
                if (Action_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles(configuredDatabase))
                {

                    DirectoryPath localRestoreDircetoryWithSubDirectory = configuredDatabase.LocalRestoreDirectoryWithSubDirectory;

                    fileName = Information_GetNewesteFilename(configuredDatabase.DatabaseName.ToString(), localRestoreDircetoryWithSubDirectory.ToString());

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
                    if (Information_DatabaseExists(configuredDatabase.DatabaseName.ToString()))
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

        private bool Information_DatabaseExists(string databaseName)
        {
            Database database = Information_UserDatabases.Where(s => s.Name.Equals(databaseName)).First();
            if(database == null)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        private string Information_GetNewesteFilename(string databaseName, string fullPathString)
        {
            FileInfo result = null;
            var directory = new DirectoryInfo(fullPathString);
            var list = directory.GetFiles("*.bak");
            if (list.Count() > 0)
            {
                result = list.Where(s => s.Name.StartsWith(databaseName)).OrderByDescending(f => f.Name).FirstOrDefault();
            }
            if (result != null)
            {
                Logger.LogDebug(string.Format("Information_GetNewesteFilename: Found {0} in {1} searching for {2}(...).bak", result.Name, fullPathString, databaseName));
                return result.Name;
            }
            else
            {
                Logger.LogDebug(string.Format("Information_GetNewesteFilename: Found nothing in {0} searching for {1}(...).bak", fullPathString, databaseName));
                return string.Empty;
            }
        }

        private void Action_DeleteAllFilesExcept(string fileName, string fullPathString)
        {
            List<string> files;
            if (string.IsNullOrWhiteSpace(fileName))
            {
                files = Directory.EnumerateFiles(fullPathString).ToList();
            }
            else
            {
                files = Directory.EnumerateFiles(fullPathString).Where(s => !s.EndsWith(fileName)).ToList();
            }
            files.ForEach(x => { try { System.IO.File.Delete(x); Logger.LogDebug(string.Format("Action_DeleteAllFilesExcept deleted file {0}.", x)); } catch { } });
            Logger.LogDebug(string.Format("Action_DeleteAllFilesExcept for {0} except {1}.", fullPathString, fileName));
        }

        /* Create local directories if not existing as this might be first time running and 
        *  returns false if no file is found and true if one is found */
        private bool Action_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles(ConfigurationForDatabase configuredDatabase)
        {
            Logger.LogDebug(string.Format("Action_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles started for {0}", configuredDatabase.DatabaseName));

            try
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
                DirectoryPath localRestoreDirectory = configuredDatabase.LocalRestoreDirectoryWithSubDirectory;
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, localRestoreDirectory);
                string localRestoreDirectoryNewestFileName = string.Empty;
                if (Information_HasAccessToRemoteServer())
                {
                    try
                    {
                        remoteLocalTransferDirectoryNewestFileName = Information_GetNewesteFilename(databaseNameString, remoteLocalTransferDirectory.ToString());
                    }
                    catch (Exception)
                    {
                        Logger.LogWarning(string.Format("Action_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles: Could not access remote directory {0} but have access to server.", remoteLocalTransferDirectory.ToString()));
                    }
                }
                localRemoteTransferDirectoryNewestFileName = Information_GetNewesteFilename(databaseNameString, localRemoteTransferDirectory.ToString());
                localRemoteDeliveryDirectoryNewestFileName = Information_GetNewesteFilename(databaseNameString, localRemoteDeliveryDirectory.ToString());
                localRestoreDirectoryNewestFileName = Information_GetNewesteFilename(databaseNameString, localRestoreDirectory.ToString());
                long remoteLocalTransferDirectoryNewestValue = Information_GetFileTimePart(remoteLocalTransferDirectoryNewestFileName);
                long localRemoteTransferDirectoryNewestValue = Information_GetFileTimePart(localRemoteTransferDirectoryNewestFileName);
                long localRemoteDeliveryDirectoryNewestValue = Information_GetFileTimePart(localRemoteDeliveryDirectoryNewestFileName);
                long localRestoreDirectoryNewestValue = Information_GetFileTimePart(localRestoreDirectoryNewestFileName);

                /* Add all to array and select max */
                long[] values = new long[] { remoteLocalTransferDirectoryNewestValue, localRemoteTransferDirectoryNewestValue, localRemoteDeliveryDirectoryNewestValue, localRestoreDirectoryNewestValue };
                long maxValue = values.Max();

                if (maxValue == 0)
                {
                    /* No files found so none moved */
                    Logger.LogDebug("Action_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles no files found so nothing was moved.");
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
                        Action_DeleteAllFilesExcept(localRestoreDirectoryNewestFileName, localRestoreDirectory.ToString());
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
                        Action_DeleteAllFilesExcept(string.Empty, localRestoreDirectory.ToString());
                        Logger.LogDebug(string.Format("Deleted all files in {0}.", localRestoreDirectory.ToString()));
                    }

                    #endregion

                    #region Local Remote Delivery

                    if (!found && localRemoteDeliveryDirectoryNewestValue == maxValue)
                    {
                        found = true;
                        Logger.LogInfo(string.Format("Backup file {0} found in {1}.", localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory.ToString()));
                        /* delete all files */
                        Action_DeleteAllFilesExcept(localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory.ToString());
                        Logger.LogDebug(string.Format("Deleted all files except {0} in {1}.", localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory.ToString()));
                        /* Move actions needed */
                        Action_MoveFileLocal(localRemoteDeliveryDirectoryNewestFileName, localRemoteDeliveryDirectory, localRestoreDirectory);
                    }
                    else if (localRemoteDeliveryDirectoryNewestValue == 0)
                    {
                        Logger.LogDebug(string.Format("No files found in {0}.", localRemoteDeliveryDirectory.ToString()));
                    }
                    else
                    {
                        /* delete all files */
                        Action_DeleteAllFilesExcept(string.Empty, localRemoteDeliveryDirectory.ToString());
                        Logger.LogDebug(string.Format("Deleted all files in {0}.", localRemoteDeliveryDirectory.ToString()));
                    }

                    #endregion

                    #region Local Remote Transfer

                    if (!found && localRemoteTransferDirectoryNewestValue == maxValue)
                    {
                        found = true;
                        Logger.LogInfo(string.Format("Backup file {0} found in {1}.", localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory.ToString()));
                        /* delete all files */
                        Action_DeleteAllFilesExcept(localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory.ToString());
                        Logger.LogDebug(string.Format("Deleted all files except {0} in {1}.", localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory.ToString()));
                        /* Move actions needed */
                        Action_MoveFileLocal(localRemoteTransferDirectoryNewestFileName, localRemoteTransferDirectory, localRemoteDeliveryDirectory);
                        Action_MoveFileLocal(localRemoteTransferDirectoryNewestFileName, localRemoteDeliveryDirectory, localRestoreDirectory);
                    }
                    else if (localRemoteDeliveryDirectoryNewestValue == 0)
                    {
                        Logger.LogDebug(string.Format("No files found in {0}.", localRemoteTransferDirectory.ToString()));
                    }
                    else
                    {
                        /* delete all files */
                        Action_DeleteAllFilesExcept(string.Empty, localRemoteTransferDirectory.ToString());
                        Logger.LogDebug(string.Format("Deleted all files in {0}.", localRemoteTransferDirectory.ToString()));
                    }

                    #endregion

                    #region Remote Local Transfer

                    if (!found && remoteLocalTransferDirectoryNewestValue == maxValue)
                    {
                        found = true;
                        Logger.LogInfo(string.Format("Backup file {0} found in {1}.", remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory.ToString()));
                        /* delete all files */
                        Action_DeleteAllFilesExcept(remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory.ToString());
                        Logger.LogDebug(string.Format("Deleted all files except {0} in {1}.", remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory.ToString()));
                        /* Move actions needed */
                        Action_MoveFileRemoteToLocal(remoteLocalTransferDirectoryNewestFileName, remoteLocalTransferDirectory, localRemoteTransferDirectory);
                        Action_MoveFileLocal(remoteLocalTransferDirectoryNewestFileName, localRemoteTransferDirectory, localRemoteDeliveryDirectory);
                        Action_MoveFileLocal(remoteLocalTransferDirectoryNewestFileName, localRemoteDeliveryDirectory, localRestoreDirectory);
                    }
                    else if (localRemoteDeliveryDirectoryNewestValue == 0)
                    {
                        Logger.LogDebug(string.Format("No files found in {0}.", remoteLocalTransferDirectory.ToString()));
                    }
                    else
                    {
                        /* delete all files */
                        Action_DeleteAllFilesExcept(string.Empty, remoteLocalTransferDirectory.ToString());
                        Logger.LogDebug(string.Format("Deleted all files in {0}.", localRemoteTransferDirectory.ToString()));
                    }
                    #endregion

                    return true;
                }
            }
            catch(Exception ex)
            {
                throw new SqlServerMirroringException("Action_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles failed", ex);
            }
        }

        private void Action_MoveFileLocal(string fileName, DirectoryPath source, DirectoryPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Action_MoveFileLocal: Trying to move file {0} from {1} to {2}.", fileName, source, destination));
                File.Move(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Action_MoveFileLocal: Moved file {0} from {1} to {2}.", fileName, source, destination));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_MoveFileLocal: Failed moving file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private void Action_MoveFileRemoteToLocal(string fileName, UncPath source, DirectoryPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Action_MoveFileRemoteToLocal: Trying to move file {0} from {1} to {2}.", fileName, source, destination));
                File.Move(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Action_MoveFileRemoteToLocal: Moved file {0} from {1} to {2}.", fileName, source, destination));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_MoveFileRemoteToLocal: Failed moving file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private void Action_MoveFileLocalToRemote(string fileName, DirectoryPath source, UncPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Action_MoveFileLocalToRemote: Trying to move file {0} from {1} to {2}.", fileName, source, destination));
                File.Move(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Action_MoveFileLocalToRemote: Moved file {0} from {1} to {2}.", fileName, source, destination));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_MoveFileLocalToRemote: Failed moving file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private void Action_MoveFileRemoteToRemote(string fileName, UncPath source, UncPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Action_MoveFileRemoteToRemote: Trying to move file {0} from {1} to {2}.", fileName, source, destination));
                File.Move(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Action_MoveFileRemoteToRemote: Moved file {0} from {1} to {2}.", fileName, source, destination));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_MoveFileRemoteToRemote: Failed moving file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private void Action_CopyFileLocal(string fileName, DirectoryPath source, DirectoryPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Action_CopyFileLocal: Trying to copy file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()));
                File.Move(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Action_CopyFileLocal: Copied file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_CopyFileLocal: Failed copying file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private long Information_GetFileTimePart(string fileName)
        {
            if(string.IsNullOrWhiteSpace(fileName))
            {
                Logger.LogDebug("fileName empty");
                return 0;
            }
            Regex regex = new Regex(@"^(?:[\w_][\w_\d]*_)(\d*)(?:\.bak)$");
            Match match = regex.Match(fileName);
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
            throw new SqlServerMirroringException(string.Format("GetFileTimePart failed to extract time part from {0}.", fileName));
        }

        #endregion
    }
}
