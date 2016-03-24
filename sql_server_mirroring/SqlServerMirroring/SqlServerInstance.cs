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
        private const string MASTER_DATABASE = "master";

        private Server _server;
        private Server _remoteServer;
        private ILogger _logger;
        private ManagedComputer _managedComputer;
        private Dictionary<ServerStateEnum, ServerState> _serverStates;
        private ServerState _activeServerState;
        private ServerState _oldServerState;
        private ConfigurationForInstance _instance_Configuration;
        private Dictionary<string, ConfigurationForDatabase> _databases_Configuration;
        private ServerRoleEnum _activeServerRole = ServerRoleEnum.NotSet;
        private Dictionary<string,DatabaseMirrorState> _databaseMirrorStates;
        private Dictionary<string, DatabaseState> _databaseStates;

        public SqlServerInstance(ILogger logger, string connectionString)
        {
            _logger = logger;
            Action_Instance_ValidateConnectionStringAndDatabaseConnection(connectionString);
            _server = new Server(new ServerConnection(new SqlConnection(connectionString)));
            _databases_Configuration = new Dictionary<string, ConfigurationForDatabase>();
            _managedComputer = new ManagedComputer("(local)");
            Action_ServerState_BuildServerStates();
        }

        #region Public Properties

        public bool Information_RemoteServer_HasAccess()
        {
            try
            {
                if (Information_RemoteServer_Status().Equals("Online"))
                {
                    Logger.LogDebug("Information_RemoteServer_HasAccess: Access to remote server.");
                    return true;
                }
                else
                {
                    Logger.LogInfo("Information_RemoteServer_HasAccess: No access to remote server.");
                    return false;
                }
            }
            catch (Exception)
            {
                Logger.LogInfo("Information_RemoteServer_HasAccess: No access to remote server.");
                return false;
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
                return _activeServerState;
            }
            set
            {
                _activeServerState = value;
                Logger.LogDebug(string.Format("Active state set to {0}.", _activeServerState));
            }
        }

        public ServerState Information_ServerState_Old
        {
            get
            {
                return _oldServerState;
            }
            set
            {
                _oldServerState = value;
            }
        }

        private Table LocalServerStateTable
        {
            // TODO nicify
            get;
            set;
        }

        public Dictionary<string, ConfigurationForDatabase> Databases_Configuration
        {
            get
            {
                return _databases_Configuration;
            }
            set
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
            set
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
                return Information_ServerState.IsDegradedState;
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

        public Dictionary<string, string> Information_SqlAgent()
        {
            Dictionary<string, string> sqlAgentInformation = new Dictionary<string, string>();
            Action_SqlAgent_EnableAgentXps();
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
            Action_SqlAgent_EnableAgentXps();
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
            if (!Information_SqlAgent_CheckRunning())
            {
                Logger.LogWarning("Sql Server Agent service is not running");
                return false;
            }
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
            Logger.LogDebug("Action_StartPrimary started");
            Action_ServerState_StartInitial();
            Action_Databases_StartUpMirrorCheck(Databases_Configuration, true);

            if (Information_ServerState_IsValidChange(ServerStateEnum.PRIMARY_STARTUP_STATE))
            {
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_STARTUP_STATE);
            }
            Logger.LogDebug("Action_StartPrimary ended");
        }

        public void Action_Instance_StartSecondary()
        {
            Logger.LogDebug("Action_StartSecondary started");
            Action_ServerState_StartInitial();
            Action_Databases_StartUpMirrorCheck(Databases_Configuration, false);

            if (Information_ServerState_IsValidChange(ServerStateEnum.SECONDARY_STARTUP_STATE))
            {
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_STARTUP_STATE);
            }
            Logger.LogDebug("Action_StartSecondary ended");
        }

        public void Action_Instance_SetupMonitoring()
        {
            Logger.LogDebug("Action_SetupMonitoring started");
            try
            {
                LocalMasterDatabase.ExecuteNonQuery("EXEC sys.sp_dbmmonitoraddmonitoring " + Instance_Configuration.MirrorMonitoringUpdateMinutes);
                Logger.LogDebug(string.Format("Mirroring monitoring every {0} minutes started with partner endpoint on {1} port {2}"
                    , Instance_Configuration.MirrorMonitoringUpdateMinutes, Instance_Configuration.RemoteServer, Instance_Configuration.Endpoint_ListenerPort));

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
                throw new SqlServerMirroringException(string.Format("Action_SetupMonitoring failed"), ex);
            }
            Logger.LogDebug("Action_SetupMonitoring ended");
        }

        public void Action_ServerState_TimedCheck()
        {
            Logger.LogDebug("Action_ServerState_TimedCheck started");

            Action_Instance_CheckDatabaseMirrorStates();
            Action_Instance_CheckDatabaseStates();
            Action_ServerRole_Recheck();

            if (!Information_ServerState.IgnoreMirrorStateCheck)
            {
                #region Start Mirroring Check

                if (Information_ServerState.State == ServerStateEnum.PRIMARY_STARTUP_STATE)
                {
                    if (Information_Instance_AllConfiguredDatabasesMirrored())
                    {
                        if (Information_RemoteServer_HasAccess())
                        {
                            Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_RUNNING_STATE);
                        }
                        else
                        {
                            Logger.LogWarning(string.Format("Action_ServerState_TimedCheck: No access to remote server"));
                            Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE);
                        }
                    }
                    else
                    {
                        if (Action_ServerState_UpdatePrimaryStartupCount_ShiftState())
                        {
                            Logger.LogWarning(string.Format("Action_ServerState_TimedCheck: Server timed out waiting for mirroring state after {0} checks", Instance_Configuration.PrimaryStartupWaitNumberOfChecksForMirroringTimeout));
                            Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                        }
                        else
                        {
                            Logger.LogDebug(string.Format("Action_ServerState_TimedCheck: Checks if mirroring state can be started"));
                            Action_Instance_CheckStartMirroring();
                        }
                    }
                }
                else if (Information_ServerState.State == ServerStateEnum.SECONDARY_STARTUP_STATE)
                {
                    if (Information_Instance_AllConfiguredDatabasesMirrored())
                    {
                        if (Information_RemoteServer_HasAccess())
                        {
                            Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_RUNNING_STATE);
                        }
                        else
                        {
                            Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE);
                        }
                    }
                    else
                    {
                        if (Action_ServerState_UpdateSecondaryStartupCount_ShiftState())
                        {
                            Logger.LogWarning(string.Format("Action_ServerState_TimedCheck: Server timed out waiting for mirroring state after {0} checks", Instance_Configuration.SecondaryStartupWaitNumberOfChecksForMirroringTimeout));
                            Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                        }
                    }
                }

                #endregion

                #region SwitchOver Check

                Action_Instance_SwitchOverCheck();

                #endregion

                #region DatabaseErrorState

                bool databaseErrorState = false;
                foreach (DatabaseMirrorState databaseMirrorState in Information_DatabaseMirrorStates.Values.Where(s => s.MirroringState != MirroringStateEnum.NotMirrored))
                {
                    if (Information_Instance_ServerRole == ServerRoleEnum.Primary)
                    {
                        if (databaseMirrorState.DatabaseState != DatabaseStateEnum.ONLINE)
                        {
                            databaseErrorState = true;
                            Logger.LogError(string.Format("Action_ServerState_TimedCheck: Server Role {0}: Database {1} has error status {2}."
                                , Information_Instance_ServerRole, databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState));
                            Action_DatabaseState_Update(databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState, Information_Instance_ServerRole, true, 1);
                        }
                        else
                        {
                            Logger.LogDebug(string.Format("Action_ServerState_TimedCheck: Server Role {0}: Database {1} has status {2}."
                                , Information_Instance_ServerRole, databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState));
                            Action_DatabaseState_Update(databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState, Information_Instance_ServerRole, false, 0);
                        }
                    }
                    else if (Information_Instance_ServerRole == ServerRoleEnum.Secondary)
                    {
                        if (!databaseMirrorState.DatabaseIsInStandby)
                        {
                            databaseErrorState = true;
                            Logger.LogError(string.Format("Action_ServerState_TimedCheck: Server Role {0}: Database {1} has error status {2}."
                                , Information_Instance_ServerRole, databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState));
                            Action_DatabaseState_Update(databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState, Information_Instance_ServerRole, true, 1);
                        }
                        else
                        {
                            Logger.LogDebug(string.Format("Action_ServerState_TimedCheck: Server Role {0}: Database {1} has status {2}."
                                , Information_Instance_ServerRole, databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState));
                            Action_DatabaseState_Update(databaseMirrorState.DatabaseName, databaseMirrorState.DatabaseState, Information_Instance_ServerRole, false, 0);
                        }
                    }
                    else
                    {
                        databaseErrorState = true;
                        Logger.LogError(string.Format("Action_ServerState_TimedCheck: Server Role {0}", Information_Instance_ServerRole));
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
                        Logger.LogDebug(string.Format("Action_ServerState_TimedCheck: Found Server Role {0}, Server State {1} and will shut down after {2} checks."
                            , Information_Instance_ServerRole, Information_ServerState, Instance_Configuration.ShutDownAfterNumberOfChecksForDatabaseState));
                    }
                }
                else
                {
                    Logger.LogDebug(string.Format("Action_ServerState_TimedCheck: Found Server Role {0} and Server State {1}."
                        , Information_Instance_ServerRole, Information_ServerState));
                }

                #endregion

                #region StateChange


                /* Check remote server access */
                if (Information_RemoteServer_HasAccess())
                {
                    Action_ServerState_UpdateLocal_ConnectedRemoteServer();
                    Action_ServerState_UpdateRemote_ConnectedRemoteServer();
                    if (Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE)
                    {
                        Action_ServerState_Update(LocalMasterDatabase, true, true, false, Information_Instance_ServerRole, Information_ServerState, 0);
                        Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_RUNNING_STATE);
                    }
                    else if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE)
                    {
                        Action_ServerState_Update(LocalMasterDatabase, true, true, false, Information_Instance_ServerRole, Information_ServerState, 0);
                        Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_RUNNING_STATE);
                    }
                }
                else
                {
                    Action_ServerState_UpdateLocal_MissingRemoteServer();
                    if (Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_STATE)
                    {
                        Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE);
                    }
                    else if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_STATE)
                    {
                        Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE);
                    }
                    else if(Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE)
                    {
                        if(Action_ServerState_UpdatePrimaryRunningNoSecondaryCount_ShiftState())
                        {
                            Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE);
                        }
                    }
                    else if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE)
                    {
                        if(Action_ServerState_UpdateSecondaryRunningNoPrimaryCount_ShiftState())
                        {
                            Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                        }
                    }
                }
                #endregion
            }
            else
            {
                Logger.LogDebug(string.Format("Action_ServerState_TimedCheck: Ignores Action_ServerState_TimedCheck because Server State is {0}.", Information_ServerState));
            }
            Logger.LogDebug("Action_ServerState_TimedCheck ended");
        }

        private bool Action_ServerState_UpdatePrimaryStartupCount_ShiftState()
        {
            throw new NotImplementedException();
            int errro;
        }

        private bool Action_ServerState_UpdateSecondaryStartupCount_ShiftState()
        {
            throw new NotImplementedException();
            int error;
        }

        private bool Information_Instance_AllConfiguredDatabasesMirrored()
        {
            bool allConfigured = true;
            foreach(ConfigurationForDatabase configuration in Databases_Configuration.Values)
            {
                DatabaseMirrorState databaseMirrorState;
                if (Information_DatabaseMirrorStates.TryGetValue(configuration.DatabaseName, out databaseMirrorState))
                {
                    if(databaseMirrorState.MirroringRole == MirroringRoleEnum.NotMirrored)
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
                    Logger.LogError(string.Format("Server Role {0} but Server State {1}. Shutting down to signal shift.", Information_Instance_ServerRole, Information_ServerState));
                    Action_Instance_ForceShutDownMirroringService();
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

        public bool Action_Instance_ResumeMirroringForAllDatabases()
        {
            Logger.LogDebug("Action_ResumeMirroringForAllDatabases started");
            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                    if (database == null)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_ResumeMirroringForAllDatabases: Could not find database {0}", configuredDatabase.DatabaseName));
                    }

                    database.ChangeMirroringState(MirroringOption.Resume);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Action_ResumeMirroringForAllDatabases: Resume failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            Logger.LogDebug("Action_ResumeMirroringForAllDatabases ended");
            return true;
        }

        public bool Action_Instance_SuspendMirroringForAllMirrorDatabases()
        {
            Logger.LogDebug("Action_SuspendMirroringForAllMirrorDatabases started");

            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                    if (database == null)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_SuspendMirroringForAllMirrorDatabases: Could not find database {0}", configuredDatabase.DatabaseName));
                    }

                    database.ChangeMirroringState(MirroringOption.Suspend);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Action_SuspendMirroringForAllMirrorDatabases: Suspend failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            Logger.LogDebug("Action_SuspendMirroringForAllMirrorDatabases ended");
            return true;
        }

        public bool Action_Instance_ForceFailoverWithDataLossForAllMirrorDatabases()
        {
            Logger.LogDebug("Action_ForceFailoverWithDataLossForAllMirrorDatabases started");

            if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_STATE ||
            Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE ||
            Information_ServerState.State == ServerStateEnum.SECONDARY_MAINTENANCE_STATE)
            {
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE);
            }
            else
            {
                Logger.LogWarning(string.Format("Server {0} triet to manually shut down but in invalid state {1} for doing so.", Information_Instance_ServerRole, Information_ServerState));
                return false;
            }
            /* TODO Only fail over the first as all others will join if on multi mirror server with witness */
            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                    if (database == null)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_ForceFailoverWithDataLossForAllMirrorDatabases: Could not find database {0}", configuredDatabase.DatabaseName));
                    }

                    database.ChangeMirroringState(MirroringOption.ForceFailoverAndAllowDataLoss);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Action_ForceFailoverWithDataLossForAllMirrorDatabases failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            if (Information_ServerState.State == ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE)
            {
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
            Logger.LogDebug("Action_ForceFailoverWithDataLossForAllMirrorDatabases ended");
            return true;
        }

        public bool Action_Instance_FailoverForAllMirrorDatabases()
        {
            Logger.LogDebug("Action_FailoverForAllMirrorDatabases started");
            if (Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_STATE ||
                Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE ||
                Information_ServerState.State == ServerStateEnum.PRIMARY_MAINTENANCE_STATE)
            {
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE);
            }
            else if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_STATE ||
            Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE ||
            Information_ServerState.State == ServerStateEnum.SECONDARY_MAINTENANCE_STATE)
            {
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE);
            }
            else
            {
                Logger.LogWarning(string.Format("Action_FailoverForAllMirrorDatabases: Server {0} triet to manually shut down but in invalid state {1} for doing so.", Information_Instance_ServerRole, Information_ServerState));
                return false;
            }


            /* TODO Only fail over the fist as all others will join if in witness mode */
            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                try
                {
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                    if (database == null)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_FailoverForAllMirrorDatabases: Could not find database {0}", configuredDatabase.DatabaseName));
                    }

                    database.ChangeMirroringState(MirroringOption.Failover);
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Action_FailoverForAllMirrorDatabases: Failover failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            if (Information_ServerState.State == ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE)
            {
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
            else if (Information_ServerState.State == ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE)
            {
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
            Logger.LogDebug("Action_FailoverForAllMirrorDatabases ended");

            return true;
        }

        public bool Action_Instance_BackupForAllConfiguredDatabases()
        {
            Logger.LogDebug("Action_BackupForAllPrincipalDatabases started");
            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                try
                {
                    Action_Databases_BackupDatabaseForMirrorServer(configuredDatabase, true);
                    Action_Databases_BackupDatabaseForMirrorServer(configuredDatabase, false);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Action_BackupForAllPrincipalDatabases: Backup failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            Logger.LogDebug("Action_BackupForAllPrincipalDatabases ended");
            return true;
        }

        public bool Action_Instance_RestoreForAllConfiguredDatabases()
        {
            Logger.LogDebug("Action_RestoreForAllMirrorDatabases started");
            foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                try
                {
                    Action_Databases_RestoreDatabase(configuredDatabase);
                    Action_Databases_RestoreDatabaseLog(configuredDatabase);
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Action_RestoreForAllMirrorDatabases: Restore failed for {0}", configuredDatabase.DatabaseName), ex);
                }
            }
            Logger.LogDebug("Action_RestoreForAllMirrorDatabases ended");
            return true;
        }

        public bool Action_Instance_ShutDown()
        {
            Logger.LogDebug("Action_ShutDownMirroringService started");
            if (Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_STATE ||
                Information_ServerState.State == ServerStateEnum.PRIMARY_MAINTENANCE_STATE ||
                Information_ServerState.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE ||
                Information_ServerState.State == ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE)
            {
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else if (Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_STATE ||
                Information_ServerState.State == ServerStateEnum.SECONDARY_MAINTENANCE_STATE ||
                Information_ServerState.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE)
            {
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
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
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else if (Information_Instance_ServerRole == ServerRoleEnum.Secondary || Information_Instance_ServerRole == ServerRoleEnum.MainlySecondary)
            {
                Logger.LogWarning(string.Format("Action_ForceShutDownMirroringService: Force shutdown of service with role {0} in state {1}.", Information_Instance_ServerRole, Information_ServerState));
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                return true;
            }
            else
            {
                Logger.LogWarning(string.Format("Action_ForceShutDownMirroringService: Force shutdown of service with role {0} in state {1}.", Information_Instance_ServerRole, Information_ServerState));
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                return true;
            }
        }

        #endregion

        #region Private Instance Methods

        private bool Action_IO_TestReadWriteAccessToDirectory(DirectoryPath directoryPath)
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

        private void Action_Databases_StartUpMirrorCheck(Dictionary<string, ConfigurationForDatabase> configuredMirrorDatabases, bool serverPrimary)
        {
            Logger.LogDebug(string.Format("Action_StartUpMirrorCheck check if databases mirrored but not configured"));
            foreach (DatabaseMirrorState databaseMirrorState in Information_DatabaseMirrorStates.Values)
            {
                Logger.LogDebug(string.Format("Checking database {0}", databaseMirrorState.DatabaseName));
                if (databaseMirrorState.MirroringRole != MirroringRoleEnum.NotMirrored)
                {
                    if (!configuredMirrorDatabases.ContainsKey(databaseMirrorState.DatabaseName))
                    {
                        Logger.LogWarning(string.Format("Database {0} was set up for mirroring but is not in configuration", databaseMirrorState.DatabaseName));
                        Action_Databases_RemoveDatabaseFromMirroring(databaseMirrorState, serverPrimary);
                    }
                    else
                    {
                        Logger.LogDebug(string.Format("Database {0} was setup for mirroring and enabled.", databaseMirrorState.DatabaseName));
                    }
                }
                else
                {
                    Logger.LogDebug(string.Format("Database {0} was not setup for mirroring and not configured for it.", databaseMirrorState.DatabaseName));
                }

            }
            Logger.LogDebug(string.Format("Action_StartUpMirrorCheck check if databases needs to be set up"));
            /* Check databases setup */
            foreach (ConfigurationForDatabase configurationDatabase in Databases_Configuration.Values)
            {
                Logger.LogDebug(string.Format("Checking database {0}", configurationDatabase.DatabaseName));
                DatabaseMirrorState databaseMirrorState;
                if(Information_DatabaseMirrorStates.TryGetValue(configurationDatabase.DatabaseName.ToString(), out databaseMirrorState))
                {
                    if (databaseMirrorState == null || databaseMirrorState.MirroringRole == MirroringRoleEnum.NotMirrored)
                    {
                        Logger.LogWarning(string.Format("Database {0} is not set up for mirroring but is in configuration", configurationDatabase.DatabaseName));
                        if (serverPrimary)
                        {
                            Action_Databases_AddDatabaseToMirroring(configurationDatabase);
                        }
                        else
                        {
                            if (Action_Databases_RestoreDatabase(configurationDatabase))
                            {
                                Logger.LogInfo("Action_StartUpMirrorCheck: Restored backup");
                            }
                            else
                            {
                                Logger.LogInfo("Action_StartUpMirrorCheck: Moved database from remote share and restored Backup");
                            }
                            if (Action_Databases_RestoreDatabaseLog(configurationDatabase))
                            {
                                Logger.LogInfo("Action_StartUpMirrorCheck: Restored log backup");
                            }
                            else
                            {
                                Logger.LogInfo("Action_StartUpMirrorCheck: Moved database from remote share and restored log Backup");
                            }
                            Action_DatabaseState_Update(configurationDatabase.DatabaseName.ToString(), DatabaseStateEnum.READY_FOR_MIRRORING, Information_Instance_ServerRole, false, 0);
                        }
                    }
                    else
                    {
                        Logger.LogDebug(string.Format("Database {0} is set up for mirroring and is in configuration", configurationDatabase.DatabaseName));
                    }
                }
                else
                {
                    Logger.LogWarning(string.Format("Database {0} is in configuration but does not exist on database", configurationDatabase.DatabaseName));
                }
            }
            Logger.LogDebug(string.Format("Action_StartUpMirrorCheck ended"));
        }

        private bool Information_ServerState_IsValidChange(ServerStateEnum newState)
        {
            return Information_ServerState.ValidTransition(newState);
        }

        private void Action_Instance_SetupForMirroring()
        {
            Logger.LogDebug("Action_SetupInstanceForMirroring started");
            Action_SqlAgent_EnableAgentXps();
            if (DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Manual ||
                DatabaseServerInstance.ServiceStartMode == Microsoft.SqlServer.Management.Smo.ServiceStartMode.Disabled)
            {
                Logger.LogDebug("Bug/SecurityIssue: Might not be able to change to automatic start");
                Action_Instance_ChangeDatabaseServiceToAutomaticStart();
                Logger.LogInfo("Sql Server was set to Automatic start");
            }
            if (!DatabaseServerInstance.JobServer.SqlAgentAutoStart)
            {
                Logger.LogDebug("Bug/SecurityIssue: Might not be able to change to automatic start");
                Action_Instance_ChangeSqlAgentServiceToAutomaticStart();
                Logger.LogInfo("Sql Agent was set to Automatic start");
            }
            if (Information_SqlAgent_CheckRunning())
            {
                Logger.LogDebug("Bug/SecurityIssue: Might not be able to stop service for enabling service broker on msdb");
                Action_SqlAgent_Stop();
                Logger.LogInfo("Sql Agent service was stopped");

                Logger.LogDebug("Action_SetupInstanceForMirroring: Try to enable service broker on msdb");
                foreach (Database database in DatabaseServerInstance.Databases)
                {
                    if (database.Name.Equals("msdb") && !database.BrokerEnabled)
                    {
                        database.BrokerEnabled = true;
                        database.Alter(TerminationClause.RollbackTransactionsImmediately);
                        Logger.LogDebug(string.Format("Action_SetupInstanceForMirroring: Database {0} has enabled service broker", database.Name));
                    }
                }
            }
            if (!Information_SqlAgent_CheckRunning())
            {
                Logger.LogDebug("Bug/SecurityIssue: Might not be able to start service");
                Action_SqlAgent_Start();
                Logger.LogInfo("Sql Agent service was started");
            }
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

        private void Action_ServerRole_Reset()
        {
            _activeServerRole = ServerRoleEnum.NotSet;
        }

        private void Action_ServerRole_Recheck()
        {
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
        }

        private void Action_Instance_SwitchOverAllDatabasesIfPossible(bool failoverPrincipal, bool failIfNotSwitchingOver)
        {
            MirroringRoleEnum failoverRole;
            MirroringRoleEnum ignoreRole;
            if(failoverPrincipal)
            {
                failoverRole = MirroringRoleEnum.Principal;
                ignoreRole = MirroringRoleEnum.Mirror;
            }
            else
            {
                failoverRole = MirroringRoleEnum.Mirror;
                ignoreRole = MirroringRoleEnum.Principal;
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
                            throw new SqlServerMirroringException(string.Format("Did not switch database {0} as it is unknown.", configuredDatabase.DatabaseName));
                        }
                    }
                    else
                    {
                        MirroringRoleEnum databaseRole = Information_Databases_GetDatabaseMirroringRole(database.Name);
                        
                        if (databaseRole == failoverRole)
                        {
                            Logger.LogDebug(string.Format("Trying to switch {0} as it in {1}.", database.Name, databaseRole));
                            database.ChangeMirroringState(MirroringOption.Failover);
                            database.Alter(TerminationClause.RollbackTransactionsImmediately);
                            Logger.LogDebug(string.Format("Database {0} switched from {1} to {2}.", database.Name, databaseRole, failoverRole));
                        }
                        else if (databaseRole == ignoreRole)
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
            Action_ServerRole_Recheck();
        }

        private void Action_Instance_CheckDatabaseMirrorStates()
        {
            try
            {
                Dictionary<string,DatabaseMirrorState> databaseMirrorStates = new Dictionary<string,DatabaseMirrorState>();

                Logger.LogDebug("Information_CheckDatabaseMirrorStates started");

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

                DataSet dataSet = LocalMasterDatabase.ExecuteWithResults(sqlQuery);
                if (dataSet == null || dataSet.Tables.Count == 0)
                {
                    Logger.LogWarning("Information_CheckDatabaseMirrorStates found no rows returning NoDatabase as it might not be an error");
                }
                else
                {
                    DataTable table = dataSet.Tables[0];
                    foreach (DataRow row in table.Rows)
                    {
                        DatabaseMirrorState databaseMirrorState = new DatabaseMirrorState();
                        foreach (DataColumn column in row.Table.Columns)
                        {
                            switch(column.ColumnName)
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
                Information_DatabaseMirrorStates = databaseMirrorStates;
                Logger.LogDebug("Information_CheckDatabaseMirrorStates ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Information_CheckDatabaseMirrorStates failed", ex);
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

        private void Action_ServerState_MakeChange(ServerStateEnum newState)
        {
            if (Information_ServerState_IsValidChange(newState))
            {
                ServerState newServerState;
                if (_serverStates.TryGetValue(newState, out newServerState))
                {
                    Logger.LogInfo(string.Format("Server state changed from {0} to {1}.", Information_ServerState, newServerState));
                    Information_ServerState_Old = Information_ServerState;
                    Information_ServerState = newServerState;
                    Action_ServerState_StartNew(newState);
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

        private void Action_Instance_ChangeSqlAgentServiceToAutomaticStart()
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

        private void Action_Instance_ChangeDatabaseServiceToAutomaticStart()
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

        private void Action_SqlAgent_Start()
        {
            /* TODO: Check that it is the correct instance sql agent */
            foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
            {
                Logger.LogDebug(string.Format("Action_StartSqlAgent: Sql Agent {0}({1}) in state {2}", service.Name, service.DisplayName, service.ServiceState));
                if (service.ServiceState != ServiceState.Running)
                {
                    int timeoutCounter = 0;
                    service.Start();
                    while (service.ServiceState != ServiceState.Running && timeoutCounter > Instance_Configuration.ServiceStartTimeout)
                    {
                        timeoutCounter += Instance_Configuration.ServiceStartTimeoutStep;
                        System.Threading.Thread.Sleep(Instance_Configuration.ServiceStartTimeoutStep);
                        Console.WriteLine(string.Format("Action_StartSqlAgent: Waited {0} seconds for Sql Agent {1}({2}) starting: {3}", (timeoutCounter / 1000), service.Name, service.DisplayName, service.ServiceState));
                        service.Refresh();
                    }
                    if (timeoutCounter > Instance_Configuration.ServiceStartTimeout)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_StartSqlAgent: Timed out waiting for Sql Agent {1}({2}) starting", service.Name, service.DisplayName));
                    }
                }
            }
        }

        private void Action_SqlAgent_Stop()
        {
            /* TODO: Check that it is the correct instance sql agent */
            foreach (Service service in Information_SqlServerServicesInstalled.Where(s => s.Type == ManagedServiceType.SqlAgent))
            {
                Logger.LogDebug(string.Format("Action_StopSqlAgent: Sql Agent {0}({1}) in state {2}", service.Name, service.DisplayName, service.ServiceState));
                if (service.ServiceState != ServiceState.Stopped)
                {
                    int timeoutCounter = 0;
                    service.Stop();
                    while (service.ServiceState != ServiceState.Stopped && timeoutCounter > Instance_Configuration.ServiceStartTimeout)
                    {
                        timeoutCounter += Instance_Configuration.ServiceStartTimeoutStep;
                        System.Threading.Thread.Sleep(Instance_Configuration.ServiceStartTimeoutStep);
                        Console.WriteLine(string.Format("Action_StopSqlAgent: Waited {0} seconds for Sql Agent {1}({2}) stopping: {3}", (timeoutCounter / 1000), service.Name, service.DisplayName, service.ServiceState));
                        service.Refresh();
                    }
                    if (timeoutCounter > Instance_Configuration.ServiceStartTimeout)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_StopSqlAgent: Timed out waiting for Sql Agent {1}({2}) stopping", service.Name, service.DisplayName));
                    }
                }
            }
        }

        private bool Information_SqlAgent_CheckRunning()
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

        private void Action_SqlAgent_EnableAgentXps()
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

        private void Action_ServerState_StartNew(ServerStateEnum newState)
        {
            switch (newState)
            {
                case ServerStateEnum.PRIMARY_STARTUP_STATE:
                    Action_ServerState_StartPrimaryStartup();
                    break;
                case ServerStateEnum.PRIMARY_RUNNING_STATE:
                    Action_ServerState_StartPrimaryRunning();
                    break;
                case ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE:
                    Action_ServerState_StartPrimaryForcedRunning();
                    break;
                case ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE:
                    Action_ServerState_StartPrimaryShuttingDown();
                    break;
                case ServerStateEnum.PRIMARY_SHUTDOWN_STATE:
                    Action_ServerState_StartPrimaryShutdown();
                    break;
                case ServerStateEnum.PRIMARY_MAINTENANCE_STATE:
                    Action_ServerState_StartPrimaryMaintenance();
                    break;
                case ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE:
                    Action_ServerState_StartPrimaryManualFailover();
                    break;
                case ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE:
                    Action_ServerState_StartPrimaryRunningNoSecondary();
                    break;
                case ServerStateEnum.SECONDARY_STARTUP_STATE:
                    Action_ServerState_StartSecondaryStartup();
                    break;
                case ServerStateEnum.SECONDARY_RUNNING_STATE:
                    Action_ServerState_StartSecondaryRunning();
                    break;
                case ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE:
                    Action_ServerState_StartSecondaryRunningNoPrimary();
                    break;
                case ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE:
                    Action_ServerState_StartSecondaryShuttingDown();
                    break;
                case ServerStateEnum.SECONDARY_SHUTDOWN_STATE:
                    Action_ServerState_StartSecondaryShutdown();
                    break;
                case ServerStateEnum.SECONDARY_MAINTENANCE_STATE:
                    Action_ServerState_StartSecondaryMaintenance();
                    break;
                case ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE:
                    Action_ServerState_StartSecondaryManualFailover();
                    break;
                case ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE:
                    Action_ServerState_StartSecondaryForcedManualFailover();
                    break;
                default:
                    throw new SqlServerMirroringException(string.Format("Unknown state {0}.", newState.ToString()));
            }
        }

        private void Action_ServerState_StartSecondaryRunningNoPrimary()
        {
            Logger.LogDebug("Action_StartSecondaryRunningNoPrimaryState starting");
            try
            {
                Logger.LogDebug("Action_StartSecondaryRunningNoPrimaryState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Running State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartSecondaryShuttingDown()
        {
            Logger.LogDebug("StartSecondaryShuttingDownState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartSecondaryShuttingDownState ended");
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTDOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTDOWN_STATE);
            }
        }

        private void Action_ServerState_StartSecondaryShutdown()
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

        private void Action_ServerState_StartSecondaryMaintenance()
        {
            Logger.LogDebug("Action_StartSecondaryMaintenanceState starting");
            try
            {
                if (Information_ServerState_Old.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE)
                {
                    Logger.LogInfo(string.Format("Action_StartSecondaryMaintenanceState: Resume mirroring if not active"));
                    if (Action_Instance_ResumeMirroringForAllDatabases())
                    {
                        Logger.LogInfo(string.Format("Action_StartSecondaryMaintenanceState: Mirroring resumed on databases needed"));
                    }
                    else
                    {
                        Logger.LogWarning(string.Format("Action_StartSecondaryMaintenanceState could not resume on databases."));
                    }
                }
                Logger.LogDebug("Action_StartSecondaryMaintenanceState starting");
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartSecondaryManualFailover()
        {
            Logger.LogDebug("Action_StartSecondaryManualFailoverState starting");
            try
            {
                Action_Instance_FailoverForAllMirrorDatabases();
                Logger.LogDebug("Action_StartSecondaryManualFailoverState ended");
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Secondary Manual Failover State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartSecondaryRunning()
        {
            Logger.LogDebug("Action_StartSecondaryRunningState starting");
            try
            {
                if (Information_ServerState_Old.State == ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE)
                {
                    Logger.LogInfo(string.Format("Action_StartSecondaryRunningState: Resume mirroring if not active"));
                    if (Action_Instance_ResumeMirroringForAllDatabases())
                    {
                        Logger.LogInfo(string.Format("Action_StartSecondaryRunningState: Mirroring resumed on databases needed"));
                    }
                    else
                    {
                        Logger.LogWarning(string.Format("Action_StartSecondaryRunningState could not resume on databases. Switching to old state."));
                        Action_ServerState_MakeChange(Information_ServerState_Old.State);
                    }
                }

                if (!(Information_Instance_ServerRole == ServerRoleEnum.Secondary))
                {
                    Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE);
                }
                Logger.LogDebug("Action_StartSecondaryRunningState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Action_StartSecondaryRunningState: State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartSecondaryStartup()
        {
            Logger.LogDebug("Action_ServerState_StartSecondaryStartup starting");
            try
            {
                Action_ServerRole_Recheck();
                Action_Instance_SwitchOverCheck();
                Logger.LogDebug("Action_ServerState_StartSecondaryStartup ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("Action_ServerState_StartSecondaryStartup: State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartPrimaryRunningNoSecondary()
        {
            Logger.LogDebug("Action_StartPrimaryRunningNoSecondaryState starting");
            try
            {
                Logger.LogDebug("Action_StartPrimaryRunningNoSecondaryState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Running State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartPrimaryStartup()
        {
            Logger.LogDebug("Action_ServerState_StartPrimaryStartup starting");
            try
            {
                Action_ServerRole_Recheck();
                Action_Instance_SwitchOverCheck();

                Logger.LogDebug("Action_ServerState_StartPrimaryStartup ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("Action_ServerState_StartPrimaryStartup: State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartPrimaryRunning()
        {
            Logger.LogDebug("Action_StartPrimaryRunningState starting");
            try
            {
                if(Information_ServerState_Old.State == ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE ||
                   Information_ServerState_Old.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE)
                {
                    Logger.LogInfo(string.Format("Action_StartPrimaryRunningState: Resume mirroring if not active"));
                    if (Action_Instance_ResumeMirroringForAllDatabases())
                    {
                        Logger.LogInfo(string.Format("Action_StartPrimaryRunningState: Mirroring resumed on databases needed"));
                    }
                    else
                    {
                        Logger.LogWarning(string.Format("Action_StartPrimaryRunningState could not resume on databases. Switching to old state."));
                        Action_ServerState_MakeChange(Information_ServerState_Old.State);
                    }
                }

                if (!(Information_Instance_ServerRole == ServerRoleEnum.Primary))
                {
                    Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE);
                }
                Logger.LogDebug("Action_StartPrimaryRunningState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Action_StartPrimaryRunningState: State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartPrimaryForcedRunning()
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
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartPrimaryShuttingDown()
        {
            Logger.LogDebug("StartShuttingDownState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartShuttingDownState ended");
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTDOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTDOWN_STATE);
            }
        }

        private void Action_ServerState_StartPrimaryShutdown()
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

        private void Action_ServerState_StartPrimaryMaintenance()
        {
            Logger.LogDebug("Action_StartPrimaryMaintenanceState starting");
            try
            {
                if (Information_ServerState_Old.State == ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE ||
                   Information_ServerState_Old.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE)
                {
                    Logger.LogInfo(string.Format("Action_StartPrimaryMaintenanceState: Resume mirroring if not active"));
                    if (Action_Instance_ResumeMirroringForAllDatabases())
                    {
                        Logger.LogInfo(string.Format("Action_StartPrimaryMaintenanceState: Mirroring resumed on databases needed"));
                    }
                    else
                    {
                        Logger.LogWarning(string.Format("Action_StartPrimaryMaintenanceState could not resume on databases."));
                    }
                }

                Logger.LogDebug("Action_StartPrimaryMaintenanceState ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("Action_StartPrimaryMaintenanceState could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartPrimaryManualFailover()
        {
            Logger.LogDebug("StartPrimaryManualFailoverState starting");
            try
            {
                Action_Instance_FailoverForAllMirrorDatabases();
                Logger.LogDebug("StartPrimaryManualFailoverState ended");
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Primary Manual Failover State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_ServerState_StartSecondaryForcedManualFailover()
        {
            Logger.LogDebug("StartForcedManualFailoverState starting");
            try
            {
                Action_Instance_ForceFailoverWithDataLossForAllMirrorDatabases();
                Logger.LogDebug("StartForcedManualFailoverState ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void Action_Instance_ValidateConnectionStringAndDatabaseConnection(string connectionString)
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

        private void Action_ServerState_BuildServerStates()
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

        private void Action_ServerState_StartInitial()
        {
            if (Instance_Configuration == null ||Databases_Configuration == null || Databases_Configuration.Count == 0)
            {
                throw new SqlServerMirroringException("Configuration not set before Initial server state");
            }

            /* Create Master Database ServerState */
            if (!Information_ServerState_CheckLocalMasterTable())
            {
                Action_ServerState_CreateMasterTable();
            }

            if (!Information_DatabaseState_CheckLocalMasterTableExists())
            {
                Action_DatabaseState_CreateMasterTable();
            }
            Action_IO_CreateDirectoryAndShare();
            if (!Information_Instance_CheckForMirroring())
            {
                Action_Instance_SetupForMirroring();
            }
            Action_Instance_CheckDatabaseMirrorStates();
            Action_ServerRole_Recheck();
            Action_Instance_CheckDatabaseStates();

            Logger.LogDebug("Action_StartInitialServerState ended.");
        }

        private void Action_IO_CreateDirectoryAndShare()
        {
            Logger.LogDebug(string.Format("Action_CreateDirectoryAndShare started"));
            try
            {
                Logger.LogDebug(string.Format("Creating LocalBackupDirectory: {0}", Instance_Configuration.LocalBackupDirectory));
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, Instance_Configuration.LocalBackupDirectory);
                Logger.LogDebug(string.Format("Creating LocalRestoreDirectory: {0}", Instance_Configuration.LocalRestoreDirectory));
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, Instance_Configuration.LocalRestoreDirectory);
                Logger.LogDebug(string.Format("Creating LocalShareDirectory {0} and LocalShare {1}", Instance_Configuration.LocalRestoreDirectory, Instance_Configuration.LocalShareName));
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(Logger, Instance_Configuration.LocalShareDirectory, Instance_Configuration.LocalShareName);
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_CreateDirectoryAndShare failed", ex);
            }
            Logger.LogDebug(string.Format("Action_CreateDirectoryAndShare ended"));
        }

        private bool Information_DatabaseState_CheckLocalMasterTableExists()
        {
            return Information_DatabaseState_CheckTableExists(LocalMasterDatabase);
        }

        private bool Information_DatabaseState_CheckRemoteMasterTableExists()
        {
            return Information_DatabaseState_CheckTableExists(RemoteMasterDatabase);
        }

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

        private void Action_DatabaseState_CreateMasterTable()
        {
            Logger.LogDebug("Action_DatabaseState_CreateMasterTable started.");
            
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
                Logger.LogDebug("Action_DatabaseState_CreateMasterTable ended.");
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_DatabaseState_CreateMasterTable failed", ex);
            }
        }

        private void Action_DatabaseState_Update(string databaseName, DatabaseStateEnum databaseState, ServerRoleEnum activeServerRole, bool errorDatabaseState, int errorCount)
        {
            try
            {
                Logger.LogDebug("Action_DatabaseState_Update started.");

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

                LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
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
                Logger.LogDebug("Information_DatabaseState_Check started");

                Dictionary<string, DatabaseState> databaseStates = new Dictionary<string, DatabaseState>();
                string sqlQuery = "SELECT DatabaseName, DatabaseState, ServerRole, LastWriteDate, ErrorDatabaseState, ErrorCount FROM DatabaseState";

                DataSet dataSet = databaseToCheck.ExecuteWithResults(sqlQuery);
                if (dataSet == null || dataSet.Tables.Count == 0)
                {
                    Logger.LogWarning("Information_DatabaseState_Check found no rows returning NoDatabase as it might not be an error");
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
                                        throw new SqlServerMirroringException(string.Format("Information_DatabaseState_Check could not parse {0} for DatabaseStateEnum", databaseStateString));
                                    }
                                    databaseState.SetDatabaseState(databaseStateEnum);
                                    break; // NVARCHAR(50)
                                case "ServerRole":
                                    string serverRoleString = (string)row[column];
                                    ServerRoleEnum serverRoleEnum;
                                    if (!Enum.TryParse(serverRoleString, out serverRoleEnum))
                                    {
                                        throw new SqlServerMirroringException(string.Format("Information_DatabaseState_Check could not parse {0} for ServerRoleEnum", serverRoleString));
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
                
                Logger.LogDebug("Information_DatabaseState_Check ended.");
                return databaseStates;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Information_DatabaseState_Check failed", ex);
            }
        }

        private bool Action_DatabaseState_ShiftState()
        {
            Logger.LogDebug("Action_DatabaseState_ShiftState started.");
            int checksToShutDown = Instance_Configuration.ShutDownAfterNumberOfChecksForDatabaseState;

            int countOfChecks = Information_DatabaseState_GetErrorCount();
            if (countOfChecks > checksToShutDown)
            {
                Logger.LogWarning(string.Format("Action_DatabaseState_ShiftState: Will shut down from state {0} because of Database State Error as count {1} is above {2}.", Information_ServerState, countOfChecks, checksToShutDown));
                return true;
            }
            else
            {
                Logger.LogDebug(string.Format("Action_DatabaseState_ShiftState: Should not shut down because of Database State Error as count {0} is not above {1}.", countOfChecks, checksToShutDown));
                return false;
            }
        }

        private bool Action_ServerState_UpdateSecondaryRunningNoPrimaryCount_ShiftState()
        {
            Logger.LogDebug("Action_CheckLastDatabaseStateErrorAndCount_ShiftState started.");
            int checksToShutDown = Instance_Configuration.ShutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState;
            Action_ServerState_Update(LocalMasterDatabase, true, true, false, Information_Instance_ServerRole, Information_ServerState, 1);

            int countOfChecks = Information_ServerState_GetSecondaryRunningNoPrimaryStateCount();
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

        private bool Action_ServerState_UpdatePrimaryRunningNoSecondaryCount_ShiftState()
        {
            Logger.LogDebug("Action_CheckLastDatabaseStateErrorAndCount_ShiftState started.");
            int checksToSwitch = Instance_Configuration.SwitchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState;
            Action_ServerState_Update(LocalMasterDatabase, true, true, false, Information_Instance_ServerRole, Information_ServerState, 1);

            int countOfChecks = Information_ServerState_GetPrimaryRunningNoSecondaryStateCount();
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

        private int Information_ServerState_GetPrimaryRunningNoSecondaryStateCount()
        {
            return Information_ServerState_GetCount(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE);
        }

        private int Information_ServerState_GetSecondaryRunningNoPrimaryStateCount()
        {
            return Information_ServerState_GetCount(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE);
        }

        private int Information_ServerState_GetCount(ServerStateEnum serverState)
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

        private int Information_DatabaseState_GetErrorCount()
        {
            try
            {
                Logger.LogDebug("Information_DatabaseState_GetErrorCount started");

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
                Logger.LogDebug(string.Format("Information_DatabaseState_GetErrorCount ended with error count {0}", maxCount.ToString()));
                return maxCount;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Information_DatabaseState_GetErrorCount failed", ex);
            }
        }

        private MirroringRoleEnum Information_Databases_GetDatabaseMirroringRole(string databaseName)
        {
            Logger.LogDebug("Information_GetDatabaseMirroringRole started");
            DatabaseMirrorState databaseMirrorState;
            if (Information_DatabaseMirrorStates.TryGetValue(databaseName, out databaseMirrorState))
            {
                Logger.LogDebug(string.Format("Information_GetDatabaseMirroringRole found {0}", databaseMirrorState.MirroringRole));
                return databaseMirrorState.MirroringRole;
            }
            else
            {
                throw new SqlServerMirroringException("Information_GetDatabaseMirroringRole failed");
            }
        }
        
        private void Action_ServerState_UpdateLocal_MissingRemoteServer()
        {
            Logger.LogDebug("Action_UpdateRemoteServerState_ConnectedRemoteServer started.");
            Action_ServerState_Update(LocalMasterDatabase, true, false, false, Information_Instance_ServerRole, Information_ServerState, 1);
            Logger.LogDebug("Action_UpdateRemoteServerState_ConnectedRemoteServer ended.");
        }

        private void Action_ServerState_UpdateRemote_ConnectedRemoteServer()
        {
            Logger.LogDebug("Action_UpdateRemoteServerState_ConnectedRemoteServer started.");
            Action_ServerState_Update(RemoteMasterDatabase, false, true, true, Information_Instance_ServerRole, Information_ServerState, 0);
            Logger.LogDebug("Action_UpdateRemoteServerState_ConnectedRemoteServer ended.");
        }

        private void Action_ServerState_UpdateLocal_ConnectedRemoteServer()
        {
            Logger.LogDebug("Action_UpdateLocalServerState_ConnectedRemoteServer started.");
            Action_ServerState_Update(LocalMasterDatabase, true, false, true, Information_Instance_ServerRole, Information_ServerState, 0);
            Logger.LogDebug("Action_UpdateLocalServerState_ConnectedRemoteServer ended.");
        }

        private void Action_ServerState_CreateMasterTable()
        {
            Logger.LogDebug("Action_ServerState_CreateMasterTable started.");

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
                Logger.LogDebug("Action_ServerState_CreateMasterTable ended.");
                Action_ServerState_InsertBaseState();
            }
            catch (SqlServerMirroringException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException("Action_ServerState_CreateMasterTable failed", ex);
            }
        }

        private void Action_ServerState_InsertBaseState()
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

        private void Action_ServerState_Update(Database databaseToUpdate, bool updaterLocal, bool aboutLocal, bool connected, ServerRoleEnum activeServerRole, ServerState activeServerState, int increaseCount)
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

        private bool Information_ServerState_CheckLocalMasterTable()
        {
            return Information_ServerState_CheckMasterTable(LocalMasterDatabase);
        }

        private bool Information_ServerState_CheckRemoteMasterTable()
        {
            return Information_ServerState_CheckMasterTable(RemoteMasterDatabase);
        }

        #endregion

        #region Individual Databases

        private void Action_Databases_AddDatabaseToMirroring(ConfigurationForDatabase configuredDatabase)
        {

            Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
            if (database == null)
            {
                throw new SqlServerMirroringException(string.Format("Action_AddDatabaseToMirroring: Could not find database {0}", configuredDatabase.DatabaseName));
            }
            if (database.RecoveryModel != RecoveryModel.Full)
            {
                try
                {
                    database.RecoveryModel = RecoveryModel.Full;
                    database.Alter(TerminationClause.RollbackTransactionsImmediately);
                    Logger.LogDebug(string.Format("Action_AddDatabaseToMirroring: Database {0} has been set to full recovery", database.Name));
                }
                catch (Exception ex)
                {
                    throw new SqlServerMirroringException(string.Format("Action_AddDatabaseToMirroring: Could not set database {0} to Full Recovery", database.Name), ex);
                }
            }
            if (Action_Databases_BackupDatabaseForMirrorServer(configuredDatabase, true))
            {
                Logger.LogInfo("Action_AddDatabaseToMirroring: Backup created and moved to remote share");
            }
            else
            {
                Logger.LogInfo("Action_AddDatabaseToMirroring: Backup created and moved to local share due to missing access to remote share");
            }
            if (Action_Databases_BackupDatabaseForMirrorServer(configuredDatabase, false))
            {
                Logger.LogInfo("Action_AddDatabaseToMirroring: Log backup created and moved to remote share");
            }
            else
            {
                Logger.LogInfo("Action_AddDatabaseToMirroring: Log backup created and moved to local share due to missing access to remote share");
            }
        }

        private bool Action_Instance_CheckStartMirroring()
        {
            if (Information_RemoteServer_HasAccess())
            {
                if (Information_RemoteServer_ReadyForMirroring())
                {
                    return Action_Instance_StartMirroring();
                }
                else
                {
                    Logger.LogInfo(string.Format("Action_Databases_StartMirroring: Remote server {0} not ready for mirroring.", Instance_Configuration.RemoteServer));
                    return false;
                }
            }
            else
            {
                Logger.LogWarning("Action_Databases_StartMirroring: Could not start mirroring as remote server is it not possible to connect to.");
                return false;
            }
        }


        private bool Information_RemoteServer_ReadyForMirroring()
        {
            bool remoteServerReady = true;
            Dictionary<string, DatabaseState> remoteDatabaseStates = Information_DatabaseState_Check(RemoteMasterDatabase);
            foreach (ConfigurationForDatabase configuration in Databases_Configuration.Values)
            {
                DatabaseState remoteDatabaseState;
                if (remoteDatabaseStates.TryGetValue(configuration.DatabaseName, out remoteDatabaseState))
                {
                    if (remoteDatabaseState.DatabaseStateRecorded == DatabaseStateEnum.READY_FOR_MIRRORING)
                    {
                        Logger.LogDebug(string.Format("Information_RemoteServer_ReadyForMirroring: Database {0} on remote server is ready for mirroring", configuration.DatabaseName));
                    }
                    else
                    {
                        Logger.LogDebug(string.Format("Information_RemoteServer_ReadyForMirroring: Database {0} on remote server is not in correct state {1}"
                            , configuration.DatabaseName, remoteDatabaseState.DatabaseStateRecorded));
                        remoteServerReady =  false;
                    }
                }
                else
                {
                    Logger.LogDebug(string.Format("Information_RemoteServer_ReadyForMirroring: Database {0} does not exist on remote server", configuration.DatabaseName));
                    remoteServerReady =  false;
                }
            }
            Logger.LogDebug(string.Format("Information_RemoteServer_ReadyForMirroring: Databases on remote server ready? : {0}", remoteServerReady?"Yes":"No"));
            return remoteServerReady;
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
            foreach(ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
            {
                if (!Action_Databases_StartMirroring(configuredDatabase))
                {
                    mirroringStarted = false;
                }
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
                    Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName)).FirstOrDefault();
                    if (database == null)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_Databases_StartMirroring: Could not find database {0}", configuredDatabase.DatabaseName));
                    }
                    try
                    {
                        Logger.LogDebug(string.Format("Action_Databases_StartMirroring: Getting ready to start mirroring {0} with partner endpoint on {1} port {2}"
                            , configuredDatabase.DatabaseName, Instance_Configuration.RemoteServer, Instance_Configuration.Endpoint_ListenerPort));

                        database.MirroringPartner = "TCP://" + Instance_Configuration.RemoteServer + ":" + Instance_Configuration.Endpoint_ListenerPort;
                        database.Alter(TerminationClause.RollbackTransactionsImmediately);
                        Logger.LogDebug(string.Format("Action_Databases_StartMirroring: Mirroring started {0} with partner endpoint on {1} port {2}", configuredDatabase.DatabaseName, Instance_Configuration.RemoteServer, Instance_Configuration.Endpoint_ListenerPort));
                        return true;
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(string.Format("Action_Databases_StartMirroring: Creation of mirroring failed for {0}", configuredDatabase.DatabaseName), ex);
                        return false;
                    }
                }
                else
                {
                    Logger.LogDebug(string.Format("Action_Databases_StartMirroring: Database {0} is already in {1}.", configuredDatabase.DatabaseName, databaseMirrorState.MirroringRole));
                    return true;
                }
            }
            else
            {
                Logger.LogDebug(string.Format("Action_Databases_StartMirroring: Could not get configuration for {0}", configuredDatabase.DatabaseName));
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
                Logger.LogDebug(string.Format("Action_CreateEndpoint: Creates endpoint {0}", Instance_Configuration.Endpoint_Name));
                endpoint.Create();
                Logger.LogDebug(string.Format("Action_CreateEndpoint: Starts endpoint {0}", Instance_Configuration.Endpoint_Name));
                endpoint.Start();
                Logger.LogDebug(string.Format("Action_CreateEndpoint: Created endpoint and started. Endpoint in state {1}.", endpoint.EndpointState));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_CreateEndpoint: Creation of endpoint for {0} failed", Instance_Configuration.Endpoint_Name), ex);
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

                DataSet dataSet = LocalMasterDatabase.ExecuteWithResults(sqlQuery);
                if (dataSet.Tables.Count == 0)
                {
                    Logger.LogDebug(string.Format("Information_Endpoint_HasConnectRights {0} has no connect for {1}", endpointName, runnerOfSqlServer));
                    return false;
                }
                else
                {
                    Logger.LogDebug(string.Format("Information_Endpoint_HasConnectRights {0} has connect for {1}", endpointName, runnerOfSqlServer));
                    return true;
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Information_Endpoint_HasConnectRights {0} failed for {1}", endpointName, runnerOfSqlServer), ex);
            }
        }

        private void Action_Endpoint_GrantConnectRights(string endpointName)
        {
            string runnerOfSqlServer = DatabaseServerInstance.ServiceAccount;
            try
            {
                string sqlQuery = string.Format("GRANT CONNECT ON ENDPOINT::{0} TO [{1}]", endpointName, runnerOfSqlServer);
                LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                Logger.LogDebug(string.Format("Action_Endpoint_GrantConnectRights {0} failed for {1}", endpointName, runnerOfSqlServer));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_Endpoint_GrantConnectRights {0} failed for {1}", endpointName, runnerOfSqlServer), ex);
            }
        }

        private void Action_Databases_RemoveDatabaseFromMirroring(DatabaseMirrorState databaseMirrorState, bool serverPrimary)
        {
            Database database = Information_UserDatabases.Where(s => s.Name.Equals(databaseMirrorState.DatabaseName)).FirstOrDefault();
            if (database == null)
            {
                throw new SqlServerMirroringException(string.Format("Action_RemoveDatabaseFromMirroring: Could not find database {0}", databaseMirrorState.DatabaseName));
            }

            try
            {
                database.ChangeMirroringState(MirroringOption.Off);
                database.Alter(TerminationClause.RollbackTransactionsImmediately);
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Removing mirroring failed for {0}", databaseMirrorState.DatabaseName), ex);
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
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, localDirectoryForBackup);

                Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                if (database == null)
                {
                    throw new SqlServerMirroringException(string.Format("Action_BackupDatabaseLog: Could not find database {0}", configuredDatabase.DatabaseName));
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
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, localDirectoryForBackup);

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
            DirectoryHelper.TestReadWriteAccessToDirectory(Logger, localBackupDirectoryWithSubDirectory);
            DirectoryPath localLocalTransferDirectoryWithSubDirectory = configuredDatabase.LocalLocalTransferDirectoryWithSubDirectory;
            ShareName localShareName = configuredDatabase.LocalShareName;
            try
            {
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(Logger, localLocalTransferDirectoryWithSubDirectory, localShareName);
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
                    Action_IO_MoveFileLocalToRemote(fileName, localLocalTransferDirectoryWithSubDirectory, remoteRemoteTransferDirectoryWithSubDirectory);
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
            Logger.LogDebug(string.Format("Action_RestoreDatabaseLog started for {0}", configuredDatabase.DatabaseName));

            string fileName;
            try
            {
                if (Action_IO_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles(configuredDatabase, Instance_Configuration.DatabaseLogBackupSearchPattern))
                {

                    DirectoryPath localRestoreDircetoryWithSubDirectory = configuredDatabase.LocalRestoreDirectoryWithSubDirectory;

                    fileName = Information_IO_GetNewesteFilename(configuredDatabase.DatabaseName.ToString(), localRestoreDircetoryWithSubDirectory.ToString(), Instance_Configuration.DatabaseLogBackupSearchPattern);
                    string fullFileName = localRestoreDircetoryWithSubDirectory + DIRECTORY_SPLITTER + fileName;
                    // Define a Restore object variable.
                    Logger.LogDebug(string.Format("Action_RestoreDatabaseLog for {0} from {1}", configuredDatabase.DatabaseName, fullFileName));
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
                    Logger.LogDebug(string.Format("Action_RestoreDatabaseLog for {0} complete", configuredDatabase.DatabaseName));

                    return true;
                }
                else
                {
                    if (Information_Databases_DatabaseExists(configuredDatabase.DatabaseName.ToString()))
                    {
                        Logger.LogInfo(string.Format("Action_RestoreDatabaseLog: No log backup to restore for {0}.", configuredDatabase.DatabaseName.ToString()));
                        return false;
                    }
                    else
                    {
                        throw new SqlServerMirroringException(string.Format("Action_RestoreDatabaseLog: Could not find database log to restore for {0}.", configuredDatabase.DatabaseName.ToString()));
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_RestoreDatabaseLog: Restore failed for {0}", configuredDatabase.DatabaseName), ex);
            }
        }

        private bool Action_Databases_RestoreDatabase(ConfigurationForDatabase configuredDatabase)
        {
            Logger.LogDebug(string.Format("Action_RestoreDatabase started for {0}", configuredDatabase.DatabaseName));

            string fileName;
            try {
                if (Action_IO_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles(configuredDatabase, Instance_Configuration.DatabaseBackupSearchPattern))
                {

                    DirectoryPath localRestoreDircetoryWithSubDirectory = configuredDatabase.LocalRestoreDirectoryWithSubDirectory;

                    fileName = Information_IO_GetNewesteFilename(configuredDatabase.DatabaseName.ToString(), localRestoreDircetoryWithSubDirectory.ToString(), Instance_Configuration.DatabaseBackupSearchPattern);
                    string fullFileName = localRestoreDircetoryWithSubDirectory + DIRECTORY_SPLITTER + fileName;
                    // Define a Restore object variable.
                    Logger.LogDebug(string.Format("Action_RestoreDatabase for {0} from {1}", configuredDatabase.DatabaseName, fullFileName));
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
                    Logger.LogDebug(string.Format("Action_RestoreDatabase for {0} complete", configuredDatabase.DatabaseName));

                    return true;
                }
                else
                {
                    if (Information_Databases_DatabaseExists(configuredDatabase.DatabaseName.ToString()))
                    {
                        Logger.LogInfo(string.Format("Action_RestoreDatabase: No backup to restore for {0}.", configuredDatabase.DatabaseName.ToString()));
                        return false;
                    }
                    else
                    {
                        throw new SqlServerMirroringException(string.Format("Action_RestoreDatabase: Could not find database to restore for {0}.", configuredDatabase.DatabaseName.ToString()));
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_RestoreDatabase: Restore failed for {0}", configuredDatabase.DatabaseName), ex);
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
                Logger.LogDebug(string.Format("Information_GetNewesteFilename: Found {0} in {1} searching for {2}(...){3}", result.Name, fullPathString, databaseName, searchPattern.Replace("*.", ".")));
                return result.Name;
            }
            else
            {
                Logger.LogDebug(string.Format("Information_GetNewesteFilename: Found nothing in {0} searching for {1}(...){2}", fullPathString, databaseName, searchPattern.Replace("*.", ".")));
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
            files.ForEach(x => { try { System.IO.File.Delete(x); Logger.LogDebug(string.Format("Action_DeleteAllFilesExcept deleted file {0}.", x)); } catch { } });
            Logger.LogDebug(string.Format("Action_DeleteAllFilesExcept for {0} except {1} in pattern {2}.", fullPathString, fileName, searchPattern));
        }

        /* Create local directories if not existing as this might be first time running and 
        *  returns false if no file is found and true if one is found */
        private bool Action_IO_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles(ConfigurationForDatabase configuredDatabase, string searchPattern)
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
                if (Information_RemoteServer_HasAccess())
                {
                    try
                    {
                        remoteLocalTransferDirectoryNewestFileName = Information_IO_GetNewesteFilename(databaseNameString, remoteLocalTransferDirectory.ToString(), searchPattern);
                    }
                    catch (Exception)
                    {
                        Logger.LogWarning(string.Format("Action_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles: Could not access remote directory {0} but have access to server.", remoteLocalTransferDirectory.ToString()));
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
                throw new SqlServerMirroringException("Action_MoveRemoteFileToLocalRestoreAndDeleteOtherFiles failed", ex);
            }
        }

        private void Action_IO_MoveFileLocal(string fileName, DirectoryPath source, DirectoryPath destination)
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

        private void Action_IO_MoveFileRemoteToLocal(string fileName, UncPath source, DirectoryPath destination)
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

        private void Action_IO_MoveFileLocalToRemote(string fileName, DirectoryPath source, UncPath destination)
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

        private void Action_IO_MoveFileRemoteToRemote(string fileName, UncPath source, UncPath destination)
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

        private void Action_IO_CopyFileLocal(string fileName, DirectoryPath source, DirectoryPath destination)
        {
            try
            {
                Logger.LogDebug(string.Format("Action_CopyFileLocal: Trying to copy file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()));
                File.Copy(source.ToString() + DIRECTORY_SPLITTER + fileName, destination.ToString() + DIRECTORY_SPLITTER + fileName);
                Logger.LogInfo(string.Format("Action_CopyFileLocal: Copied file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()));
            }
            catch (Exception ex)
            {
                throw new SqlServerMirroringException(string.Format("Action_CopyFileLocal: Failed copying file {0} from {1} to {2}.", fileName, source.ToString(), destination.ToString()), ex);
            }
        }

        private long Information_IO_GetFileTimePart(string fileName)
        {
            if(string.IsNullOrWhiteSpace(fileName))
            {
                Logger.LogDebug("Information_GetFileTimePart: fileName empty");
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
                    Logger.LogDebug(string.Format("Information_GetFileTimePart: Found filename {0} datepart {1}", fileName, returnValue));
                    return returnValue;
                }
            }
            throw new SqlServerMirroringException(string.Format("Information_GetFileTimePart: Failed to extract time part from {0}.", fileName));
        }

        #endregion
    }
}
