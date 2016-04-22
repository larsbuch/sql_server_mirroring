using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLib
{
    public class ServerStateMonitor
    {
        private SqlServerInstance _sqlServerInstance;
        private Dictionary<ServerStateEnum, ServerState> _serverStates;
        private ServerState _serverState_Active;
        private ServerState _serverState_Old;

        public ServerStateMonitor(SqlServerInstance sqlServerInstance)
        {
            _sqlServerInstance = sqlServerInstance;
            BuildServerStates();
        }

        private void BuildServerStates()
        {
            _serverStates = new Dictionary<ServerStateEnum, ServerState>();

            /* Add Initial State */
            _serverStates.Add(ServerStateEnum.NOT_SET
                , new ServerState(ServerStateEnum.NOT_SET, true, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_INITIAL_STATE, ServerStateEnum.SECONDARY_INITIAL_STATE}));

            /* Add Primary Role server states */
            _serverStates.Add(ServerStateEnum.PRIMARY_INITIAL_STATE
                , new ServerState(ServerStateEnum.PRIMARY_INITIAL_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_CONFIGURATION_STATE, ServerStateEnum.PRIMARY_STARTUP_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_CONFIGURATION_STATE
                , new ServerState(ServerStateEnum.PRIMARY_CONFIGURATION_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE
                , new ServerState(ServerStateEnum.PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_CONFIGURATION_BACKUP_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_CONFIGURATION_BACKUP_STATE
                , new ServerState(ServerStateEnum.PRIMARY_CONFIGURATION_BACKUP_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_CONFIGURATION_WAITING_FOR_SECONDARY_RESTORE_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_CONFIGURATION_WAITING_FOR_SECONDARY_RESTORE_STATE
                , new ServerState(ServerStateEnum.PRIMARY_CONFIGURATION_WAITING_FOR_SECONDARY_RESTORE_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_CONFIGURATION_STARTING_MIRRORING_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_CONFIGURATION_STARTING_MIRRORING_STATE
                , new ServerState(ServerStateEnum.PRIMARY_CONFIGURATION_STARTING_MIRRORING_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_STARTUP_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_STARTUP_STATE
                , new ServerState(ServerStateEnum.PRIMARY_STARTUP_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE, ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_RUNNING_STATE
                , new ServerState(ServerStateEnum.PRIMARY_RUNNING_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_MAINTENANCE_STATE, ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE
                , ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE, ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE }));

            _serverStates.Add(ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE
                , new ServerState(ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE, false, new List<ServerStateEnum>()
                {ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_MAINTENANCE_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE }));

            _serverStates.Add(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE
                , new ServerState(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTDOWN_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_SHUTDOWN_STATE
                , new ServerState(ServerStateEnum.PRIMARY_SHUTDOWN_STATE, true, new List<ServerStateEnum>()
                { }));

            _serverStates.Add(ServerStateEnum.PRIMARY_MAINTENANCE_STATE
                , new ServerState(ServerStateEnum.PRIMARY_MAINTENANCE_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE, ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE
                , ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE
                , new ServerState(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE, ServerStateEnum.PRIMARY_MAINTENANCE_STATE
                , ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE, ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE}));


            /* Add Secondary Role server states */
            _serverStates.Add(ServerStateEnum.SECONDARY_INITIAL_STATE
                , new ServerState(ServerStateEnum.SECONDARY_INITIAL_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_CONFIGURATION_STATE, ServerStateEnum.SECONDARY_STARTUP_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_PRIMARY_BACKUP_FINISH_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_PRIMARY_BACKUP_FINISH_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_PRIMARY_BACKUP_FINISH_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_STARTUP_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_STARTUP_STATE
                , new ServerState(ServerStateEnum.SECONDARY_STARTUP_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_RUNNING_STATE, ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_RUNNING_STATE
                , new ServerState(ServerStateEnum.SECONDARY_RUNNING_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_MAINTENANCE_STATE, ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE
                , ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE }));

            _serverStates.Add(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE
                , new ServerState(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE, false, new List<ServerStateEnum>()
                {ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE
                , ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, ServerStateEnum.SECONDARY_RUNNING_STATE }));

            _serverStates.Add(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE
                , new ServerState(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTDOWN_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_SHUTDOWN_STATE
                , new ServerState(ServerStateEnum.SECONDARY_SHUTDOWN_STATE, true, new List<ServerStateEnum>()
                { }));

            _serverStates.Add(ServerStateEnum.SECONDARY_MAINTENANCE_STATE
                , new ServerState(ServerStateEnum.SECONDARY_MAINTENANCE_STATE, false, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_RUNNING_STATE
                , ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE, true, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE}));

            /* Set default state */
            if (!_serverStates.TryGetValue(ServerStateEnum.NOT_SET, out _serverState_Active))
            {
                throw new SqlServerMirroringException("Could not get NOT_SET Server State.");
            }
        }

        #region Properties

        public SqlServerInstance SqlServerInstance
        {
            get
            {
                return _sqlServerInstance;
            }
        }

        public ServerState ServerState_Old
        {
            get
            {
                return _serverState_Old;
            }
            set
            {
                _serverState_Old = value;
            }
        }
        public ServerState ServerState_Active
        {
            get
            {
                return _serverState_Active;
            }
            set
            {
                _serverState_Active = value;
            }
        }

        public bool IsInDegradedState
        {
            get
            {
                return _serverState_Active.IsDegradedState;
            }
        }

        public HelperFunctions.ILogger Logger
        {
            get
            {
                return SqlServerInstance.Logger;
            }
        }

        #endregion
        
        #region TimedChecks

        public void TimedCheck()
        {
            SqlServerInstance.Action_ServerState_Update();
            switch (ServerState_Active.State)
            {
                case ServerStateEnum.PRIMARY_INITIAL_STATE:
                    TimedCheck_PrimaryInitialState();
                    break;
                case ServerStateEnum.PRIMARY_CONFIGURATION_STATE:
                    TimedCheck_PrimaryConfigurationState();
                    break;
                case ServerStateEnum.PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE:
                    TimedCheck_PrimaryConfigurationCreateDatabaseFoldersState();
                    break;
                case ServerStateEnum.PRIMARY_CONFIGURATION_BACKUP_STATE:
                    TimedCheck_PrimaryConfigurationBackupState();
                    break;
                case ServerStateEnum.PRIMARY_CONFIGURATION_WAITING_FOR_SECONDARY_RESTORE_STATE:
                    TimedCheck_PrimaryConfigurationWaitingForSecondaryRestoreState();
                    break;
                case ServerStateEnum.PRIMARY_CONFIGURATION_STARTING_MIRRORING_STATE:
                    TimedCheck_PrimaryConfigurationStartingMirroringState();
                    break;
                case ServerStateEnum.PRIMARY_STARTUP_STATE:
                    TimedCheck_PrimaryStartupState();
                    break;
                case ServerStateEnum.PRIMARY_RUNNING_STATE:
                    TimedCheck_PrimaryRunningState();
                    break;
                case ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE:
                    TimedCheck_PrimaryForcedRunningState();
                    break;
                case ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE:
                    TimedCheck_PrimaryShuttingDownState();
                    break;
                case ServerStateEnum.PRIMARY_SHUTDOWN_STATE:
                    TimedCheck_PrimaryShutdownState();
                    break;
                case ServerStateEnum.PRIMARY_MAINTENANCE_STATE:
                    TimedCheck_PrimaryMaintainanceState();
                    break;
                case ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE:
                    TimedCheck_PrimaryManualFailoverState();
                    break;
                case ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE:
                    TimedCheck_PrimaryRunningNoSecondaryState();
                    break;
                case ServerStateEnum.SECONDARY_INITIAL_STATE:
                    TimedCheck_SecondaryInitialState();
                    break;
                case ServerStateEnum.SECONDARY_CONFIGURATION_STATE:
                    TimedCheck_SecondaryConfigurationState();
                    break;
                case ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE:
                    TimedCheck_SecondaryConfigurationCreateDatabaseFoldersState();
                    break;
                case ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_PRIMARY_BACKUP_FINISH_STATE:
                    TimedCheck_SecondaryConfigurationWaitingForPrimaryBackupFinishState();
                    break;
                case ServerStateEnum.SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE:
                    TimedCheck_SecondaryConfigurationRestoringDatabasesState();
                    break;
                case ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE:
                    TimedCheck_SecondaryConfigurationWaitingForMirroringState();
                    break;
                case ServerStateEnum.SECONDARY_STARTUP_STATE:
                    TimedCheck_SecondaryStartupState();
                    break;
                case ServerStateEnum.SECONDARY_RUNNING_STATE:
                    TimedCheck_SecondaryRunningState();
                    break;
                case ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE:
                    TimedCheck_SecondaryShuttingDownState();
                    break;
                case ServerStateEnum.SECONDARY_SHUTDOWN_STATE:
                    TimedCheck_SecondaryShutdownState();
                    break;
                case ServerStateEnum.SECONDARY_MAINTENANCE_STATE:
                    TimedCheck_SecondaryMaintenanceState();
                    break;
                case ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE:
                    TimedCheck_SecondaryManualFailoverState();
                    break;
                case ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE:
                    TimedCheck_SecondaryForcedManualFailoverState();
                    break;
                case ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE:
                    TimedCheck_SecondaryRunningNoPrimaryState();
                    break;
            }
        }

        #region Primary States

        private void TimedCheck_PrimaryInitialState()
        {
            if (SqlServerInstance.Information_Instance_ConfigurationComplete)
            {
                throw new SqlServerMirroringException("Configuration not set before Initial server state");
            }

            if (SqlServerInstance.Information_Instance_Configured())
            {
                MakeServerStateChange(ServerStateEnum.PRIMARY_STARTUP_STATE);
            }
            else
            {
                MakeServerStateChange(ServerStateEnum.PRIMARY_CONFIGURATION_STATE);
            }
        }

        private void TimedCheck_PrimaryConfigurationState()
        {
            try
            {
                /* Create Master Database ServerState */
                if (!SqlServerInstance.Information_ServerState_CheckLocalMasterTable())
                {
                    SqlServerInstance.Action_ServerState_CreateMasterTable();
                }

                if (!SqlServerInstance.Information_DatabaseState_CheckLocalMasterTableExists())
                {
                    SqlServerInstance.Action_DatabaseState_CreateMasterTable();
                }

                if (!SqlServerInstance.Information_Instance_CheckForMirroring())
                {
                    SqlServerInstance.Action_Instance_SetupForMirroring();
                }

                MakeServerStateChange(ServerStateEnum.PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("TimedCheck_PrimaryConfigurationCreateDatabaseFoldersState failed", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryConfigurationCreateDatabaseFoldersState()
        {
            try
            {
                SqlServerInstance.Action_IO_CreateDirectoryAndShare();
                MakeServerStateChange(ServerStateEnum.PRIMARY_CONFIGURATION_BACKUP_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("TimedCheck_PrimaryConfigurationCreateDatabaseFoldersState failed", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryConfigurationBackupState()
        {
            try
            {
                SqlServerInstance.Action_Instance_BackupForAllConfiguredDatabasesForMirrorServer();
                MakeServerStateChange(ServerStateEnum.PRIMARY_CONFIGURATION_WAITING_FOR_SECONDARY_RESTORE_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("TimedCheck_PrimaryConfigurationBackupState failed", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryConfigurationWaitingForSecondaryRestoreState()
        {
            throw new NotImplementedException();
        }

        private void TimedCheck_PrimaryConfigurationStartingMirroringState()
        {
            throw new NotImplementedException();
        }

        private void TimedCheck_PrimaryStartupState()
        {
            Action_RemoteServer_CheckAccess();
            Action_Instance_CheckDatabaseMirrorStates();
            Action_ServerRole_Recheck();
            Action_Instance_CheckDatabaseStates();

            Action_ServerState_StartTimedCheckTimer();
            Action_Databases_StartUpMirrorCheck(true);
            Action_Instance_StartBackupTimer();


            if (SqlServerInstance.Information_Instance_AllConfiguredDatabasesMirrored())
            {
                if (SqlServerInstance.Information_RemoteServer_HasAccess())
                {
                    MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_STATE);
                }
                else
                {
                    Logger.LogWarning(string.Format("Action_ServerState_TimedCheck: No access to remote server"));
                    MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE);
                }
            }
            else
            {
                MakeServerStateChange(ServerStateEnum.PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE);
            }
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
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryRunningState()
        {
            if (!SqlServerInstance.Information_RemoteServer_HasAccess())
            {
                Action_ServerState_UpdateLocal_MissingRemoteServer();
                MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE);
            }
            Logger.LogDebug("Action_StartPrimaryRunningState starting");
            try
            {
                if (Information_ServerState_Old.State == ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE ||
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
                    MakeServerStateChange(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE);
                }
                Logger.LogDebug("Action_StartPrimaryRunningState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Action_StartPrimaryRunningState: State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryForcedRunningState()
        {
            Logger.LogDebug("StartForcedRunningState starting");
            try
            {
                Logger.LogDebug("StartForcedRunningState starting");
            }
            catch (Exception ex)
            {
                Logger.LogError("Forced Running State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryShuttingDownState()
        {
            Logger.LogDebug("StartShuttingDownState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartShuttingDownState ended");
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTDOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTDOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryShutdownState()
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

        private void TimedCheck_PrimaryMaintainanceState()
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
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryManualFailoverState()
        {
            Logger.LogDebug("StartPrimaryManualFailoverState starting");
            try
            {
                /* TODO Only fail over the fist as all others will join if in witness mode */
                foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
                {
                    try
                    {
                        Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                        if (database == null)
                        {
                            throw new SqlServerMirroringException(string.Format("Action_FailoverAction_Instance_FailoverForAllMirrorDatabasesForAllMirrorDatabases: Could not find database {0}", configuredDatabase.DatabaseName));
                        }

                        string sqlQuery = string.Format("ALTER DATABASE {0} SET PARTNER FAILOVER", database.Name);

                        LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                        Logger.LogWarning(string.Format("Action_Instance_FailoverForAllMirrorDatabases: Database {0} has been switched over", database.Name));
                    }
                    catch (Exception ex)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_Instance_FailoverForAllMirrorDatabases: Failover failed for {0}", configuredDatabase.DatabaseName), ex);
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
                Logger.LogDebug("StartPrimaryManualFailoverState ended");
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Primary Manual Failover State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryRunningNoSecondaryState()
        {
            if(SqlServerInstance.Information_RemoteServer_HasAccess())
            {
                Action_ServerState_UpdateLocal_ConnectedRemoteServer();
                Action_ServerState_UpdateRemote_ConnectedRemoteServer();

                Action_ServerState_Update(LocalMasterDatabase, true, true, false, Information_Instance_ServerRole, Information_ServerState, 0);
                MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_STATE);
            }
            else
            {
                Action_ServerState_UpdateLocal_MissingRemoteServer();
                if (Action_ServerState_UpdatePrimaryRunningNoSecondaryCount_ShiftState())
                {
                    MakeServerStateChange(ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE);
                }
            }
            Logger.LogDebug("Action_StartPrimaryRunningNoSecondaryState starting");
            try
            {
                Action_Instance_StartEmergencyBackupTimer();
                Logger.LogDebug("Action_StartPrimaryRunningNoSecondaryState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Running State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        #endregion

        #region Secondary States

        private void TimedCheck_SecondaryInitialState()
        {
            if (SqlServerInstance.Information_Instance_ConfigurationComplete)
            {
                throw new SqlServerMirroringException("Configuration not set before Initial server state");
            }

            if (SqlServerInstance.Information_Instance_Configured())
            {
                MakeServerStateChange(ServerStateEnum.SECONDARY_STARTUP_STATE);
            }
            else
            {
                MakeServerStateChange(ServerStateEnum.SECONDARY_CONFIGURATION_STATE);
            }
        }

        private void TimedCheck_SecondaryConfigurationState()
        {
            throw new NotImplementedException();
        }

        private void TimedCheck_SecondaryConfigurationCreateDatabaseFoldersState()
        {
            throw new NotImplementedException();
        }

        private void TimedCheck_SecondaryConfigurationWaitingForPrimaryBackupFinishState()
        {
            throw new NotImplementedException();
        }

        private void TimedCheck_SecondaryConfigurationRestoringDatabasesState()
        {
            try
            {
                SqlServerInstance.Action_Instance_RestoreDatabases();
                MakeServerStateChange(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE);
            }
            catch(Exception ex)
            {
                Logger.LogError("TimedCheck_SecondaryConfigurationRestoringDatabasesState failed", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryConfigurationWaitingForMirroringState()
        {
            throw new NotImplementedException();
        }

        private void TimedCheck_SecondaryStartupState()
        {
            if (SqlServerInstance.Information_Instance_AllConfiguredDatabasesMirrored())
            {
                if (SqlServerInstance.Information_RemoteServer_HasAccess())
                {
                    MakeServerStateChange(ServerStateEnum.SECONDARY_RUNNING_STATE);
                }
                else
                {
                    MakeServerStateChange(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE);
                }
            }
            else
            {
                MakeServerStateChange(ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE);
            }
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
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryRunningState()
        {
            if (!SqlServerInstance.Information_RemoteServer_HasAccess())
            {
                Action_ServerState_UpdateLocal_MissingRemoteServer();
                MakeServerStateChange(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE);
            }
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
                    MakeServerStateChange(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE);
                }
                Logger.LogDebug("Action_StartSecondaryRunningState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Action_StartSecondaryRunningState: State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryShuttingDownState()
        {
            Logger.LogDebug("StartSecondaryShuttingDownState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("StartSecondaryShuttingDownState ended");
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTDOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTDOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryShutdownState()
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

        private void TimedCheck_SecondaryMaintenanceState()
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
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryManualFailoverState()
        {
            Logger.LogDebug("Action_StartSecondaryManualFailoverState starting");
            try
            {
                /* TODO Only fail over the fist as all others will join if in witness mode */
                foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
                {
                    try
                    {
                        Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                        if (database == null)
                        {
                            throw new SqlServerMirroringException(string.Format("Action_FailoverAction_Instance_FailoverForAllMirrorDatabasesForAllMirrorDatabases: Could not find database {0}", configuredDatabase.DatabaseName));
                        }

                        string sqlQuery = string.Format("ALTER DATABASE {0} SET PARTNER FAILOVER", database.Name);

                        LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                        Logger.LogWarning(string.Format("Action_Instance_FailoverForAllMirrorDatabases: Database {0} has been switched over", database.Name));
                    }
                    catch (Exception ex)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_Instance_FailoverForAllMirrorDatabases: Failover failed for {0}", configuredDatabase.DatabaseName), ex);
                    }
                }
                else if (Information_ServerState.State == ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE)
                {
                    Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                }
                Logger.LogDebug("Action_StartSecondaryManualFailoverState ended");
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Secondary Manual Failover State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryForcedManualFailoverState()
        {
            Logger.LogDebug("StartForcedManualFailoverState starting");
            try
            {
                /* TODO Only fail over the first as all others will join if on multi mirror server with witness */
                foreach (ConfigurationForDatabase configuredDatabase in Databases_Configuration.Values)
                {
                    try
                    {
                        Database database = Information_UserDatabases.Where(s => s.Name.Equals(configuredDatabase.DatabaseName.ToString())).FirstOrDefault();
                        if (database == null)
                        {
                            throw new SqlServerMirroringException(string.Format("Action_Instance_ForceFailoverWithDataLossForAllMirrorDatabases: Could not find database {0}", configuredDatabase.DatabaseName));
                        }
                        string sqlQuery = string.Format("ALTER DATABASE {0} SET PARTNER FORCE_SERVICE_ALLOW_DATA_LOSS", database.Name);

                        LocalMasterDatabase.ExecuteNonQuery(sqlQuery);
                        Logger.LogWarning(string.Format("Action_Instance_ForceFailoverWithDataLossForAllMirrorDatabases: Database {0} has been switched over with data loss", database.Name));

                    }
                    catch (Exception ex)
                    {
                        throw new SqlServerMirroringException(string.Format("Action_Instance_ForceFailoverWithDataLossForAllMirrorDatabases failed for {0}", configuredDatabase.DatabaseName), ex);
                    }
                }
                if (Information_ServerState.State == ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE)
                {
                    Action_ServerState_MakeChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                }
                Logger.LogDebug("StartForcedManualFailoverState ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryRunningNoPrimaryState()
        {
            if (SqlServerInstance.Information_RemoteServer_HasAccess())
            {
                Action_ServerState_UpdateLocal_ConnectedRemoteServer();
                Action_ServerState_UpdateRemote_ConnectedRemoteServer();
                Action_ServerState_Update(LocalMasterDatabase, true, true, false, Information_Instance_ServerRole, Information_ServerState, 0);
                MakeServerStateChange(ServerStateEnum.SECONDARY_RUNNING_STATE);
            }

            else
            {
                Action_ServerState_UpdateLocal_MissingRemoteServer();
                if (Action_ServerState_UpdateSecondaryRunningNoPrimaryCount_ShiftState())
                {
                    MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                }

            }
            Logger.LogDebug("Action_StartSecondaryRunningNoPrimaryState starting");
            try
            {
                Logger.LogDebug("Action_StartSecondaryRunningNoPrimaryState ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("Running State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        #endregion

        #endregion

        public void MakeServerStateChange(ServerStateEnum newState)
        {
            if (ServerState_Active.ValidTransition(newState))
            {
                ServerState newServerState;
                if (_serverStates.TryGetValue(newState, out newServerState))
                {
                    Logger.LogInfo(string.Format("Server state changed from {0} to {1}.", ServerState_Active, newServerState));
                    ServerState_Old = ServerState_Active;
                    ServerState_Active = newServerState;
                    Logger.LogDebug(string.Format("Server in new state {0}.", newServerState));
                }
                else
                {
                    throw new SqlServerMirroringException(string.Format("Server in state {0} could not get new state {1}.", ServerState_Active, newState));
                }
            }
            else
            {
                throw new SqlServerMirroringException(string.Format("Server in state {0} does not allow state shange to {1}.", ServerState_Active, newState));
            }
        }

        public void StartPrimary()
        {
            MakeServerStateChange(ServerStateEnum.PRIMARY_INITIAL_STATE);
            TimedCheck();
        }

        public void StartSecondary()
        {
            MakeServerStateChange(ServerStateEnum.SECONDARY_INITIAL_STATE);
            TimedCheck();
        }
    }
}
