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
                , new ServerState(ServerStateEnum.NOT_SET, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_INITIAL_STATE, ServerStateEnum.SECONDARY_INITIAL_STATE}));

            #region Primary Role States

            /* Add Primary Role server states */
            _serverStates.Add(ServerStateEnum.PRIMARY_INITIAL_STATE
                , new ServerState(ServerStateEnum.PRIMARY_INITIAL_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_CONFIGURATION_STATE, ServerStateEnum.PRIMARY_STARTUP_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_CONFIGURATION_STATE
                , new ServerState(ServerStateEnum.PRIMARY_CONFIGURATION_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE
                , new ServerState(ServerStateEnum.PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_CONFIGURATION_BACKUP_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_CONFIGURATION_BACKUP_STATE
                , new ServerState(ServerStateEnum.PRIMARY_CONFIGURATION_BACKUP_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_CONFIGURATION_WAITING_FOR_SECONDARY_RESTORE_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_CONFIGURATION_WAITING_FOR_SECONDARY_RESTORE_STATE
                , new ServerState(ServerStateEnum.PRIMARY_CONFIGURATION_WAITING_FOR_SECONDARY_RESTORE_STATE, CountStates.Yes, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_CONFIGURATION_STARTING_MIRRORING_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_CONFIGURATION_STARTING_MIRRORING_STATE
                , new ServerState(ServerStateEnum.PRIMARY_CONFIGURATION_STARTING_MIRRORING_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_STARTUP_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_STARTUP_STATE
                , new ServerState(ServerStateEnum.PRIMARY_STARTUP_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE, ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_RUNNING_STATE
                , new ServerState(ServerStateEnum.PRIMARY_RUNNING_STATE, CountStates.No, MirrorState.Mirrored, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_MAINTENANCE_STATE, ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE
                , ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE, ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE }));

            _serverStates.Add(ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE
                , new ServerState(ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                {ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_MAINTENANCE_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE }));

            _serverStates.Add(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE
                , new ServerState(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTDOWN_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_SHUTDOWN_STATE
                , new ServerState(ServerStateEnum.PRIMARY_SHUTDOWN_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { }));

            _serverStates.Add(ServerStateEnum.PRIMARY_MAINTENANCE_STATE
                , new ServerState(ServerStateEnum.PRIMARY_MAINTENANCE_STATE, CountStates.No, MirrorState.Mirrored, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE, ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE
                , ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE}));

            _serverStates.Add(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE
                , new ServerState(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE, ServerStateEnum.PRIMARY_RUNNING_STATE, ServerStateEnum.PRIMARY_MAINTENANCE_STATE
                , ServerStateEnum.PRIMARY_MANUAL_FAILOVER_STATE, ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE}));

            #endregion

            #region Secondary Role States

            /* Add Secondary Role server states */
            _serverStates.Add(ServerStateEnum.SECONDARY_INITIAL_STATE
                , new ServerState(ServerStateEnum.SECONDARY_INITIAL_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_CONFIGURATION_STATE, ServerStateEnum.SECONDARY_STARTUP_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_PRIMARY_BACKUP_FINISH_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_LOOKING_FOR_BACKUP_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_PRIMARY_BACKUP_FINISH_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_PRIMARY_BACKUP_FINISH_STATE, CountStates.Yes, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_LOOKING_FOR_BACKUP_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_LOOKING_FOR_BACKUP_STATE, CountStates.Yes, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE}));
            
            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE
                , new ServerState(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE, CountStates.Yes, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_STARTUP_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_STARTUP_STATE
                , new ServerState(ServerStateEnum.SECONDARY_STARTUP_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_RUNNING_STATE, ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_RUNNING_STATE
                , new ServerState(ServerStateEnum.SECONDARY_RUNNING_STATE, CountStates.No, MirrorState.Mirrored, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_MAINTENANCE_STATE, ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE
                , ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE }));

            _serverStates.Add(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE
                , new ServerState(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                {ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE
                , ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, ServerStateEnum.SECONDARY_RUNNING_STATE }));

            _serverStates.Add(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE
                , new ServerState(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTDOWN_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_SHUTDOWN_STATE
                , new ServerState(ServerStateEnum.SECONDARY_SHUTDOWN_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { }));

            _serverStates.Add(ServerStateEnum.SECONDARY_MAINTENANCE_STATE
                , new ServerState(ServerStateEnum.SECONDARY_MAINTENANCE_STATE, CountStates.No, MirrorState.Mirrored, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE, ServerStateEnum.SECONDARY_RUNNING_STATE
                , ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.SECONDARY_MANUAL_FAILOVER_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE}));

            _serverStates.Add(ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE
                , new ServerState(ServerStateEnum.SECONDARY_FORCED_MANUAL_FAILOVER_STATE, CountStates.No, MirrorState.Degraded, new List<ServerStateEnum>()
                { ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE}));

            #endregion

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

        public int ServerState_ActiveCount
        {
            get
            {
                return ServerState_Active.ServerStateCount;
            }
            set
            {
                ServerState_Active.ServerStateCount = value;
            }
        }

        public bool IsInDegradedState
        {
            get
            {
                return _serverState_Active.MirrorState == MirrorState.Degraded? true :false;
            }
        }

        public SqlServerLogger Logger
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
            ServerState_ActiveCount += 1;
            SqlServerInstance.Action_ServerState_Update(ServerState_Active);

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
                case ServerStateEnum.SECONDARY_CONFIGURATION_LOOKING_FOR_BACKUP_STATE:
                    TimedCheck_SecondaryConfigurationLookingForBackupState();
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
            try
            {
                if (SqlServerInstance.Information_Instance_ConfigurationComplete)
                {
                    Logger.LogError("Configuration not set before Initial server state");
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
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryConfigurationState()
        {
            try
            {
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
                Logger.LogError("Failed", ex);
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
                Logger.LogError("Failed", ex);
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
                Logger.LogError("Failed", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryConfigurationWaitingForSecondaryRestoreState()
        {
            try
            {
                if(SqlServerInstance.Information_RemoteServer_SecondaryRestoreFinished())
                {
                    Logger.LogDebug("Restore on secondary finished");
                    MakeServerStateChange(ServerStateEnum.PRIMARY_CONFIGURATION_STARTING_MIRRORING_STATE);
                }

                CheckTimeout(SqlServerInstance.Instance_Configuration.PrimaryConfigurationWaitNumberOfChecksForSecondaryRestoreTimeout, ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Failed", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryConfigurationStartingMirroringState()
        {
            try
            {
                if(SqlServerInstance.Action_Instance_CheckStartMirroring())
                {
                    Logger.LogInfo("Mirroring started");
                    if (SqlServerInstance.Instance_Configuration.StopRunAfterConfiguration)
                    {
                        MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                    }
                    else
                    {
                        MakeServerStateChange(ServerStateEnum.PRIMARY_STARTUP_STATE);
                    }
                }
                else
                {
                    CheckTimeout(SqlServerInstance.Instance_Configuration.PrimaryConfigurationWaitNumberOfChecksForMirroringStartTimeout, ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError("Failed", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryStartupState()
        {
            Logger.LogDebug("Starting");
            try
            {
                SqlServerInstance.Action_Instance_StartDelayedBackupTimer();

                if (SqlServerInstance.Information_Instance_AllConfiguredDatabasesMirrored())
                {
                    if (SqlServerInstance.Information_RemoteServer_HasAccess())
                    {
                        MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_STATE);
                    }
                    else
                    {
                        Logger.LogWarning(string.Format("No access to remote server"));
                        MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE);
                    }
                }
                else
                {
                    MakeServerStateChange(ServerStateEnum.PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE);
                }
                Logger.LogDebug("Ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryRunningState()
        {
            Logger.LogDebug("Starting");
            try
            {
                if (!SqlServerInstance.Information_RemoteServer_HasAccess())
                {
                    MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE);
                }
                else if ( ServerState_ActiveCount == 1 && 
                        (ServerState_Old.State == ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE ||
                        ServerState_Old.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE))
                {
                    Logger.LogInfo("Resume mirroring if not active");
                    if (SqlServerInstance.Action_Instance_ResumeMirroringForAllDatabases())
                    {
                        Logger.LogInfo(string.Format("Mirroring resumed on databases needed"));
                    }
                    else
                    {
                        Logger.LogWarning(string.Format("Could not resume on databases. Switching to old state."));
                        MakeServerStateChange(ServerState_Old.State);
                    }
                }
                Logger.LogDebug("Ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryForcedRunningState()
        {
            Logger.LogDebug("Starting");
            try
            {
                /* Do nothing special */
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
                Logger.LogDebug("Ended");
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTDOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting Down State could not be started.", ex);
                throw new SqlServerMirroringException("Start Shutting Down State could not be started.", ex);
            }
        }

        private void TimedCheck_PrimaryShutdownState()
        {
            Logger.LogDebug("Starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("Ended");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                Logger.LogError("Shutdown State could not be finished", ex);
                throw new SqlServerMirroringException("Shutdown State could not be finished", ex);
            }
        }

        private void TimedCheck_PrimaryMaintainanceState()
        {
            Logger.LogDebug("Starting");
            try
            {
                if (ServerState_ActiveCount == 1 &&
                    SqlServerInstance.Information_RemoteServer_HasAccess() &&
                        (ServerState_Old.State == ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE ||
                        ServerState_Old.State == ServerStateEnum.PRIMARY_RUNNING_NO_SECONDARY_STATE))
                {
                    Logger.LogInfo("Resume mirroring if not active");
                    if (SqlServerInstance.Action_Instance_ResumeMirroringForAllDatabases())
                    {
                        Logger.LogInfo(string.Format("Mirroring resumed on databases needed"));
                    }
                    else
                    {
                        Logger.LogWarning(string.Format("Could not resume on databases."));
                    }
                }
                Logger.LogDebug("Ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryManualFailoverState()
        {
            Logger.LogDebug("Starting");
            try
            {
                SqlServerInstance.Action_Instance_FailoverForAllMirrorDatabases();
                Logger.LogDebug("Ended");
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_PrimaryRunningNoSecondaryState()
        {
            Logger.LogDebug("Starting");
            try
            {
                if (SqlServerInstance.Information_RemoteServer_HasAccess())
                {
                    MakeServerStateChange(ServerStateEnum.PRIMARY_RUNNING_STATE);
                }
                else if (ServerState_ActiveCount == 1)
                {
                    SqlServerInstance.Action_Instance_StartEmergencyBackupTimer();
                }

                else
                {
                    CheckTimeout(SqlServerInstance.Instance_Configuration.SwitchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState, ServerStateEnum.PRIMARY_FORCED_RUNNING_STATE);
                }
                Logger.LogDebug("Ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.PRIMARY_SHUTTING_DOWN_STATE);
            }
        }

        #endregion

        #region Secondary States

        private void TimedCheck_SecondaryInitialState()
        {
            try
            {
                if (SqlServerInstance.Information_Instance_ConfigurationComplete)
                {
                    Logger.LogError("Configuration not set before Initial server state");
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
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryConfigurationState()
        {
            try
            {
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

                MakeServerStateChange(ServerStateEnum.SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Failed", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryConfigurationCreateDatabaseFoldersState()
        {
            try
            {
                SqlServerInstance.Action_IO_CreateDirectoryAndShare();
                if (SqlServerInstance.Information_RemoteServer_ServeredMirroring())
                {
                    MakeServerStateChange(ServerStateEnum.SECONDARY_CONFIGURATION_LOOKING_FOR_BACKUP_STATE);
                }
                else
                {
                    MakeServerStateChange(ServerStateEnum.SECONDARY_CONFIGURATION_WAITING_FOR_PRIMARY_BACKUP_FINISH_STATE);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError("Failed", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryConfigurationWaitingForPrimaryBackupFinishState()
        {
            try
            {
                if(SqlServerInstance.Information_RemoteServer_BackupFinished())
                {
                    MakeServerStateChange(ServerStateEnum.SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE);
                }
                else
                {
                    CheckTimeout(SqlServerInstance.Instance_Configuration.SecondaryConfigurationWaitNumberOfChecksForPrimaryBackupTimeout, ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError("Failed", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryConfigurationLookingForBackupState()
        {
            try
            {
                if (SqlServerInstance.Information_IO_BackupLocatedForAllDatabases())
                {
                    MakeServerStateChange(ServerStateEnum.SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE);
                }
                else
                {
                    CheckTimeout(SqlServerInstance.Instance_Configuration.SecondaryConfigurationWaitNumberOfChecksLookingForBackupTimeout, ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError("Failed", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
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
                Logger.LogError("Failed", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryConfigurationWaitingForMirroringState()
        {
            try
            {
                if (SqlServerInstance.Information_Instance_AllConfiguredDatabasesMirrored())
                {
                    if (SqlServerInstance.Instance_Configuration.StopRunAfterConfiguration)
                    {
                        MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                    }
                    else
                    {
                        MakeServerStateChange(ServerStateEnum.SECONDARY_STARTUP_STATE);
                    }
                }
                else
                {
                    CheckTimeout(SqlServerInstance.Instance_Configuration.SecondaryConfigurationWaitNumberOfChecksForMirroringTimeout, ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError("Failed", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryStartupState()
        {
            Logger.LogDebug("Starting");
            try
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
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryRunningState()
        {
            Logger.LogDebug("Starting");
            try
            {
                if (!SqlServerInstance.Information_RemoteServer_HasAccess())
                {
                    MakeServerStateChange(ServerStateEnum.SECONDARY_RUNNING_NO_PRIMARY_STATE);
                }
                /* Does not do something special */
                Logger.LogDebug("Ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryShuttingDownState()
        {
            Logger.LogDebug("Starting");
            try
            {
                /* Does not do something special */
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTDOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("Start Shutting down State could not be started.", ex);
                throw new SqlServerMirroringException("Start Shutting Down State could not be started.", ex);
            }
        }

        private void TimedCheck_SecondaryShutdownState()
        {
            Logger.LogDebug("Action_StartSecondaryShutdownState starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("Ended");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                Logger.LogError("Shutdown State could not be finished", ex);
                throw new SqlServerMirroringException("Shutdown State could not be finished", ex);
            }
        }

        private void TimedCheck_SecondaryMaintenanceState()
        {
            Logger.LogDebug("Starting");
            try
            {
                /* Does not do something special */
                Logger.LogDebug("Ended");
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be finished", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryManualFailoverState()
        {
            Logger.LogDebug("Starting");
            try
            {
                SqlServerInstance.Action_Instance_FailoverForAllMirrorDatabases();
                Logger.LogDebug("Ended");
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryForcedManualFailoverState()
        {
            Logger.LogDebug("Starting");
            try
            {
                SqlServerInstance.Action_Instance_FailoverForAllMirrorDatabases();
                Logger.LogDebug("Ended");
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        private void TimedCheck_SecondaryRunningNoPrimaryState()
        {
            Logger.LogDebug("Starting");
            try
            {
                if (SqlServerInstance.Information_RemoteServer_HasAccess())
                {
                    MakeServerStateChange(ServerStateEnum.SECONDARY_RUNNING_STATE);
                }
                else
                {
                    CheckTimeout(SqlServerInstance.Instance_Configuration.ShutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState, ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
                }
                Logger.LogDebug("Ended.");
            }
            catch (Exception ex)
            {
                Logger.LogError("State could not be started.", ex);
                MakeServerStateChange(ServerStateEnum.SECONDARY_SHUTTING_DOWN_STATE);
            }
        }

        #endregion

        #endregion

        public void MakeServerStateChange(ServerStateEnum newState
            , [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
            , [System.Runtime.CompilerServices.CallerFilePath] string callerSourceFilePath = ""
            , [System.Runtime.CompilerServices.CallerLineNumber] int callerSourceLineNumber = 0
            )
        {
            if (ServerState_Active.ValidTransition(newState))
            {
                ServerState newServerState;
                if (_serverStates.TryGetValue(newState, out newServerState))
                {
                    Logger.LogInfo(string.Format("Server state changed from {0} to {1}.", ServerState_Active, newServerState));
                    ServerState_ActiveCount = 0;
                    ServerState_Old = ServerState_Active;
                    ServerState_Active = newServerState;
                    Logger.LogDebug(string.Format("Server in new state {0}.", newServerState));
                }
                else
                {
                    throw new SqlServerMirroringException(string.Format("Server in state {0} could not get new state {1}.", ServerState_Active, newState), callerMemberName, callerSourceFilePath, callerSourceLineNumber);
                }
            }
            else
            {
                throw new SqlServerMirroringException(string.Format("Server in state {0} does not allow state shange to {1}.", ServerState_Active, newState), callerMemberName, callerSourceFilePath, callerSourceLineNumber);
            }
        }
        private void CheckTimeout(int timeoutNumber, ServerStateEnum serverStateToShiftToOnTimeout
            , [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
            , [System.Runtime.CompilerServices.CallerFilePath] string callerSourceFilePath = ""
            , [System.Runtime.CompilerServices.CallerLineNumber] int callerSourceLineNumber = 0
            )
        {
            if(ServerState_Active.CountStates == CountStates.No)
            {
                throw new SqlServerMirroringException(string.Format("State {0} has CountStates set to No but uses CheckTimeout function assuming a Yes", ServerState_Active.State), callerMemberName, callerSourceFilePath, callerSourceLineNumber);
            }

            if (ServerState_ActiveCount > timeoutNumber)
            {
                Logger.LogError(string.Format("{0} timed out", callerMemberName),callerMemberName, callerSourceFilePath, callerSourceLineNumber);
                MakeServerStateChange(serverStateToShiftToOnTimeout);
            }
        }


        public void StartPrimary()
        {
            MakeServerStateChange(ServerStateEnum.PRIMARY_INITIAL_STATE);
            SqlServerInstance.Action_ServerState_StartTimedCheckTimer();
        }

        public void StartSecondary()
        {
            MakeServerStateChange(ServerStateEnum.SECONDARY_INITIAL_STATE);
            SqlServerInstance.Action_ServerState_StartTimedCheckTimer();
        }
    }
}
