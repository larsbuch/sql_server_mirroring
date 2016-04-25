using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MirrorLib
{
    public enum ServerRoleEnum
    {
        NotSet,
        Primary,
        Secondary,
        MainlyPrimary,
        MainlySecondary,
        Neither
    }

    public enum DatabaseUserAccessEnum
    {
        UNKNOWN = -1,
        MULTI_USER = 0,
        SINGLE_USER = 1,
        RESTICTED_USER = 2
    }

    public enum DatabaseRecoveryModelEnum
    {
        UNKNOWN = 0,
        FULL = 1,
        BULK_LOGGED = 2,
        SIMPLE = 3
    }

    public enum DatabaseStateEnum
    {
        UNKNOWN = -1,
        ONLINE = 0,
        RESTORING = 1,
        RECOVERING = 2,
        RECOVERY_PENDING = 3,
        SUSPECT = 4,
        EMERGENCY = 5,
        OFFLINE = 6,
        COPYING = 7,
        OFFLINE_SECONDARY = 10
    }

    public enum MirroringSafetyLevelEnum
    {
        NotMirrored = -1,
        UnknownState = 0,
        Off_Asynchonous = 1,
        Full_Synchronous = 2
    }

    public enum MirroringRoleEnum
    {
        NotMirrored = 0,
        Principal = 1,
        Mirror = 2
    }

    public enum MirroringStateEnum
    {
        NotMirrored = -1,
        Suspended = 0,
        DisconnectedFromOtherPartner = 1,
        Synchronizing = 2,
        PendingFailover = 3,
        Synchonized = 4,
        PartnerNotSynchronized = 5,
        PartnerSynchronized = 6
    }

    public enum ServerStateEnum
    {
        NOT_SET,
        PRIMARY_INITIAL_STATE,
        PRIMARY_CONFIGURATION_STATE,
        PRIMARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE,
        PRIMARY_CONFIGURATION_BACKUP_STATE,
        PRIMARY_CONFIGURATION_WAITING_FOR_SECONDARY_RESTORE_STATE,
        PRIMARY_CONFIGURATION_STARTING_MIRRORING_STATE,
        PRIMARY_STARTUP_STATE,
        PRIMARY_RUNNING_STATE,
        PRIMARY_FORCED_RUNNING_STATE,
        PRIMARY_SHUTTING_DOWN_STATE,
        PRIMARY_SHUTDOWN_STATE,
        PRIMARY_MAINTENANCE_STATE,
        PRIMARY_MANUAL_FAILOVER_STATE,
        PRIMARY_RUNNING_NO_SECONDARY_STATE,
        SECONDARY_INITIAL_STATE,
        SECONDARY_CONFIGURATION_STATE,
        SECONDARY_CONFIGURATION_CREATE_DATABASE_FOLDERS_STATE,
        SECONDARY_CONFIGURATION_WAITING_FOR_PRIMARY_BACKUP_FINISH_STATE,
        SECONDARY_CONFIGURATION_LOOKING_FOR_BACKUP_STATE,
        SECONDARY_CONFIGURATION_RESTORING_DATABASES_STATE,
        SECONDARY_CONFIGURATION_WAITING_FOR_MIRRORING_STATE,
        SECONDARY_STARTUP_STATE,
        SECONDARY_RUNNING_STATE,
        SECONDARY_SHUTTING_DOWN_STATE,
        SECONDARY_SHUTDOWN_STATE,
        SECONDARY_MAINTENANCE_STATE,
        SECONDARY_MANUAL_FAILOVER_STATE,
        SECONDARY_FORCED_MANUAL_FAILOVER_STATE,
        SECONDARY_RUNNING_NO_PRIMARY_STATE
    }

    public enum ServerPlacement
    {
        Local,
        Remote
    }

    public enum CountStates
    {
        Yes,
        No
    }

    public enum MirrorState
    {
        Mirrored,
        Degraded
    }
}
