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
        OFFLINE_SECONDARY = 10,
        READY_FOR_MIRRORING = 200,
        READY_FOR_RESTORE = 201,
        BACKUP_DELIVERED = 202,
        BACKUP_REPORTED_DELIVERED = 203
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
        INITIAL_STATE,
        PRIMARY_STARTUP_STATE,
        PRIMARY_RUNNING_STATE,
        PRIMARY_FORCED_RUNNING_STATE,
        PRIMARY_SHUTTING_DOWN_STATE,
        PRIMARY_SHUTDOWN_STATE,
        PRIMARY_MAINTENANCE_STATE,
        PRIMARY_MANUAL_FAILOVER_STATE,
        PRIMARY_RUNNING_NO_SECONDARY_STATE,
        SECONDARY_STARTUP_STATE,
        SECONDARY_RUNNING_STATE,
        SECONDARY_SHUTTING_DOWN_STATE,
        SECONDARY_SHUTDOWN_STATE,
        SECONDARY_MAINTENANCE_STATE,
        SECONDARY_MANUAL_FAILOVER_STATE,
        SECONDARY_FORCED_MANUAL_FAILOVER_STATE,
        SECONDARY_RUNNING_NO_PRIMARY_STATE
    }
}
