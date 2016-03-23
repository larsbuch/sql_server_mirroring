using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MirrorLib
{
    public class DatabaseMirrorState
    {
        private MirroringSafetyLevel _mirroringSafetyLevel;
        private MirroringRole _mirroringRole;
        private MirroringState _mirroringState;
        private string _mirroringInstancePartner;
        private string _databaseName;
        private int _databaseId;
        private Guid _mirroringGuid;
        private byte _compatibilityLevel;
        private DatabaseState _databaseState;
        private DatabaseRecoveryModel _databaseRecoveryModel;
        private DatabaseUserAccess _databaseUserAccess;
        private bool _databaseIsInStandby;

        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Format("Database Name: {0} | ", _databaseName));
            stringBuilder.AppendLine(string.Format("Database Id: {0} | ", _databaseId));
            stringBuilder.AppendLine(string.Format("Database State: {0} | ", _databaseState));
            stringBuilder.AppendLine(string.Format("Database Compatability Level: {0} | ", _compatibilityLevel));
            stringBuilder.AppendLine(string.Format("Database Recovery Model: {0} | ", _databaseRecoveryModel));
            stringBuilder.AppendLine(string.Format("Database User Access: {0} | ", _databaseUserAccess));
            stringBuilder.AppendLine(string.Format("Database is in Standby: {0} | ", _databaseIsInStandby?"Yes":"No"));
            stringBuilder.AppendLine(string.Format("Mirror State: {0} | ", _mirroringState));
            stringBuilder.AppendLine(string.Format("Mirror Role: {0} | ", _mirroringRole));
            stringBuilder.AppendLine(string.Format("Mirroring Instance Partner: {0} | ", _mirroringInstancePartner));
            stringBuilder.AppendLine(string.Format("Mirroring Safety Level: {0} | ", _mirroringSafetyLevel));
            stringBuilder.AppendLine(string.Format("Mirroring Guid: {0}", _mirroringGuid.ToString()));
            return stringBuilder.ToString();
        }

        internal void SetMirroringGuid(Guid mirroringGuid)
        {
            _mirroringGuid = mirroringGuid;
        }

        internal void SetDatabaseId(int? databaseId)
        {
            if(databaseId.HasValue)
            {
                _databaseId = databaseId.Value;
            }
            else
            {
                _databaseId = -1;
            }
        }

        internal void SetDatabaseName(string databaseName)
        {
            _databaseName = databaseName;
        }

        internal void SetMirroringParnerInstance(string mirroringInstancePartner)
        {
            _mirroringInstancePartner = mirroringInstancePartner;
        }

        internal void SetMirroringSafetyLevel(byte? mirroringSafetyLevel)
        {
            if(mirroringSafetyLevel.HasValue)
            {
                _mirroringSafetyLevel = (MirroringSafetyLevel)mirroringSafetyLevel;
            }
            else
            {
                _mirroringSafetyLevel = MirroringSafetyLevel.NotMirrored;
            }
        }

        internal void SetMirroringRole(byte? mirroringRole)
        {
            if (mirroringRole.HasValue)
            {
                _mirroringRole = (MirroringRole)mirroringRole;
            }
            else
            {
                _mirroringRole = MirroringRole.NotMirrored;
            }
        }

        internal void SetMirroringState(byte? mirroringState)
        {
            if (mirroringState.HasValue)
            {
                _mirroringState = (MirroringState)mirroringState;
            }
            else
            {
                _mirroringState = MirroringState.NotMirrored;
            }
        }

        public MirroringSafetyLevel MirroringSafetyLevel
        {
            get
            {
                return _mirroringSafetyLevel;
            }
        }

        public MirroringRole MirroringRole
        {
            get
            {
                return _mirroringRole;
            }
        }

        public MirroringState MirroringState
        {
            get
            {
                return _mirroringState;
            }
        }

        public string MirroringInstancePartner
        {
            get
            {
                return _mirroringInstancePartner;
            }
        }

        public string DatabaseName
        {
            get
            {
                return _databaseName;
            }
        }

        public int DatabaseId
        {
            get
            {
                return _databaseId;
            }
        }

        public Guid MirroringGuid
        {
            get
            {
                return _mirroringGuid;
            }
        }

        internal void SetCompatibilityLevel(byte? compatibilityLevel)
        {
            if (compatibilityLevel.HasValue)
            {
                _compatibilityLevel = compatibilityLevel.Value;
            }
            else
            {
                _compatibilityLevel = 0;
            }
        }

        public byte CompatibilityLevel
        {
            get
            {
                return _compatibilityLevel;
            }
        }

        internal void SetDatabaseState(byte? databaseState)
        {
            if(databaseState.HasValue)
            {
                _databaseState = (DatabaseState)databaseState;
            }
            else
            {
                _databaseState = DatabaseState.UNKNOWN;
            }
        }

        public DatabaseState DatabaseState
        {
            get
            {
                return _databaseState;
            }
        }

        internal void SetDatabaseRecoveryModel(byte? databaseRecoveryModel)
        {
            if(databaseRecoveryModel.HasValue)
            {
                _databaseRecoveryModel = (DatabaseRecoveryModel)databaseRecoveryModel;
            }
            else
            {
                _databaseRecoveryModel = DatabaseRecoveryModel.UNKNOWN;
            }
        }

        public DatabaseRecoveryModel DatabaseRecoveryModel
        {
            get
            {
                return _databaseRecoveryModel;
            }
        }

        internal void SetDatabaseUserAccess(byte? databaseUserAccess)
        {
            if(databaseUserAccess.HasValue)
            {
                _databaseUserAccess = (DatabaseUserAccess) databaseUserAccess.Value;
            }
            else
            {
                _databaseUserAccess = DatabaseUserAccess.UNKNOWN;
            }
        }

        public DatabaseUserAccess DatabaseUserAccess
        {
            get
            {
                return _databaseUserAccess;
            }
        }

        internal void SetDatabaseIsInStandby(bool? databaseIsInStandby)
        {
            if(databaseIsInStandby.HasValue)
            {
                if(databaseIsInStandby.Value)
                {
                    _databaseIsInStandby = true;
                }
            }
            _databaseIsInStandby = false;
        }

        public bool DatabaseIsInStandby
        {
            get
            {
                return _databaseIsInStandby;
            }
        }
    }

    public enum DatabaseUserAccess
    {
        UNKNOWN = -1,
        MULTI_USER = 0,
        SINGLE_USER = 1,
        RESTICTED_USER = 2
    }

    public enum DatabaseRecoveryModel
    {
        UNKNOWN = 0,
        FULL = 1,
        BULK_LOGGED = 2,
        SIMPLE = 3
    }

    public enum DatabaseState
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

    public enum MirroringSafetyLevel
    {
        NotMirrored = -1,
        UnknownState = 0,
        Off_Asynchonous = 1,
        Full_Synchronous = 2
    }

    public enum MirroringRole
    {
        NotMirrored = 0,
        Principal = 1,
        Mirror = 2
    }

    public enum MirroringState
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
}
