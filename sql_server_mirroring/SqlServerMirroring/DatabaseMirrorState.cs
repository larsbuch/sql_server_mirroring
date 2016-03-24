using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MirrorLib
{
    public class DatabaseMirrorState
    {
        private MirroringSafetyLevelEnum _mirroringSafetyLevel;
        private MirroringRoleEnum _mirroringRole;
        private MirroringStateEnum _mirroringState;
        private string _mirroringInstancePartner;
        private string _databaseName;
        private int _databaseId;
        private Guid _mirroringGuid;
        private byte _compatibilityLevel;
        private DatabaseStateEnum _databaseState;
        private DatabaseRecoveryModelEnum _databaseRecoveryModel;
        private DatabaseUserAccessEnum _databaseUserAccess;
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
                _mirroringSafetyLevel = (MirroringSafetyLevelEnum)mirroringSafetyLevel;
            }
            else
            {
                _mirroringSafetyLevel = MirroringSafetyLevelEnum.NotMirrored;
            }
        }

        internal void SetMirroringRole(byte? mirroringRole)
        {
            if (mirroringRole.HasValue)
            {
                _mirroringRole = (MirroringRoleEnum)mirroringRole;
            }
            else
            {
                _mirroringRole = MirroringRoleEnum.NotMirrored;
            }
        }

        internal void SetMirroringState(byte? mirroringState)
        {
            if (mirroringState.HasValue)
            {
                _mirroringState = (MirroringStateEnum)mirroringState;
            }
            else
            {
                _mirroringState = MirroringStateEnum.NotMirrored;
            }
        }

        public MirroringSafetyLevelEnum MirroringSafetyLevel
        {
            get
            {
                return _mirroringSafetyLevel;
            }
        }

        public MirroringRoleEnum MirroringRole
        {
            get
            {
                return _mirroringRole;
            }
        }

        public MirroringStateEnum MirroringState
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
                _databaseState = (DatabaseStateEnum)databaseState;
            }
            else
            {
                _databaseState = DatabaseStateEnum.UNKNOWN;
            }
        }

        public DatabaseStateEnum DatabaseState
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
                _databaseRecoveryModel = (DatabaseRecoveryModelEnum)databaseRecoveryModel;
            }
            else
            {
                _databaseRecoveryModel = DatabaseRecoveryModelEnum.UNKNOWN;
            }
        }

        public DatabaseRecoveryModelEnum DatabaseRecoveryModel
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
                _databaseUserAccess = (DatabaseUserAccessEnum) databaseUserAccess.Value;
            }
            else
            {
                _databaseUserAccess = DatabaseUserAccessEnum.UNKNOWN;
            }
        }

        public DatabaseUserAccessEnum DatabaseUserAccess
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

}
