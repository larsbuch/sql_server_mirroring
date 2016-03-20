using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using HelperFunctions;

namespace MirrorLib
{
    public class MirrorDatabase
    {
        private DatabaseName _databaseName;
        private bool _isMirroringEnabled;
        private DatabaseStatus _databaseStatus;
        private int _databaseID;
        private Guid _mirroringGuid;
        private MirroringStatus _mirroringStatus;
        private RecoveryModel _recoveryModel;
        private bool _isUpdateable;
        private string _mirrorRole;

        public MirrorDatabase(Database database, string mirrorRoleDesc)
        {
            // TODO interrogate database for mirroring
            _databaseName = new DatabaseName(database.Name);
            _mirrorRole = mirrorRoleDesc;
            _isMirroringEnabled = database.IsMirroringEnabled;
            _databaseStatus = database.Status;
            _databaseID = database.ID;
            _mirroringGuid = database.MirroringID;
            _mirroringStatus = database.MirroringStatus;
            _recoveryModel = database.RecoveryModel;
            _isUpdateable = database.IsUpdateable;
        }

        public DatabaseName DatabaseName
        {
            get
            {
                return _databaseName;
            }
        }

        public int DatabaseID
        {
            get
            {
                return _databaseID;
            }
        }

        public string DatabaseMirrorRole
        {
            get
            {
                return _mirrorRole;
            }
        }

        public bool IsPrincipal
        {
            get
            {
                return _mirrorRole == "Principal" ? true : false;
            }
        }

        public bool IsUpdateable
        {
            get
            {
                return _isUpdateable;
            }
        }

        public RecoveryModel RecoveryModel
        {
            get
            {
                return _recoveryModel;
            }
        }

        public MirroringStatus DatabaseMirroringStatus
        {
            get
            {
                return _mirroringStatus;
            }
        }

        public Guid DatabaseMirroringGuid
        {
            get
            {
                return _mirroringGuid;
            }
        }

        public bool IsMirroringEnabled
        {
            get
            {
                return _isMirroringEnabled;
            }
        }

        public DatabaseStatus Status
        {
            get
            {
                return _databaseStatus;
            }
        }

        public bool IsMirror
        {
            get
            {
                if(IsMirroringEnabled)
                {
                    return Status == DatabaseStatus.Restoring ? true : false;
                }
                else
                {
                    return false;
                }
            }
        }
    }
}
