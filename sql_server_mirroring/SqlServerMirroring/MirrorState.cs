using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using HelperFunctions;

namespace SqlServerMirroring
{
    public class MirrorState
    {
        private DatabaseName _databaseName;
        private bool _isConfiguredForMirroring;
        private DatabaseStatus _databaseStatus;

        public MirrorState(Database database)
        {
            // TODO interrogate database for mirroring
            _databaseName = new DatabaseName(database.Name);
            _isConfiguredForMirroring = (database.MirroringStatus != MirroringStatus.None);
            _databaseStatus = database.Status;
        }

        public DatabaseName DatabaseName
        {
            get
            {
                return _databaseName;
            }
        }

        public bool IsConfiguredForMirroring
        {
            get
            {
                return _isConfiguredForMirroring;
            }
        }

        public DatabaseStatus Status
        {
            get
            {
                return _databaseStatus;
            }
        }

        public bool IsPrincipal
        {
            get
            {
                if(IsConfiguredForMirroring)
                {
                    return Status == DatabaseStatus.Normal? true :false;
                }
                else
                {
                    return false;
                }
            }
        }

        public bool IsMirror
        {
            get
            {
                if(IsConfiguredForMirroring)
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
