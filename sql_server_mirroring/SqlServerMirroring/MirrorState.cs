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

        public MirrorState(Database database)
        {
            // TODO interrogate database for mirroring
            _databaseName = new DatabaseName(database.Name);
            _isConfiguredForMirroring = (database.MirroringStatus != MirroringStatus.None);
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
    }
}
