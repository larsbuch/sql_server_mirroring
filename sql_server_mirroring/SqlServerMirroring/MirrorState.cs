using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;

namespace SqlServerMirroring
{
    public class MirrorState
    {
        private Database database;

        public MirrorState(Database database)
        {
            this.database = database;
        }

        public string DatabaseName { get; internal set; }
        public bool IsConfiguredForMirroring { get; internal set; }
        public bool IsGoodMirrorState { get; internal set; }
    }
}
