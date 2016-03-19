using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace HelperFunctions
{
    public class DatabaseName
    {
        private string _databaseName;

        public DatabaseName(string databaseName)
        {
            ValidateDatabaseName(databaseName);
            _databaseName = databaseName;
        }

        public string Endpoint_Name
        {
            get
            {
                return "Mirroring_Endpoint_" + _databaseName;
            }
        }

        private void ValidateDatabaseName(string databaseName)
        {
            Regex regex = new Regex(@"^[\w_][\w_\d]{0,127}$");
            if (!regex.IsMatch(databaseName))
            {
                throw new DirectoryException(string.Format("The database name {0} is not valid.", databaseName));
            }
        }

        public string GenerateBackupFileName()
        {
            return _databaseName + "_" + DateTime.Now.ToFileTime() + ".bak";
        }

        public override string ToString()
        {
            return _databaseName;
        }
    }
}
