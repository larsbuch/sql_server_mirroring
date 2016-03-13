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

            private void ValidateDatabaseName(string databaseName)
            {
                Regex regex = new Regex(@"^[\w_]+$");
                if (!regex.IsMatch(databaseName))
                {
                    throw new DirectoryException(string.Format("The database name {0} is not valid.", databaseName));
                }
            }

            public override string ToString()
            {
                return _databaseName;
            }
    }
}
