using System;
using System.Text;

namespace MirrorLib
{
    public class DatabaseState
    {
        private string _databaseName;
        private DatabaseStateEnum _databaseStateRecorded;
        private ServerRoleEnum _serverRole;
        private DateTime _lastWriteDate;
        private bool _errorDatabaseState;
        private int _errorCount;

        internal void SetDatabaseName(string databaseName)
        {
            _databaseName = databaseName;
        }

        public string DatabaseName
        {
            get
            {
                return _databaseName;
            }
        }

        internal void SetDatabaseState(DatabaseStateEnum databaseStateRecorded)
        {
            _databaseStateRecorded = databaseStateRecorded;
        }

        public DatabaseStateEnum DatabaseStateRecorded
        {
            get
            {
                return _databaseStateRecorded;
            }
        }

        internal void SetServerRole(ServerRoleEnum serverRole)
        {
            _serverRole = serverRole;
        }

        public ServerRoleEnum ServerRole
        {
            get
            {
                return _serverRole;
            }
        }

        internal void SetLastWriteDate(DateTime lastWriteDate)
        {
            _lastWriteDate = lastWriteDate;
        }

        public DateTime LastWriteDate
        {
            get
            {
                return _lastWriteDate;
            }
        }

        internal void SetErrorDatabaseState(bool errorDatabaseState)
        {
            _errorDatabaseState = errorDatabaseState;
        }

        public bool ErrorDatabaseState
        {
            get
            {
                return _errorDatabaseState;
            }
        }

        internal void SetErrorCount(int errorCount)
        {
            _errorCount = errorCount;
        }

        public int ErrorCount
        {
            get
            {
                return _errorCount;
            }
        }

        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Format("Database Name: {0}|", _databaseName));
            stringBuilder.AppendLine(string.Format("Database State Recorded: {0}|", _databaseStateRecorded));
            stringBuilder.AppendLine(string.Format("Server Role: {0}|", _serverRole));
            stringBuilder.AppendLine(string.Format("Last Write Date: {0}|", _lastWriteDate.ToLongDateString()));
            stringBuilder.AppendLine(string.Format("Error Database State: {0}|", _errorDatabaseState ? "Yes" : "No"));
            stringBuilder.AppendLine(string.Format("Error Count: {0}|", _errorCount.ToString()));
            return stringBuilder.ToString();
        }
    }
}