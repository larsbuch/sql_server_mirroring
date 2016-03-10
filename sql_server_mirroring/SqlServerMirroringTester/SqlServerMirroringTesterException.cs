using System;
using System.Runtime.Serialization;

namespace SqlServerMirroringTester
{
    [Serializable]
    internal class SqlServerMirroringTesterException : Exception
    {
        public SqlServerMirroringTesterException()
        {
        }

        public SqlServerMirroringTesterException(string message) : base(message)
        {
        }

        public SqlServerMirroringTesterException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected SqlServerMirroringTesterException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}