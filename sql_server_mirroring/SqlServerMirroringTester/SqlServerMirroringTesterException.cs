using System;
using System.Runtime.Serialization;

namespace MirrorLibTester
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