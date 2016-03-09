using System;
using System.Runtime.Serialization;

namespace SqlServerMirroring
{
    [Serializable]
    internal class SqlServerMirroringException : Exception
    {
        public SqlServerMirroringException()
        {
        }

        public SqlServerMirroringException(string message) : base(message)
        {
        }

        public SqlServerMirroringException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected SqlServerMirroringException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}