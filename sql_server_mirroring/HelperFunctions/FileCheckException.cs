using System;
using System.Runtime.Serialization;

namespace HelperFunctions
{
    [Serializable]
    internal class FileCheckException : Exception
    {
        public FileCheckException()
        {
        }

        public FileCheckException(string message) : base(message)
        {
        }

        public FileCheckException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected FileCheckException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}