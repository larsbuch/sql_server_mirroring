using System;
using System.Runtime.Serialization;

namespace HelperFunctions
{
    [Serializable]
    internal class ShareException : Exception
    {
        public ShareException()
        {
        }

        public ShareException(string message) : base(message)
        {
        }

        public ShareException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected ShareException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}