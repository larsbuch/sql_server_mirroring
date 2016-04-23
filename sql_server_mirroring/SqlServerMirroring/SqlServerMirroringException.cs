using System;
using System.Runtime.Serialization;

namespace MirrorLib
{
    [Serializable]
    internal class SqlServerMirroringException : Exception
    {
        public SqlServerMirroringException(string message
            , [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
            , [System.Runtime.CompilerServices.CallerFilePath] string callerSourceFilePath = ""
            , [System.Runtime.CompilerServices.CallerLineNumber] int callerSourceLineNumber = 0
            ) : base(string.Format("{0} ({2}): {3}", callerMemberName, callerSourceFilePath, callerSourceLineNumber, message))
        {
        }

        public SqlServerMirroringException(string message, Exception innerException
            , [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
            , [System.Runtime.CompilerServices.CallerFilePath] string callerSourceFilePath = ""
            , [System.Runtime.CompilerServices.CallerLineNumber] int callerSourceLineNumber = 0
            ) : base(string.Format("{0} ({2}): {3}", callerMemberName, callerSourceFilePath, callerSourceLineNumber, message), innerException)
        {
        }

        protected SqlServerMirroringException(SerializationInfo info, StreamingContext context
            ) : base(info, context)
        {
        }
    }
}