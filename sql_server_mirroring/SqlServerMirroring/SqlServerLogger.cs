using HelperFunctions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLib
{
    public class SqlServerLogger
    {
        public ILogger _logger;
        public SqlServerLogger(ILogger logger)
        {
            _logger = logger;
        }

        public ILogger InternalLogger
        {
            get
            {
                return _logger;
            }
        }

        public void LogDebug(string message
            , [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
            , [System.Runtime.CompilerServices.CallerFilePath] string callerSourceFilePath = ""
            , [System.Runtime.CompilerServices.CallerLineNumber] int callerSourceLineNumber = 0
            )
        {
            _logger.LogDebug(string.Format("{0} ({1}: {2}): {3}", callerMemberName, callerSourceFilePath, callerSourceLineNumber, message));
        }

        public void LogInfo(string message
            , [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
            , [System.Runtime.CompilerServices.CallerFilePath] string callerSourceFilePath = ""
            , [System.Runtime.CompilerServices.CallerLineNumber] int callerSourceLineNumber = 0
            )
        {
            _logger.LogInfo(string.Format("{0} ({1}: {2}): {3}", callerMemberName, callerSourceFilePath, callerSourceLineNumber, message));
        }

        public void LogWarning(string message
            , [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
            , [System.Runtime.CompilerServices.CallerFilePath] string callerSourceFilePath = ""
            , [System.Runtime.CompilerServices.CallerLineNumber] int callerSourceLineNumber = 0
            )
        {
            _logger.LogWarning(string.Format("{0} ({1}: {2}): {3}", callerMemberName, callerSourceFilePath, callerSourceLineNumber, message));
        }

        public void LogError(string message
            , [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
            , [System.Runtime.CompilerServices.CallerFilePath] string callerSourceFilePath = ""
            , [System.Runtime.CompilerServices.CallerLineNumber] int callerSourceLineNumber = 0
            )
        {
            _logger.LogError(string.Format("{0} ({1}: {2}): {3}", callerMemberName, callerSourceFilePath, callerSourceLineNumber, message));
        }

        public void LogError(string message, Exception exception
            , [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
            , [System.Runtime.CompilerServices.CallerFilePath] string callerSourceFilePath = ""
            , [System.Runtime.CompilerServices.CallerLineNumber] int callerSourceLineNumber = 0
            )
        {
            _logger.LogError(string.Format("{0} ({1}: {2}): {3}", callerMemberName, callerSourceFilePath, callerSourceLineNumber, message), exception);
        }

    }
}
