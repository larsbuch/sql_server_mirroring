using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HelperFunctions
{
    public interface ILogger
    {
        void LogDebug(string message);
        void LogInfo(string message);
        void LogWarning(string message);
    }
}
