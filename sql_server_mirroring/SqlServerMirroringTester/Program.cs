using ConsoleTester;
using HelperFunctions;
using SqlServerMirroring;
using System;
using System.Collections.Generic;
using System.IO;

namespace SqlServerMirroringTester
{
    class Program
    {

        static void Main(string[] args)
        {
            Configuration.Add(CONNECTION_STRING, "Server=localhost;Trusted_Connection=True;");

            ConsoleTest.AddTest("SQL Server Instance", "Try to connect to server with SMO", () => Test_ConnectToSMO());
            ConsoleTest.AddTest("SQL Server Instance", "Get instance information", () => Test_InstanceInformation());
            ConsoleTest.AddTest("SQL Server Instance", "Check for instance readyness for mirroring", () => Test_CheckInstanceForMirroring());
            ConsoleTest.AddTest("SQL Server Instance", "Setup for instance for mirroring", () => Test_SetupInstanceForMirroring());
            ConsoleTest.AddTest("SQL Server Instance", "Run startup mirror check on principal", () => Test_StartUpMirrorCheck_Principal());
            ConsoleTest.AddTest("SQL Server Instance", "Run startup mirror check on mirror", () => Test_StartUpMirrorCheck_Mirror());
            ConsoleTest.AddTest("SQL Server Agent", "Get sql agent information", () => Test_SqlServerAgentInformation());

            ConsoleTest.Run();
        }

        #region Tests

        private static void Test_ConnectToSMO()
        {
            Console.WriteLine(string.Format("Instance Status: {0}", SqlServer.Instance_Status()));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_InstanceInformation()
        {
            Console.WriteLine("Instance Information:");
            foreach (KeyValuePair<string, string> pair in SqlServer.Instance_Information())
            {
                Console.WriteLine(string.Format("{0}: {1}", pair.Key, pair.Value));
            }

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_CheckInstanceForMirroring()
        {
            Console.WriteLine(string.Format("Instance Ready for mirroring: {0}", SqlServer.CheckInstanceForMirroring()?"Yes":"No"));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_SetupInstanceForMirroring()
        {
            Console.WriteLine("Start setup instance for mirroring");

            SqlServer.SetupInstanceForMirroring();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_SqlServerAgentInformation()
        {
            Console.WriteLine("Sql Server Agent Information:");
            foreach (KeyValuePair<string, string> pair in SqlServer.SqlAgent_Information())
            {
                Console.WriteLine(string.Format("{0}: {1}", pair.Key, pair.Value));
            }

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_StartUpMirrorCheck_Principal()
        {
            //Build data
            Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredMirrorDatabases = new Dictionary<DatabaseName, ConfiguredDatabaseForMirroring>();

            SqlServer.StartUpMirrorCheck(configuredMirrorDatabases, true);

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_StartUpMirrorCheck_Mirror()
        {
            //Build data
            Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredMirrorDatabases = new Dictionary<DatabaseName, ConfiguredDatabaseForMirroring>();

            SqlServer.StartUpMirrorCheck(configuredMirrorDatabases, false);

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_ResumeMirroringForAllDatabases()
        {
            //Build data
            Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredMirrorDatabases = new Dictionary<DatabaseName, ConfiguredDatabaseForMirroring>();

            SqlServer.ResumeMirroringForAllDatabases(configuredMirrorDatabases);

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_ForceFailoverWithDataLossForAllMirrorDatabases()
        {
            //Build data
            Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredMirrorDatabases = new Dictionary<DatabaseName, ConfiguredDatabaseForMirroring>();

            SqlServer.ForceFailoverWithDataLossForAllMirrorDatabases(configuredMirrorDatabases);

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_FailoverForAllMirrorDatabases()
        {
            //Build data
            Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredMirrorDatabases = new Dictionary<DatabaseName, ConfiguredDatabaseForMirroring>();

            SqlServer.FailoverForAllMirrorDatabases(configuredMirrorDatabases);

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        #endregion

        #region Helper functions and constructs

        private const string CONNECTION_STRING = "CONNECTION_STRING";
        private static SqlServerInstance _sqlServerInstance;
        private static Dictionary<string, string> _configuration;

        private static Dictionary<string, string> Configuration
        {
            get
            {
                if(_configuration == null)
                {
                    _configuration = new Dictionary<string, string>();
                }
                return _configuration;
            }
        }

        private static SqlServerInstance SqlServer
        {
            get
            {
                if(_sqlServerInstance == null)
                {
                    string connectionString;
                    if(Configuration.TryGetValue(CONNECTION_STRING, out connectionString))
                    {
                        _sqlServerInstance = new SqlServerInstance(Logger, connectionString);
                    }
                    else
                    {
                        throw new SqlServerMirroringTesterException(string.Format("Configuration value {0} is missing in Configuration.", CONNECTION_STRING));
                    }
                }
                return _sqlServerInstance;
            }
        }

        private static bool CompareByteArray(byte[] byteArray1, byte[] byteArray2)
        {
            if (byteArray1 != null && byteArray2 != null && byteArray1.Length == byteArray2.Length)
            {
                for (int counter = 0; counter < byteArray1.Length; counter += 1)
                {
                    if (byteArray1[counter] != byteArray2[counter])
                    {
                        return false;
                    }
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        private static bool CompareStringArray(string[] stringArray1, string[] stringArray2)
        {
            if (stringArray1 != null && stringArray2 != null && stringArray1.Length == stringArray2.Length)
            {
                for (int counter = 0; counter < stringArray1.Length; counter += 1)
                {
                    if (stringArray1[counter] != stringArray2[counter])
                    {
                        return false;
                    }
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        private static string DirectoryCreateAbsolutePath(string subDirectory)
        {
            return Directory.GetCurrentDirectory() + "\\" + subDirectory;
        }

        static ILogger _logger = new ConsoleLogger();

        private static ILogger Logger
        {
            get
            {
                return _logger;
            }
        }

        public class ConsoleLogger : ILogger
        {
            public void LogDebug(string message)
            {
                Console.WriteLine("LogDebug: " + message);
            }

            public void LogInfo(string message)
            {
                Console.WriteLine("LogInfo: " + message);
            }

            public void LogWarning(string message)
            {
                Console.WriteLine("LogWarning: " + message);
            }
            public void LogError(string message)
            {
                Console.WriteLine("LogError: " + message);
            }
            public void LogError(string message, Exception exception)
            {
                Console.WriteLine("LogError: " + message);
                Console.WriteLine("Exception Message: " + exception.Message);
                Console.WriteLine("Exception StackTrace: " + exception.StackTrace);
            }
        }
        #endregion
    }
}
