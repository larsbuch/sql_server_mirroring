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

            ConsoleTest.AddTest("Information", "Try to connect to server with SMO", () => Test_Information_InstanceStatus());
            ConsoleTest.AddTest("Information", "Get instance information", () => Test_Information_Instance());
            ConsoleTest.AddTest("Information", "Get sql agent information", () => Test_Information_SqlAgent());
            ConsoleTest.AddTest("Information", "Check Windows Authentification", () => Test_Information_WindowsAuthentificationActive());
            ConsoleTest.AddTest("Information", "Check Sql Server Authentification", () => Test_Information_SqlServerAuthentificationActive());
            ConsoleTest.AddTest("Information", "Check for instance readyness for mirroring", () => Test_Information_CheckInstanceForMirroring());
            ConsoleTest.AddTest("Action", "Run startup mirror check on principal", () => Test_Action_StartPrimary());
            ConsoleTest.AddTest("Action", "Run startup mirror check on mirror", () => Test_Action_StartSecondary());
            ConsoleTest.AddTest("Action", "Setup Monitoring", () => Test_Action_SetupMonitoring());
            ConsoleTest.AddTest("Action", "Run CheckServerState", () => Test_Action_CheckServerState());
            ConsoleTest.AddTest("Action", "Resume Mirroring For All Mirror Databases", () => Test_Action_ResumeMirroringForAllDatabases());
            ConsoleTest.AddTest("Action", "Suspend Mirroring For All Mirror Databases", () => Test_Action_SuspendMirroringForAllMirrorDatabases());
            ConsoleTest.AddTest("Action", "Force Failover With Data Loss For All Mirror Databases", () => Test_Action_ForceFailoverWithDataLossForAllMirrorDatabases());
            ConsoleTest.AddTest("Action", "Failover For All Mirror Databases", () => Test_Action_FailoverForAllMirrorDatabases());
            ConsoleTest.AddTest("Action", "Backup For All Mirror Databases", () => Test_Action_BackupForAllMirrorDatabases());
            ConsoleTest.AddTest("Action", "Shut Down Mirroring Service", () => Test_Action_ShutDownMirroringService());
            ConsoleTest.AddTest("Action", "Force Shut Down Mirroring Service", () => Test_Action_ForceShutDownMirroringService());

            ConsoleTest.Run();
        }

        #region Information Tests

        private static void Test_Information_InstanceStatus()
        {
            Console.WriteLine(string.Format("Instance Status: {0}", SqlServer.Information_InstanceStatus()));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Information_Instance()
        {
            Console.WriteLine("Instance Information:");
            foreach (KeyValuePair<string, string> pair in SqlServer.Information_Instance())
            {
                Console.WriteLine(string.Format("{0}: {1}", pair.Key, pair.Value));
            }

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Information_SqlAgent()
        {
            Console.WriteLine("Sql Server Agent Information:");
            foreach (KeyValuePair<string, string> pair in SqlServer.Information_SqlAgent())
            {
                Console.WriteLine(string.Format("{0}: {1}", pair.Key, pair.Value));
            }

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Information_WindowsAuthentificationActive()
        {
            Console.WriteLine(string.Format("Instance Windows Authentification active: {0}", SqlServer.Information_WindowsAuthentificationActive() ? "Yes" : "No"));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Information_SqlServerAuthentificationActive()
        {
            Console.WriteLine(string.Format("Instance Sql server Authentification active: {0}", SqlServer.Information_SqlServerAuthentificationActive() ? "Yes" : "No"));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Information_CheckInstanceForMirroring()
        {
            Console.WriteLine(string.Format("Instance Ready for mirroring: {0}", SqlServer.Information_CheckInstanceForMirroring() ? "Yes" : "No"));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        #endregion

        #region Action Tests

        private static void Test_Action_StartPrimary()
        {

            SqlServer.Action_StartPrimary();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_StartSecondary()
        {

            SqlServer.Action_StartSecondary();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_SetupMonitoring()
        {
            SqlServer.Action_SetupMonitoring();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_CheckServerState()
        {
            SqlServer.Action_CheckServerState();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_ResumeMirroringForAllDatabases()
        {
            SqlServer.Action_ResumeMirroringForAllDatabases();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_SuspendMirroringForAllMirrorDatabases()
        {
            SqlServer.Action_SuspendMirroringForAllMirrorDatabases();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_ForceFailoverWithDataLossForAllMirrorDatabases()
        {
            SqlServer.Action_ForceFailoverWithDataLossForAllMirrorDatabases();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_FailoverForAllMirrorDatabases()
        {
            SqlServer.Action_FailoverForAllMirrorDatabases();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_BackupForAllMirrorDatabases()
        {
            SqlServer.Action_BackupForAllMirrorDatabases();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_ShutDownMirroringService()
        {
            SqlServer.Action_ShutDownMirroringService();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_ForceShutDownMirroringService()
        {
            SqlServer.Action_ForceShutDownMirroringService();

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
                        _sqlServerInstance.ConfiguredMirrorDatabases = BuildConfigurationData();
                    }
                    else
                    {
                        throw new SqlServerMirroringTesterException(string.Format("Configuration value {0} is missing in Configuration.", CONNECTION_STRING));
                    }
                }
                return _sqlServerInstance;
            }
        }

        private static Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> BuildConfigurationData()
        {
            //Build data
            Dictionary<DatabaseName, ConfiguredDatabaseForMirroring> configuredMirrorDatabases = new Dictionary<DatabaseName, ConfiguredDatabaseForMirroring>();
            ConfiguredDatabaseForMirroring configured1 = new ConfiguredDatabaseForMirroring(
                new DatabaseName("TestMirror1"),
                new DirectoryPath("E:\\Test\\LocalBackup"),
                new DirectoryPath("E:\\Test\\Share"),
                new DirectoryPath("E:\\Test\\LocalRestore"),
                new ShareName("LocalShare"),
                new RemoteServer("RemoteServer"),
                new ShareName("RemoteShare"),
                new SubDirectory("LocalTransfer"),
                new SubDirectory("RemoteTransfer"),
                new SubDirectory("RemoteDelivery"),
                5022,
                7022,
                7,
                60,
                1,
                60,
                60
                );
            configuredMirrorDatabases.Add(configured1.DatabaseName, configured1);
            ConfiguredDatabaseForMirroring configured2 = new ConfiguredDatabaseForMirroring(
                new DatabaseName("TestMirror2"),
                new DirectoryPath("E:\\Test\\LocalBackup"),
                new DirectoryPath("E:\\Test\\Share"),
                new DirectoryPath("E:\\Test\\LocalRestore"),
                new ShareName("LocalShare"),
                new RemoteServer("RemoteServer"),
                new ShareName("RemoteShare"),
                new SubDirectory("LocalTransfer"),
                new SubDirectory("RemoteTransfer"),
                new SubDirectory("RemoteDelivery"),
                5023,
                7023,
                7,
                60,
                1,
                60,
                60
                );
            configuredMirrorDatabases.Add(configured2.DatabaseName, configured2);
            return configuredMirrorDatabases;
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
