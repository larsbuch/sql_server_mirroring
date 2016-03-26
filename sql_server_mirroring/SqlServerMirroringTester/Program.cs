using ConsoleTester;
using HelperFunctions;
using MirrorLib;
using System;
using System.Collections.Generic;
using System.IO;

namespace MirrorLibTester
{
    class Program
    {

        static void Main(string[] args)
        {
            Configuration.Add(CONNECTION_STRING, "Server=localhost;Trusted_Connection=True;");
            string localServer = ConsoleTest.GetNextInput("Local Server: ", Environment.MachineName);
            Configuration.Add(LOCALSERVER, localServer);
            Configuration.Add(REMOTESERVER, ConsoleTest.GetNextInput("Remote Server: "));
            Configuration.Add(DIRECORYFORLOCALBACKUP, ConsoleTest.GetNextInput("Directory for local backup: ", "C:\\Test\\LocalBackup"));
            Configuration.Add(DIRECORYFORLOCALSHARE, ConsoleTest.GetNextInput("Directory for local share: ", "C:\\Test\\Share"));
            Configuration.Add(DIRECORYFORLOCALRESTORE, ConsoleTest.GetNextInput("Directory for local restore: ", "C:\\Test\\LocalRestore"));
            Configuration.Add(LOCALSHARENAME, ConsoleTest.GetNextInput("Local share name: ", localServer.Replace("-", "_") + "Share"));
            Configuration.Add(ENDPOINT_NAME, ConsoleTest.GetNextInput("Endpoint Name: ", "Mirroring_Endpoint"));
            Configuration.Add(ENDPOINT_LISTENERPORT, ConsoleTest.GetNextInput("Endpoint Listener Port: ", "7022"));
            Configuration.Add(REMOTESERVERCOMMUNICATIONTIMEOUT, ConsoleTest.GetNextInput("Remote Communication Timeout: ", 10.ToString()));
            Configuration.Add(DATABASESFORMIRRORING, ConsoleTest.GetNextInput("Databases for mirroring (comma separates): "));

            ConsoleTest.AddTest("Information", "Get local instance status", () => Test_Information_InstanceStatus());
            ConsoleTest.AddTest("Information", "Get instance information", () => Test_Information_Instance());
            ConsoleTest.AddTest("Information", "Get sql agent information", () => Test_Information_SqlAgent());
            ConsoleTest.AddTest("Information", "Check Windows Authentification", () => Test_Information_WindowsAuthentificationActive());
            ConsoleTest.AddTest("Information", "Check Sql Server Authentification", () => Test_Information_SqlServerAuthentificationActive());
            ConsoleTest.AddTest("Information", "Check for instance readyness for mirroring", () => Test_Information_CheckInstanceForMirroring());

            ConsoleTest.AddTest("RemoteServer", "Check for connection to remote server", () => Test_Information_HasAccessToRemoteServer());
            ConsoleTest.AddTest("RemoteServer", "Get remote instance status", () => Test_Information_RemoteServer_InstanceStatus());
            
            ConsoleTest.AddTest("Action", "Run Start Primary", () => Test_Action_StartPrimary());
            ConsoleTest.AddTest("Action", "Run Start Secondary", () => Test_Action_StartSecondary());
            ConsoleTest.AddTest("Action", "Setup Monitoring", () => Test_Action_SetupMonitoring());
            ConsoleTest.AddTest("Action", "Run CheckServerState", () => Test_Action_CheckServerState());
            ConsoleTest.AddTest("Action", "Resume Mirroring For All Mirror Databases", () => Test_Action_ResumeMirroringForAllDatabases());
            ConsoleTest.AddTest("Action", "Suspend Mirroring For All Mirror Databases", () => Test_Action_SuspendMirroringForAllMirrorDatabases());
            ConsoleTest.AddTest("Action", "Force Failover With Data Loss For All Mirror Databases", () => Test_Action_ForceFailoverWithDataLossForAllMirrorDatabases());
            ConsoleTest.AddTest("Action", "Failover For All Mirror Databases", () => Test_Action_FailoverForAllMirrorDatabases());
            ConsoleTest.AddTest("Action", "Backup For All Configured Databases", () => Test_Action_BackupForAllConfiguredDatabases());
            ConsoleTest.AddTest("Action", "Restore All Configured Databases", () => Test_Action_RestoreForAllConfiguredDatabases());
            ConsoleTest.AddTest("Action", "Shut Down Mirroring Service", () => Test_Action_ShutDownMirroringService());
            ConsoleTest.AddTest("Action", "Force Shut Down Mirroring Service", () => Test_Action_ForceShutDownMirroringService());

            ConsoleTest.Run();
        }

        #region Information Tests

        private static void Test_Information_InstanceStatus()
        {
            Console.WriteLine(string.Format("Instance Status: {0}", SqlServer.Information_Instance_Status()));

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
            Console.WriteLine(string.Format("Instance Windows Authentification active: {0}", SqlServer.Information_Instance_WindowsAuthentificationActive() ? "Yes" : "No"));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Information_SqlServerAuthentificationActive()
        {
            Console.WriteLine(string.Format("Instance Sql server Authentification active: {0}", SqlServer.Information_Instance_SqlServerAuthentificationActive() ? "Yes" : "No"));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Information_CheckInstanceForMirroring()
        {
            Console.WriteLine(string.Format("Instance Ready for mirroring: {0}", SqlServer.Information_Instance_CheckForMirroring() ? "Yes" : "No"));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Information_HasAccessToRemoteServer()
        {
            Console.WriteLine(string.Format("Instance Ready for mirroring: {0}", SqlServer.Information_RemoteServer_HasAccess() ? "Yes" : "No"));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Information_RemoteServer_InstanceStatus()
        {
            Console.WriteLine(string.Format("Remote Instance Status: {0}", SqlServer.Information_RemoteServer_Status()));

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        #endregion

        #region Action Tests

        private static void Test_Action_StartPrimary()
        {

            SqlServer.Action_Instance_StartPrimary();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_StartSecondary()
        {

            SqlServer.Action_Instance_StartSecondary();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_SetupMonitoring()
        {
            SqlServer.Action_Instance_SetupMonitoring();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_CheckServerState()
        {
            SqlServer.Action_ServerState_TimedCheck();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_ResumeMirroringForAllDatabases()
        {
            SqlServer.Action_Instance_ResumeMirroringForAllDatabases();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_SuspendMirroringForAllMirrorDatabases()
        {
            SqlServer.Action_Instance_SuspendMirroringForAllMirrorDatabases();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_ForceFailoverWithDataLossForAllMirrorDatabases()
        {
            SqlServer.Action_Instance_ForceFailoverWithDataLossForAllMirrorDatabases();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_FailoverForAllMirrorDatabases()
        {
            SqlServer.Action_Instance_FailoverForAllMirrorDatabases();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_BackupForAllConfiguredDatabases()
        {
            SqlServer.Action_Instance_BackupForAllConfiguredDatabasesForMirrorServer();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_RestoreForAllConfiguredDatabases()
        {
            SqlServer.Action_Instance_RestoreForAllConfiguredDatabases();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_ShutDownMirroringService()
        {
            SqlServer.Action_Instance_ShutDown();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        private static void Test_Action_ForceShutDownMirroringService()
        {
            SqlServer.Action_Instance_ForceShutDownMirroringService();

            ConsoleTest.GetNextInput("Press Enter to exit test.");
        }

        #endregion

        #region Helper functions and constructs

        private const string CONNECTION_STRING = "CONNECTION_STRING";
        private const string LOCALSERVER = "LOCALSERVER";
        private const string REMOTESERVER = "REMOTESERVER";
        private const string DIRECORYFORLOCALBACKUP = "DIRECORYFORLOCALBACKUP";
        private const string DIRECORYFORLOCALSHARE = "DIRECORYFORLOCALSHARE";
        private const string DIRECORYFORLOCALRESTORE = "DIRECORYFORLOCALRESTORE";
        private const string LOCALSHARENAME = "LOCALSHARENAME";
        private const string ENDPOINT_LISTENERPORT = "ENDPOINT_LISTENERPORT";
        private const string ENDPOINT_NAME = "ENDPOINT_NAME";
        private const string DATABASESFORMIRRORING = "DATABASESFORMIRRORING";
        private const string REMOTESERVERCOMMUNICATIONTIMEOUT = "REMOTESERVERCOMMUNICATIONTIMEOUT";

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

        private static string GetConfiguration(string key)
        {
            string returnString;
            if (Configuration.TryGetValue(key, out returnString))
            {
                return returnString;
            }
            else
            {
                throw new SqlServerMirroringTesterException(string.Format("Configuration value {0} is missing in Configuration.", CONNECTION_STRING));
            }

        }

        private static SqlServerInstance SqlServer
        {
            get
            {
                if (_sqlServerInstance == null)
                {
                    _sqlServerInstance = new SqlServerInstance(Logger, GetConfiguration(CONNECTION_STRING));
                    _sqlServerInstance.Databases_Configuration = BuildDatabaseConfiguration();
                    _sqlServerInstance.Instance_Configuration = BuildInstanceConfiguration();
                }
                return _sqlServerInstance;
            }
        }

        private static ConfigurationForInstance BuildInstanceConfiguration()
        {
            return new ConfigurationForInstance(
                new RemoteServer(GetConfiguration(REMOTESERVER)),
                new DirectoryPath(GetConfiguration(DIRECORYFORLOCALBACKUP)),
                new DirectoryPath(GetConfiguration(DIRECORYFORLOCALSHARE)),
                new DirectoryPath(GetConfiguration(DIRECORYFORLOCALRESTORE)),
                new ShareName(GetConfiguration(LOCALSHARENAME)),
                int.Parse(GetConfiguration(ENDPOINT_LISTENERPORT)),
                7,
                12,
                12,
                12,
                1,
                int.Parse(GetConfiguration(REMOTESERVERCOMMUNICATIONTIMEOUT)),
                GetConfiguration(ENDPOINT_NAME),
                120,
                5,
                30,
                30,
                24,
                true,
                new BackupTime(03,00),
                5
                );

        }

        private static Dictionary<string, ConfigurationForDatabase> BuildDatabaseConfiguration()
        {
            string localServer = GetConfiguration(LOCALSERVER);
            string remoteServer = GetConfiguration(REMOTESERVER);
            string directoryForLocalBackup = GetConfiguration(DIRECORYFORLOCALBACKUP);
            string directoryForLocalShare = GetConfiguration(DIRECORYFORLOCALSHARE);
            string directoryForLocalRestore = GetConfiguration(DIRECORYFORLOCALRESTORE);
            string localShareName = GetConfiguration(LOCALSHARENAME);
            string databasesForMirroring = GetConfiguration(DATABASESFORMIRRORING);
            Dictionary<string, ConfigurationForDatabase> configuredMirrorDatabases = new Dictionary<string, ConfigurationForDatabase>();

            string[] databases = databasesForMirroring.Split(',');

            foreach(string database in databases)
            {
                ConfigurationForDatabase configured = new ConfigurationForDatabase(
                    new DatabaseName(database.Trim()),
                    new DirectoryPath(directoryForLocalBackup),
                    new DirectoryPath(directoryForLocalShare),
                    new DirectoryPath(directoryForLocalRestore),
                    new ShareName(localServer.Replace("-","_") + "Share"),
                    new RemoteServer(remoteServer),
                    new ShareName(remoteServer.Replace("-", "_") + "Share"),
                    new SubDirectory("LocalTransfer"),
                    new SubDirectory("RemoteTransfer"),
                    new SubDirectory("RemoteDelivery")
                    );
                configuredMirrorDatabases.Add(configured.DatabaseName.ToString(), configured);
            }
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
            private const string LOGNAME = "Log.log";
            public ConsoleLogger()
            {
                File.Delete(LOGNAME);
            }

            public void LogDebug(string message)
            {
                Console.WriteLine("LogDebug: " + message);
                File.AppendAllText(LOGNAME, "LogDebug: " + message + System.Environment.NewLine);
            }

            public void LogInfo(string message)
            {
                Console.WriteLine("LogInfo: " + message);
                File.AppendAllText(LOGNAME, "LogInfo: " + message + System.Environment.NewLine);
            }

            public void LogWarning(string message)
            {
                Console.WriteLine("LogWarning: " + message);
                File.AppendAllText(LOGNAME, "LogWarning: " + message + System.Environment.NewLine);
            }
            public void LogError(string message)
            {
                Console.WriteLine("LogError: " + message);
                File.AppendAllText(LOGNAME, "LogError: " + message + System.Environment.NewLine);
            }
            public void LogError(string message, Exception exception)
            {
                Console.WriteLine("LogError: " + message);
                File.AppendAllText(LOGNAME, "LogError: " + message + System.Environment.NewLine);
                string exceptionHierarcy = BuildExceptionHierarchy(exception);
                Console.WriteLine("Exception Message: " + exceptionHierarcy);
                File.AppendAllText(LOGNAME, "Exception Message: " + exceptionHierarcy);
                Console.WriteLine("Exception StackTrace: " + exception.StackTrace);
                File.AppendAllText(LOGNAME, "Exception StackTrace: " + exception.StackTrace + System.Environment.NewLine);
            }

            private string BuildExceptionHierarchy(Exception exception)
            {
                if(exception.InnerException == null)
                {
                    return exception.Message + System.Environment.NewLine;
                }
                else
                {
                    return exception.Message + System.Environment.NewLine + "InnerException: " + BuildExceptionHierarchy(exception.InnerException);
                }
            }
        }
        #endregion
    }
}
