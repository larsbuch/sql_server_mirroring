using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HelperFunctions
{
    public class Program
    {
        static void Main(string[] args)
        {
            AddTest("Test validate local directory.", () => TestValidateDirectory());
            AddTest("Test read write access in current dircetory.", () => TestReadWriteAccessToDirectory());
            AddTest("Test creation of local directory run once.", () => TestCreateLocalDirectory(1));
            AddTest("Test creation of local directory run twice.", () => TestCreateLocalDirectory(2));
            AddTest("Test creation of local directory with access for Everyone.", () => TestCreateLocalDirectoryWithAccessForEveryone(1));
            AddTest("Test local machine name as server name.", () => TestValidServerName());
            AddTest("Test creation of local share run once.", () => TestCreateShareDirectory(1));
            AddTest("Test creation of local share run twice.", () => TestCreateShareDirectory(2));
            AddTest("Test creation of local share with access for Everyone.", () => TestCreateLocalShareWithAccessForEveryone(1));
            AddTest("Test of read/write access to share.", () => TestReadWriteAccessToShare());
            AddTest("Test of correct WOW registry key.", () => TestCorrect64Or32bit());
            AddTest("Test valid registry key.", () => TestValidRegistryKey());
            AddTest("Test of existance of registry key.", () => TestRegistryKeyExists());
            AddTest("Test user has read/write access to registry key.", () => TestRegistryKeyUserHasReadWriteAccess());
            AddTest("Test of write/read of Binary registry key.", () => TestRegistryKeyReadWriteBinary());
            AddTest("Test of write/read of DWord registry key.", () => TestRegistryKeyReadWriteDWord());
            AddTest("Test of write/read of ExpandString registry key.", () => TestRegistryKeyReadWriteExpandString());
            AddTest("Test of write/read of MultiString registry key.", () => TestRegistryKeyReadWriteMultiString());
            AddTest("Test of write/read of QWord registry key.", () => TestRegistryKeyReadWriteQWord());
            AddTest("Test of write/read of String registry key.", () => TestRegistryKeyReadWriteString());

            string inputLine = string.Empty;
            bool exit = false;
            while(!exit)
            {
                Console.Clear();
                Console.WriteLine("Use: Select a test to run of the following by entering number and hit Enter.");
                Console.WriteLine("0) Exit");
                foreach (Test test in TestsToRun.Values)
                {
                    Console.WriteLine(test.ListNumber.ToString() + ") " + test.Explanation);
                }
                inputLine = Console.ReadLine();
                if(inputLine.Equals("0"))
                {
                    exit = true;
                }
                else
                {
                    Test test = null;
                    if(TestsToRun.TryGetValue(inputLine, out test))
                    {
                        try
                        {
                            test.TestToRun.Invoke();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(string.Format("Exception cast. Type : {0}", ex.GetType().ToString()));
                            Console.WriteLine(string.Format("Exception message: {0}", ex.Message));
                            Console.WriteLine(string.Format("Exception stacktrace: {0}", ex.StackTrace));
                            GetNextInput("Press Enter to end test");
                        }
                    }
                    else
                    {
                        Console.WriteLine(string.Format("Unknown input: |{0}|. Please try again.", inputLine));
                    }
                }
            }
        }

        #region Tests
        private static void TestValidateDirectory()
        {
            string subDirectoryString = GetNextInput("Enter sub dircetory to validate. End with Enter.");
            string directoryString = DirectoryCreateAbsolutePath(subDirectoryString);
            Console.WriteLine(string.Format("Testing validation of {0} with {1}.", subDirectoryString, directoryString));
            DirectoryHelper.ValidDirectoryName(Logger, directoryString);

            GetNextInput("Press Enter to end test");
        }

        private static void TestReadWriteAccessToDirectory()
        {
            string subDirectoryString = GetNextInput("Testing read write access test. Proceede with Enter.");
            DirectoryHelper.TestReadWriteAccessToDirectory(Logger, Directory.GetCurrentDirectory());

            GetNextInput("Press Enter to end test");
        }

        private static void TestCreateLocalDirectory(int runs)
        {
            string subDirectoryString = GetNextInput("Enter sub dircetory to test. End with Enter.");
            string directoryString = DirectoryCreateAbsolutePath(subDirectoryString);
            Console.WriteLine(string.Format("Testing creation of local directory {0} with absolute path {1} {2} timers.", subDirectoryString, directoryString, runs));
            for (int counter = 0; counter < runs; counter += 1)
            {
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, directoryString);
            }

            GetNextInput("Press Enter to clean up test");
            Directory.Delete(directoryString, true);
        }

        private static void TestCreateLocalDirectoryWithAccessForEveryone(int runs)
        {
            string subDirectoryString = GetNextInput("Enter sub dircetory to test. End with Enter.");
            string directoryString = DirectoryCreateAbsolutePath(subDirectoryString);
            Console.WriteLine(string.Format("Testing creation of local directory {0} with absolute path {1} {2} timers for Everyone.", subDirectoryString, directoryString, runs));
            for (int counter = 0; counter < runs; counter += 1)
            {
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToEveryone(Logger, directoryString);
            }

            GetNextInput("Press Enter to clean up test");
            Directory.Delete(directoryString, true);
        }

        private static void TestValidServerName()
        {
            ShareHelper.ValidServerName(Logger, Environment.MachineName);

            GetNextInput("Press Enter to end test");
        }

        private static void TestCreateShareDirectory(int runs)
        {
            string subDirectoryString = GetNextInput("Enter sub dircetory to test. End with Enter.");
            string shareName = GetNextInput("Enter share name to test. End with Enter.");
            string directoryString = DirectoryCreateAbsolutePath(subDirectoryString);
            Console.WriteLine(string.Format("Testing creation of local share {0} with absolute path {1} {2} timers.", shareName, directoryString, runs));
            for (int counter = 0; counter < runs; counter += 1)
            {
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(Logger, directoryString, shareName);
            }

            GetNextInput("Press Enter to clean up test");
            Directory.Delete(directoryString, true);
        }

        private static void TestCreateLocalShareWithAccessForEveryone(int runs)
        {
            string subDirectoryString = GetNextInput("Enter sub dircetory to test. End with Enter.");
            string shareName = GetNextInput("Enter share name to test. End with Enter.");
            string directoryString = DirectoryCreateAbsolutePath(subDirectoryString);
            Console.WriteLine(string.Format("Testing creation of local share {0} with absolute path {1} {2} timers for Everyone.", shareName, directoryString, runs));
            for (int counter = 0; counter < runs; counter += 1)
            {
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveEveryoneAccess(Logger, directoryString, shareName);
            }

            GetNextInput("Press Enter to clean up test");
            Directory.Delete(directoryString, true);
        }

        private static void TestReadWriteAccessToShare()
        {
            string serverName = GetNextInput("Enter server name to test. End with Enter.", Environment.MachineName);
            string shareName = GetNextInput("Enter share name to test. End with Enter.", "Test");

            ShareHelper.TestReadWriteAccessToShare(Logger, serverName, shareName);

            GetNextInput("Press Enter to end test");
        }

        private static void TestCorrect64Or32bit()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            RegistryHelper.Correct64Or32bit(Logger, regKey);

            GetNextInput("Press Enter to end test");
        }

        private static void TestValidRegistryKey()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            RegistryHelper.ValidRegistryKey(Logger, regKey);

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyExists()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "Status");
            RegistryHelper.Exists(Logger, regKey, regValue);

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyUserHasReadWriteAccess()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "Status");
            if (RegistryHelper.HasReadWriteAccess(Logger, regKey, regValue))
            {
                Console.WriteLine(string.Format("User has read/write accessto {0}\\{1}", regKey, regValue));
            }
            else
            {
                Console.WriteLine(string.Format("User does not have write access {0}\\{1}", regKey, regValue));
            }

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyGetRegistryValueKind()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "Status");

            Microsoft.Win32.RegistryValueKind registryValueKind = RegistryHelper.GetRegistryValueKind(Logger, regKey, regValue);
            Console.WriteLine(string.Format("RegistryValueKind: {0}",registryValueKind.ToString()));

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyReadWriteExpandString()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "R_ExpandString");
            string value = "TestValue with %Path%";

            RegistryHelper.SetRegistryValue_ExpandString(Logger, regKey, regValue, value);
            if(RegistryHelper.GetRegistryValue_ExpandString(Logger, regKey, regValue).Equals(value))
            {
                Console.WriteLine("Success");
            }
            else
            {
                Console.WriteLine("Failure");
            }

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyReadWriteString()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "R_String");
            string value = "TestValue";

            RegistryHelper.SetRegistryValue_String(Logger, regKey, regValue, value);
            if (RegistryHelper.GetRegistryValue_String(Logger, regKey, regValue).Equals(value))
            {
                Console.WriteLine("Success");
            }
            else
            {
                Console.WriteLine("Failure");
            }

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyReadWriteQWord()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "R_QWord");
            UInt64 value = 158156464545;

            RegistryHelper.SetRegistryValue_QWord(Logger, regKey, regValue, value);
            if (RegistryHelper.GetRegistryValue_QWord(Logger, regKey, regValue) == value)
            {
                Console.WriteLine("Success");
            }
            else
            {
                Console.WriteLine("Failure");
            }

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyReadWriteMultiString()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "R_MultiString");
            string[] value = new string[] { "TestValue1", "TestValue2", "TestValue3" };

            RegistryHelper.SetRegistryValue_MultiString(Logger, regKey, regValue, value);
            if (CompareStringArray(RegistryHelper.GetRegistryValue_MultiString(Logger, regKey, regValue), value))
            {
                Console.WriteLine("Success");
            }
            else
            {
                Console.WriteLine("Failure");
            }

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyReadWriteDWord()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "R_DWord");
            UInt32 value = 15874;

            RegistryHelper.SetRegistryValue_DWord(Logger, regKey, regValue, value);
            if (RegistryHelper.GetRegistryValue_DWord(Logger, regKey, regValue) == value)
            {
                Console.WriteLine("Success");
            }
            else
            {
                Console.WriteLine("Failure");
            }

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyReadWriteBinary()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "R_Binary");
            byte[] value = { 1, 2, 4, 8, 16, 32 };

            RegistryHelper.SetRegistryValue_Binary(Logger, regKey, regValue, value);
            if (CompareByteArray(RegistryHelper.GetRegistryValue_Binary(Logger, regKey, regValue),value))
            {
                Console.WriteLine("Success");
            }
            else
            {
                Console.WriteLine("Failure");
            }

            GetNextInput("Press Enter to end test");
        }

        #endregion

        #region Helper functions and constructs

        private static bool CompareByteArray(byte[] byteArray1, byte[] byteArray2)
        {
            //throw new NotImplementedException();
        }

        private static bool CompareStringArray(string[] array1, string[] array2)
        {
            //throw new NotImplementedException();
        }

        private static string DirectoryCreateAbsolutePath(string subDirectory)
        {
            return Directory.GetCurrentDirectory() + "\\" + subDirectory;
        }

        private static string GetNextInput(string inputRequest)
        {
            return GetNextInput(inputRequest, null);
        }


        private static string GetNextInput(string inputRequest, string defaultValue)
        {
            Console.WriteLine();
            if (inputRequest == null)
            {
                Console.Write(inputRequest);
            }
            if (defaultValue == null)
            {
                System.Windows.Forms.SendKeys.SendWait(defaultValue);
            }
            return Console.ReadLine();
        }

        static Dictionary<string, Test> _testsToRun = new Dictionary<string, Test>();
        static int _highestTestNumber = 1;
        static ILogger _logger = new ConsoleLogger();

        private static int HighestTestNumber
        {
            get
            {
                return _highestTestNumber++;
            }
        }

        private static ILogger Logger
        {
            get
            {
                return _logger;
            }
        }

        private static void AddTest(string explanation, Action testToRun)
        {
            Test test = new Test(HighestTestNumber, explanation, testToRun);
            _testsToRun.Add(test.ListNumber.ToString(), test);
        }

        private static Dictionary<string, Test> TestsToRun
        {
            get
            {
                return _testsToRun;
            }
        }

        private class Test
        {
            public Test(int listNumber, string explanation, Action testToRun)
            {
                ListNumber = listNumber;
                Explanation = explanation;
                TestToRun = testToRun;
            }

            public int ListNumber { get; private set; }

            public string Explanation { get; private set; }

            public Action TestToRun { get; private set; }
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
        }
        #endregion
    }
}
