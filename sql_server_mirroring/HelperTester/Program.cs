using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WindowsInput;

namespace HelperFunctions
{
    public class Program
    {
        static void Main(string[] args)
        {
            AddTest("DirectoryHelper","Test validate local directory.", () => TestValidateDirectory());
            AddTest("DirectoryHelper", "Test read write access in current dircetory.", () => TestReadWriteAccessToDirectory());
            AddTest("DirectoryHelper", "Test creation of local directory run once.", () => TestCreateLocalDirectory(1));
            AddTest("DirectoryHelper", "Test creation of local directory run twice.", () => TestCreateLocalDirectory(2));
            AddTest("DirectoryHelper", "Test creation of local directory with access for Everyone.", () => TestCreateLocalDirectoryWithAccessForEveryone(1));
            AddTest("ShareHelper", "Test local machine name as server name.", () => TestValidServerName());
            AddTest("ShareHelper", "Test creation of local share run once.", () => TestCreateShareDirectory(1));
            AddTest("ShareHelper", "Test creation of local share run twice.", () => TestCreateShareDirectory(2));
            AddTest("ShareHelper", "Test creation of local share with access for Everyone.", () => TestCreateLocalShareWithAccessForEveryone(1));
            AddTest("ShareHelper", "Test of read/write access to share.", () => TestReadWriteAccessToShare());
            AddTest("RegistryHelper", "Test of correct WOW registry key.", () => TestCorrect64Or32bit());
            AddTest("RegistryHelper", "Test valid registry key.", () => TestValidRegistryKey());
            AddTest("RegistryHelper", "Test registry value creation.", () => TestRegistryCreateValue());
            AddTest("RegistryHelper", "Test of existance of registry key.", () => TestRegistryValueExists());
            AddTest("RegistryHelper", "Test user has read/write access to registry key.", () => TestRegistryUserHasReadWriteAccess());
            AddTest("RegistryHelper", "Test deletion of registry value.", () => TestRegistryDeleteRegistryValue());
            AddTest("RegistryHelper", "Test of write/read of Binary registry key.", () => TestRegistryKeyReadWriteBinary());
            AddTest("RegistryHelper", "Test of write/read of DWord registry key.", () => TestRegistryKeyReadWriteDWord());
            AddTest("RegistryHelper", "Test of write/read of ExpandString registry key.", () => TestRegistryKeyReadWriteExpandString());
            AddTest("RegistryHelper", "Test of write/read of MultiString registry key.", () => TestRegistryKeyReadWriteMultiString());
            AddTest("RegistryHelper", "Test of write/read of QWord registry key.", () => TestRegistryKeyReadWriteQWord());
            AddTest("RegistryHelper", "Test of write/read of String registry key.", () => TestRegistryKeyReadWriteString());

            string inputLine = string.Empty;
            bool exit = false;
            string selectedGroupName = null;
            while (!exit)
            {
                Console.Clear();
                if (string.IsNullOrWhiteSpace(selectedGroupName))
                {
                    Console.WriteLine("Use: Select a test group to run of the following by entering number and hit Enter.");
                    Console.WriteLine("0) Exit");
                    Dictionary<string, string> selectionList = new Dictionary<string, string>();
                    int counter = 1;
                    foreach (string groupName in TestGroupsToRun.Keys)
                    {
                        selectionList.Add(counter.ToString(), groupName);
                        Console.WriteLine(counter.ToString() + ") " + groupName);
                        counter += 1;
                    }
                    inputLine = Console.ReadLine();
                    if (inputLine.Equals("0"))
                    {
                        exit = true;
                    }
                    else
                    {
                        if (!selectionList.TryGetValue(inputLine, out selectedGroupName))
                        {
                            GetNextInput(string.Format("Unknown input: |{0}|. Please try again. Press Enter to procede.", inputLine));
                        }
                    }
                }
                else
                {
                    Dictionary<string, Test> testGroup;
                    if (!TestGroupsToRun.TryGetValue(selectedGroupName, out testGroup))
                    {
                        GetNextInput(string.Format("Could not get {0}. Returning to group selection. Press Enter to procede.", selectedGroupName));
                        selectedGroupName = null;
                    }
                    else
                    {
                        Console.WriteLine("Use: Select a test to run of the following by entering number and hit Enter.");
                        Console.WriteLine("0) Return to previous list.");
                        foreach (Test test in testGroup.Values)
                        {
                            Console.WriteLine(test.ListNumber.ToString() + ") " + test.Explanation);
                        }
                        inputLine = Console.ReadLine();
                        if (inputLine.Equals("0"))
                        {
                            selectedGroupName = null;
                        }
                        else
                        {
                            Test test = null;
                            if (testGroup.TryGetValue(inputLine, out test))
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
                                GetNextInput(string.Format("Unknown input: |{0}|. Please try again. Press Enter to procede.", inputLine));
                            }
                        }
                    }
                }
            } }

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

        private static void TestRegistryCreateValue()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "Status");

            RegistryHelper.SetRegistryValue_String(Logger, regKey, regValue, "TestValue");

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyExists()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            if(RegistryHelper.RegistryKeyExists(Logger, regKey))
            {
                Console.WriteLine(string.Format("Registry key {0} exists.", regKey));
            }
            else
            {
                Console.WriteLine(string.Format("Registry key {0} does not exists.", regKey));
            }
            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryValueExists()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "Status");
            RegistryHelper.RegistryValueExists(Logger, regKey, regValue);

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryUserHasReadWriteAccess()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "Status");
            if (RegistryHelper.HasReadWriteAccess(Logger, regKey, regValue))
            {
                Console.WriteLine(string.Format("User has read/write access to {0}\\{1}", regKey, regValue));
            }
            else
            {
                Console.WriteLine(string.Format("User does not have write access {0}\\{1}", regKey, regValue));
            }

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryGetRegistryValueKind()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "Status");

            Microsoft.Win32.RegistryValueKind registryValueKind = RegistryHelper.GetRegistryValueKind(Logger, regKey, regValue);
            Console.WriteLine(string.Format("RegistryValueKind: {0}",registryValueKind.ToString()));

            GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryDeleteRegistryValue()
        {
            string regKey = GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = GetNextInput("Registry Value: ", "Status");

            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);

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

            GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
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

            GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
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

            GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
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

            GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
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

            GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
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

            GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
        }

        #endregion

        #region Helper functions and constructs

        private static bool CompareByteArray(byte[] byteArray1, byte[] byteArray2)
        {
            if(byteArray1 != null && byteArray2 != null && byteArray1.Length == byteArray2.Length)
            {
                for(int counter = 0; counter < byteArray1.Length;counter+=1)
                {
                    if(byteArray1[counter] != byteArray2[counter])
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

        private static string GetNextInput(string inputRequest)
        {
            return GetNextInput(inputRequest, null);
        }


        private static string GetNextInput(string inputRequest, string defaultValue)
        {
            Console.WriteLine();
            if (inputRequest != null)
            {
                Console.Write(inputRequest);
            }
            if (defaultValue != null)
            {
                InputSimulator.SimulateTextEntry(defaultValue);
            }
            string capturedString = Console.ReadLine();
            Console.WriteLine(string.Format("Captured: {0}", capturedString));

            return capturedString;
        }

        static Dictionary<string,Dictionary<string, Test>> _testsToRun = new Dictionary<string, Dictionary<string, Test>>();
        static ILogger _logger = new ConsoleLogger();

        private static int NewTestNumber(string groupName)
        {
            return TestsToRun(groupName).Count + 1;
        }

        private static ILogger Logger
        {
            get
            {
                return _logger;
            }
        }

        private static void AddTest(string groupName, string explanation, Action testToRun)
        {
            Test test = new Test(NewTestNumber(groupName), explanation, testToRun);
            TestsToRun(groupName).Add(test.ListNumber.ToString(), test);
        }

        private static Dictionary<string, Dictionary<string, Test>> TestGroupsToRun
        {
            get
            {
                return _testsToRun;
            }
        }

        private static Dictionary<string, Test> TestsToRun(string groupName)
        {
            Dictionary<string, Test> testsToRun;
            if (TestGroupsToRun.ContainsKey(groupName))
            {
                if (TestGroupsToRun.TryGetValue(groupName, out testsToRun))
                {
                    return testsToRun;
                }
                else
                {
                    throw new Exception("Could not get test group out.");
                }
            }
            else
            {
                testsToRun = new Dictionary<string, Test>();
                TestGroupsToRun.Add(groupName, testsToRun);
                return testsToRun;
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
