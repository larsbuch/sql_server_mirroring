using ConsoleTester;
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
            ConsoleTest.AddTest("DirectoryHelper", "Test validate local directory.", () => TestValidateDirectory());
            ConsoleTest.AddTest("DirectoryHelper", "Test read write access in current dircetory.", () => TestReadWriteAccessToDirectory());
            ConsoleTest.AddTest("DirectoryHelper", "Test creation of local directory run once.", () => TestCreateLocalDirectory(1));
            ConsoleTest.AddTest("DirectoryHelper", "Test creation of local directory run twice.", () => TestCreateLocalDirectory(2));
            ConsoleTest.AddTest("DirectoryHelper", "Test creation of local directory with access for Everyone.", () => TestCreateLocalDirectoryWithAccessForEveryone(1));
            ConsoleTest.AddTest("ShareHelper", "Test local machine name as server name.", () => TestValidServerName());
            ConsoleTest.AddTest("ShareHelper", "Test creation of local share run once.", () => TestCreateShareDirectory(1));
            ConsoleTest.AddTest("ShareHelper", "Test creation of local share run twice.", () => TestCreateShareDirectory(2));
            ConsoleTest.AddTest("ShareHelper", "Test creation of local share with access for Everyone.", () => TestCreateLocalShareWithAccessForEveryone(1));
            ConsoleTest.AddTest("ShareHelper", "Test of read/write access to share.", () => TestReadWriteAccessToShare());
            ConsoleTest.AddTest("RegistryHelper", "Test of correct WOW registry key.", () => TestCorrect64Or32bit());
            ConsoleTest.AddTest("RegistryHelper", "Test valid registry key.", () => TestValidRegistryKey());
            ConsoleTest.AddTest("RegistryHelper", "Test registry value creation.", () => TestRegistryCreateValue());
            ConsoleTest.AddTest("RegistryHelper", "Test of existance of registry key.", () => TestRegistryValueExists());
            ConsoleTest.AddTest("RegistryHelper", "Test user has read/write access to registry key.", () => TestRegistryUserHasReadWriteAccess());
            ConsoleTest.AddTest("RegistryHelper", "Test deletion of registry value.", () => TestRegistryDeleteRegistryValue());
            ConsoleTest.AddTest("RegistryHelper", "Test of write/read of Binary registry key.", () => TestRegistryKeyReadWriteBinary());
            ConsoleTest.AddTest("RegistryHelper", "Test of write/read of DWord registry key.", () => TestRegistryKeyReadWriteDWord());
            ConsoleTest.AddTest("RegistryHelper", "Test of write/read of ExpandString registry key.", () => TestRegistryKeyReadWriteExpandString());
            ConsoleTest.AddTest("RegistryHelper", "Test of write/read of MultiString registry key.", () => TestRegistryKeyReadWriteMultiString());
            ConsoleTest.AddTest("RegistryHelper", "Test of write/read of QWord registry key.", () => TestRegistryKeyReadWriteQWord());
            ConsoleTest.AddTest("RegistryHelper", "Test of write/read of String registry key.", () => TestRegistryKeyReadWriteString());

            ConsoleTest.Run();
        }

        #region Tests
        private static void TestValidateDirectory()
        {
            string subDirectoryString = ConsoleTest.GetNextInput("Enter sub dircetory to validate. End with Enter: ", "test");
            string directoryString = DirectoryCreateAbsolutePath(subDirectoryString);
            Console.WriteLine(string.Format("Testing validation of {0} with {1}.", subDirectoryString, directoryString));
            DirectoryHelper.ValidDirectoryName(Logger, directoryString);

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestReadWriteAccessToDirectory()
        {
            string subDirectoryString = ConsoleTest.GetNextInput("Testing read write access test. End with Enter: ", "test");
            DirectoryHelper.TestReadWriteAccessToDirectory(Logger, Directory.GetCurrentDirectory());

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestCreateLocalDirectory(int runs)
        {
            string subDirectoryString = ConsoleTest.GetNextInput("Enter sub dircetory to test. End with Enter: ", "test");
            string directoryString = DirectoryCreateAbsolutePath(subDirectoryString);
            Console.WriteLine(string.Format("Testing creation of local directory {0} with absolute path {1} {2} timers.", subDirectoryString, directoryString, runs));
            for (int counter = 0; counter < runs; counter += 1)
            {
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(Logger, directoryString);
            }

            ConsoleTest.GetNextInput("Press Enter to clean up test");
            Directory.Delete(directoryString, true);
        }

        private static void TestCreateLocalDirectoryWithAccessForEveryone(int runs)
        {
            string subDirectoryString = ConsoleTest.GetNextInput("Enter sub dircetory to test. End with Enter: ", "test");
            string directoryString = DirectoryCreateAbsolutePath(subDirectoryString);
            Console.WriteLine(string.Format("Testing creation of local directory {0} with absolute path {1} {2} timers for Everyone.", subDirectoryString, directoryString, runs));
            for (int counter = 0; counter < runs; counter += 1)
            {
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToEveryone(Logger, directoryString);
            }

            ConsoleTest.GetNextInput("Press Enter to clean up test");
            Directory.Delete(directoryString, true);
        }

        private static void TestValidServerName()
        {
            ShareHelper.ValidServerName(Logger, Environment.MachineName);

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestCreateShareDirectory(int runs)
        {
            string subDirectoryString = ConsoleTest.GetNextInput("Enter sub dircetory to test. End with Enter: ", "test");
            string shareName = ConsoleTest.GetNextInput("Enter share name to test. End with Enter: ", "testShare");
            string directoryString = DirectoryCreateAbsolutePath(subDirectoryString);
            Console.WriteLine(string.Format("Testing creation of local share {0} with absolute path {1} {2} timers.", shareName, directoryString, runs));
            for (int counter = 0; counter < runs; counter += 1)
            {
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(Logger, directoryString, shareName);
            }

            ConsoleTest.GetNextInput("Press Enter to clean up test");
            Directory.Delete(directoryString, true);
        }

        private static void TestCreateLocalShareWithAccessForEveryone(int runs)
        {
            string subDirectoryString = ConsoleTest.GetNextInput("Enter sub dircetory to test. End with Enter: ", "test");
            string shareName = ConsoleTest.GetNextInput("Enter share name to test. End with Enter: ", "testShare");
            string directoryString = DirectoryCreateAbsolutePath(subDirectoryString);
            Console.WriteLine(string.Format("Testing creation of local share {0} with absolute path {1} {2} timers for Everyone.", shareName, directoryString, runs));
            for (int counter = 0; counter < runs; counter += 1)
            {
                ShareHelper.CreateLocalShareDirectoryIfNotExistingAndGiveEveryoneAccess(Logger, directoryString, shareName);
            }

            ConsoleTest.GetNextInput("Press Enter to clean up test");
            Directory.Delete(directoryString, true);
        }

        private static void TestReadWriteAccessToShare()
        {
            string serverName = ConsoleTest.GetNextInput("Enter server name to test. End with Enter: ", Environment.MachineName);
            string shareName = ConsoleTest.GetNextInput("Enter share name to test. End with Enter: ", "Test");

            ShareHelper.TestReadWriteAccessToShare(Logger, serverName, shareName);

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestCorrect64Or32bit()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            RegistryHelper.Correct64Or32bit(Logger, regKey);

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestValidRegistryKey()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            RegistryHelper.ValidRegistryKey(Logger, regKey);

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryCreateValue()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "Status");

            RegistryHelper.SetRegistryValue_String(Logger, regKey, regValue, "TestValue");

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyExists()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            if(RegistryHelper.RegistryKeyExists(Logger, regKey))
            {
                Console.WriteLine(string.Format("Registry key {0} exists.", regKey));
            }
            else
            {
                Console.WriteLine(string.Format("Registry key {0} does not exists.", regKey));
            }
            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryValueExists()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "Status");
            RegistryHelper.RegistryValueExists(Logger, regKey, regValue);

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryUserHasReadWriteAccess()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "Status");
            if (RegistryHelper.HasReadWriteAccess(Logger, regKey, regValue))
            {
                Console.WriteLine(string.Format("User has read/write access to {0}\\{1}", regKey, regValue));
            }
            else
            {
                Console.WriteLine(string.Format("User does not have write access {0}\\{1}", regKey, regValue));
            }

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryGetRegistryValueKind()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "Status");

            Microsoft.Win32.RegistryValueKind registryValueKind = RegistryHelper.GetRegistryValueKind(Logger, regKey, regValue);
            Console.WriteLine(string.Format("RegistryValueKind: {0}",registryValueKind.ToString()));

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryDeleteRegistryValue()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "Status");

            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);

            ConsoleTest.GetNextInput("Press Enter to end test");
        }

        private static void TestRegistryKeyReadWriteExpandString()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "R_ExpandString");
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

            ConsoleTest.GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
        }

        private static void TestRegistryKeyReadWriteString()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "R_String");
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

            ConsoleTest.GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
        }

        private static void TestRegistryKeyReadWriteQWord()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "R_QWord");
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

            ConsoleTest.GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
        }

        private static void TestRegistryKeyReadWriteMultiString()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "R_MultiString");
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

            ConsoleTest.GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
        }

        private static void TestRegistryKeyReadWriteDWord()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "R_DWord");
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

            ConsoleTest.GetNextInput("Press Enter to clean up after test");
            RegistryHelper.DeleteRegistryValue(Logger, regKey, regValue);
        }

        private static void TestRegistryKeyReadWriteBinary()
        {
            string regKey = ConsoleTest.GetNextInput("Registry Key: ", "HKEY_CURRENT_USER\\Software\\SqlMirror");
            string regValue = ConsoleTest.GetNextInput("Registry Value: ", "R_Binary");
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

            ConsoleTest.GetNextInput("Press Enter to clean up after test");
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
        }
        #endregion
    }
}
