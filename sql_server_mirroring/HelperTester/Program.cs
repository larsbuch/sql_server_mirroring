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
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToEveryone(Logger, directoryString);
            }

            GetNextInput("Press Enter to clean up test");
            Directory.Delete(directoryString, true);
        }

        #endregion

        #region Helper functions and constructs
        private static string DirectoryCreateAbsolutePath(string subDirectory)
        {
            return Directory.GetCurrentDirectory() + "\\" + subDirectory;
        }

        private static string GetNextInput(string inputRequest)
        {
            Console.WriteLine();
            Console.WriteLine(inputRequest);
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
