using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace HelperFunctions
{
    public static class FileCheckHelper
    {
        private const string DIRECTORYTESTFILE = "\\DirectoryTestFile.txt";
        private static string[] lines = { "First line", "Second line", "Third line" };

        public static void WriteTestFileToDirectory(ILogger logger, DirectoryPath directoryPath)
        {
            File.WriteAllLines(directoryPath.PathString + DIRECTORYTESTFILE, lines);
            logger.LogDebug(string.Format("Test file written {0}.", directoryPath.PathString + DIRECTORYTESTFILE));
        }

        public static void ReadTestFileFromDirectoryAndCompare(ILogger logger, DirectoryPath directoryPath)
        {
            bool match = false;
            string[] readLines = File.ReadAllLines(directoryPath.PathString + DIRECTORYTESTFILE);
            if (readLines.Length == 3)
            {
                if (readLines[0].Equals(lines[0]) && readLines[1].Equals(lines[1]) && readLines[2].Equals(lines[2]))
                {
                    logger.LogDebug("Lines read from testfile correct.");
                    match = true;
                }
            }

            if (!match)
            {
                throw new FileCheckException(string.Format("Filecheck failed for {0}", directoryPath));
            }
        }

        public static void DeleteTestFileFromDirectory(ILogger logger, DirectoryPath directoryPath)
        {
            File.Delete(directoryPath.PathString + DIRECTORYTESTFILE);
            logger.LogDebug(string.Format("Test file {0} deleted.", directoryPath.PathString + DIRECTORYTESTFILE));
        }
        public static void WriteTestFileToDirectory(ILogger logger, Uri uriPath)
        {
            File.WriteAllLines(uriPath.ToString() + DIRECTORYTESTFILE, lines);
            logger.LogDebug(string.Format("Test file written {0}.", uriPath.ToString() + DIRECTORYTESTFILE));
        }

        public static void ReadTestFileFromDirectoryAndCompare(ILogger logger, Uri uriPath)
        {
            bool match = false;
            string[] readLines = File.ReadAllLines(uriPath.ToString() + DIRECTORYTESTFILE);
            if (readLines.Length == 3)
            {
                if (readLines[0].Equals(lines[0]) && readLines[1].Equals(lines[1]) && readLines[2].Equals(lines[2]))
                {
                    logger.LogDebug("Lines read from testfile correct.");
                    match = true;
                }
            }

            if (!match)
            {
                throw new FileCheckException(string.Format("Filecheck failed for {0}", uriPath.ToString()));
            }
        }

        public static void DeleteTestFileFromDirectory(ILogger logger, Uri uriPath)
        {
            File.Delete(uriPath.ToString() + DIRECTORYTESTFILE);
            logger.LogDebug(string.Format("Test file {0} deleted.", uriPath.ToString() + DIRECTORYTESTFILE));
        }
    }
}
