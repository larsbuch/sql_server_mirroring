using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace HelperFunctions
{
    public static class DirectoryHelper
    {
        public static void TestReadWriteAccessToDirectory(ILogger logger, DirectoryPath directoryPath)
        {
            try
            {
                FileCheckHelper.WriteTestFileToDirectory(logger, directoryPath);
                FileCheckHelper.ReadTestFileFromDirectoryAndCompare(logger, directoryPath);
                FileCheckHelper.DeleteTestFileFromDirectory(logger, directoryPath);
            }
            catch(FileCheckException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new DirectoryException(string.Format("Test write and write fails for directory {0}", directoryPath), ex);
            }
        }

        public static void CreateLocalDirectoryIfNotExistingAndGiveFullControlToEveryone(ILogger logger, DirectoryPath directoryPath)
        {
            CreateLocalDirectoryIfNotExistingAndGiveFullControlToUser(logger, directoryPath, "NT Authority", "Everyone");
        }
        public static void CreateLocalDirectoryIfNotExistingAndGiveFullControlToAuthenticatedUsers(ILogger logger, DirectoryPath directoryPath)
        {
            CreateLocalDirectoryIfNotExistingAndGiveFullControlToUser(logger, directoryPath, "NT Authority", "Authenticated Users");
        }
        
        public static void CreateLocalDirectoryIfNotExistingAndGiveFullControlToUser(ILogger logger, DirectoryPath directoryPath, string domain, string user)
        {
            try
            {
                string account = BuildAccount(logger, domain, user);
                if (!directoryPath.Exists)
                {
                    logger.LogDebug(string.Format("Creating directory {0}.", directoryPath));
                    directoryPath.CreateDirectory();
                }
                DirectoryInfo directoryInfo = new DirectoryInfo(directoryPath.PathString);
                DirectorySecurity dSecurity = directoryInfo.GetAccessControl();
                dSecurity.AddAccessRule(new FileSystemAccessRule(account, FileSystemRights.FullControl, InheritanceFlags.ObjectInherit | InheritanceFlags.ContainerInherit, PropagationFlags.NoPropagateInherit, AccessControlType.Allow));
                logger.LogDebug(string.Format("Setting Full control for folder to {0}", account));
                directoryInfo.SetAccessControl(dSecurity);
                if (directoryInfo == null)
                {
                    throw new DirectoryException(string.Format("Could not set directory access control for {0} to {1}", directoryPath, account));
                }
            }
            catch(DirectoryException)
            {
                throw;
            }
            catch (Exception ex)
            {
                string error = string.Format("Unknown error sharing folders.", ex.GetType().ToString());
                throw new DirectoryException(error, ex);
            }
        }

        private static string BuildAccount(ILogger logger, string domain, string user)
        {
            if (user.Equals("Everyone"))
            {
                return user;
            }
            else if (user.Equals("Authenticated Users"))
            {
                return user;
            }
            else
            {
                string domainPart = string.Empty;
                if (!string.IsNullOrWhiteSpace(domain))
                {
                    domainPart = domain + "\\";
                }
                string userPart = user;
                if(string.IsNullOrWhiteSpace(userPart))
                {
                    throw new DirectoryException("User is not valid");
                }
                return domainPart + userPart;
            }
        }
    }
}
