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
        public static void TestReadWriteAccessToDirectory(string directoryPath)
        {
            try
            {
                ValidDirectoryName(directoryPath);

                FileCheckHelper.WriteTestFileToDirectory(directoryPath);
                FileCheckHelper.ReadTestFileFromDirectoryAndCompare(directoryPath);
                FileCheckHelper.DeleteTestFileFromDirectory(directoryPath);
            }
            catch (Exception ex)
            {
                throw new DirectoryException(string.Format("Test write and write fails for directory {0}", directoryPath), ex);
            }
        }

        public static void ValidDirectoryName(string directoryPath)
        {
            Uri uri = new Uri(directoryPath);

            if(uri.IsUnc || uri.IsFile)
            {
                throw new DirectoryException(string.Format("DirectoryPath {0} is not valid", directoryPath));
            }
        }

        public static void CreateLocalDirectoryIfNotExistingAndGiveFullControlToEveryone(string directoryPath)
        {
            CreateLocalDirectoryIfNotExistingAndGiveFullControlToUser(directoryPath, "NT Authority", "Everyone");
        }

        public static void CreateLocalDirectoryIfNotExistingAndGiveFullControlToUser(string directoryPath, string domain, string user)
        {
            try
            {
                ValidDirectoryName(directoryPath);
                string account = BuildAccount(domain, user);
                if (!Directory.Exists(directoryPath))
                {
                    Directory.CreateDirectory(directoryPath);
                }
                DirectoryInfo directoryInfo = new DirectoryInfo(directoryPath);
                DirectorySecurity dSecurity = directoryInfo.GetAccessControl();
                dSecurity.AddAccessRule(new FileSystemAccessRule(account, FileSystemRights.FullControl, InheritanceFlags.ObjectInherit | InheritanceFlags.ContainerInherit, PropagationFlags.NoPropagateInherit, AccessControlType.Allow));
                directoryInfo.SetAccessControl(dSecurity);
                if (directoryInfo == null)
                {
                    throw new DirectoryException(string.Format("Could not set directory access control for {0} to {1}", directoryPath, account));
                }
            }
            catch (Exception ex)
            {
                throw new DirectoryException("Error sharing folders.", ex);
            }
        }

        private static string BuildAccount(string domain, string user)
        {
            if (user.Equals("Everyone"))
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
