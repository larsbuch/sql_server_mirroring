using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Management;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;

namespace HelperFunctions
{
    public static class ShareHelper
    {
        public static void TestReadWriteAccessToShare(string serverName, string shareName)
        {
            string uri = string.Empty;
            try
            {
                ValidServerName(serverName);
                ValidShareDirectoryName(shareName);
                uri = "\\\\" + serverName + "\\" + shareName + "\\";
                FileCheckHelper.WriteTestFileToDirectory(uri);
                FileCheckHelper.ReadTestFileFromDirectoryAndCompare(uri);
                FileCheckHelper.DeleteTestFileFromDirectory(uri);
            }
            catch (Exception ex)
            {
                throw new DirectoryException(string.Format("Test write and write fails on {0} for share {1} in combined uri {2}.", serverName, shareName, uri), ex);
            }
        }

        public static void ValidRemotePath(string path)
        {
            try
            {
                Uri uri = new Uri(path);
                if(!uri.IsUnc)
                {
                    throw new ShareException(string.Format("Path is not in Unc format: {0}", path));
                }
            }
            catch(ShareException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new ShareException(string.Format("Path is not a valid Uri: {0}", path), ex);
            }
        }

        public static void ValidServerName(string serverName)
        {
            try
            {
                Uri uri = new Uri("\\" + serverName + "\test");
                if (!uri.IsUnc)
                {
                    throw new ShareException(string.Format("Server name {0} is not valid", serverName));
                }
            }
            catch (ShareException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new ShareException(string.Format("The servername {0} is not valid.", serverName), ex);
            }
        }

        public static void ValidShareDirectoryName(string shareName)
        {
            // name between 1 and 80 characters and not including pipe or mailslot
            Regex regex = new Regex(@"^(?!pipe|mailslot)\w{1,80}$");
            if (!regex.IsMatch(shareName))
            {
                throw new ShareException(string.Format("Sharename {0} does not conform to word between 1 and 80 characters and not \"pipe\" or \"mailslot\"", shareName));
            }
        }

        public static void CreateLocalShareDirectoryIfNotExistingAndGiveEveryoneAccess(string directoryPath, string shareName)
        {
            CreateLocalShareDirectoryIfNotExisting(directoryPath, shareName, "NT Authority", "Everyone");
        }

        public static void CreateLocalShareDirectoryIfNotExisting(string directoryPath, string shareName, string domain, string user)
        {
            try
            {
                ValidShareDirectoryName(shareName);
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToUser(directoryPath, domain, user);
                DirectoryHelper.TestReadWriteAccessToDirectory(directoryPath);
                string shareDescription = string.Format("Shared {0} with {1} on {2}", directoryPath, shareName, DateTime.UtcNow.ToLongDateString());
                if (WindowsShare.GetShareByName(shareName) == null)
                {
                    ShareFolder(directoryPath, shareName, shareDescription);
                }
                SharePermissions(shareName, domain, user, WindowsShare.AccessMaskTypes.FullControl);
                TestReadWriteAccessToShare(Environment.MachineName, shareName);
            }
            catch (Exception ex)
            {
                throw new ShareException("Error sharing folders.", ex);
            }
        }


        private static void ShareFolder(string directoryPath, string shareName, string shareDescription)
        {
            WindowsShare.MethodStatus methodStatus = WindowsShare.Create(directoryPath, shareName, WindowsShare.ShareType.DiskDrive, null, shareDescription, null);
            if (methodStatus != WindowsShare.MethodStatus.Success)
            {
                throw new ShareException(string.Format("Creating share failed for {1} at {0}. Error {2}.", directoryPath, shareName, methodStatus.ToString()));
            }
        }

        private static void SharePermissions(string shareName, string domain, string user, WindowsShare.AccessMaskTypes accessMask)
        {
            WindowsShare windowsShare = WindowsShare.GetShareByName(shareName);
            if (windowsShare == null)
            {
                throw new ShareException(string.Format("Could not find share {0}.", shareName));
            }
            WindowsShare.MethodStatus methodStatus = windowsShare.SetPermission(domain, user, accessMask);
            if (methodStatus != WindowsShare.MethodStatus.Success)
            {
                throw new ShareException(string.Format("Could not set AccessMask {0} for user {1}\\{2} on share {3}", accessMask.ToString(), domain, user, shareName));
            }
        }
    }
}
