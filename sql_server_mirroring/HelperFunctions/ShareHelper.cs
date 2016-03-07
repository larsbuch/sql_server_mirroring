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
        public static void TestReadWriteAccessToShare(ILogger logger, string serverName, string shareName)
        {
            string uri = string.Empty;
            try
            {
                ValidServerName(logger, serverName);
                ValidShareDirectoryName(logger, shareName);
                uri = "\\\\" + serverName + "\\" + shareName + "\\";
                logger.LogInfo(string.Format("Trying to test uri {0}.", uri));
                FileCheckHelper.WriteTestFileToDirectory(logger, uri);
                FileCheckHelper.ReadTestFileFromDirectoryAndCompare(logger, uri);
                FileCheckHelper.DeleteTestFileFromDirectory(logger, uri);
            }
            catch (Exception ex)
            {
                throw new DirectoryException(string.Format("Test write and write fails on {0} for share {1} in combined uri {2}.", serverName, shareName, uri), ex);
            }
        }

        public static void ValidRemotePath(ILogger logger, string path)
        {
            try
            {
                Uri uri = new Uri(path);
                if(!uri.IsUnc)
                {
                    throw new ShareException(string.Format("Path is not in Unc format: {0}", path));
                }
                logger.LogDebug(string.Format("Path {0} seems to be valid uri.", path));
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

        public static void ValidServerName(ILogger logger, string serverName)
        {
            try
            {
                Uri uri = new Uri("\\" + serverName + "\test");
                if (!uri.IsUnc)
                {
                    throw new ShareException(string.Format("Server name {0} is not valid", serverName));
                }
                logger.LogDebug(string.Format("Server name {0} in valid format.", serverName));
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

        public static void ValidShareDirectoryName(ILogger logger, string shareName)
        {
            // name between 1 and 80 characters and not including pipe or mailslot
            Regex regex = new Regex(@"^(?!pipe|mailslot)\w{1,80}$");
            if (!regex.IsMatch(shareName))
            {
                throw new ShareException(string.Format("Sharename {0} does not conform to word between 1 and 80 characters and not \"pipe\" or \"mailslot\"", shareName));
            }
            logger.LogDebug(string.Format("Share name {0} is valid. ", shareName));
        }

        public static void CreateLocalShareDirectoryIfNotExistingAndGiveEveryoneAccess(ILogger logger, string directoryPath, string shareName)
        {
            CreateLocalShareDirectoryIfNotExisting(logger, directoryPath, shareName, "NT Authority", "Everyone");
        }

        public static void CreateLocalShareDirectoryIfNotExisting(ILogger logger, string directoryPath, string shareName, string domain, string user)
        {
            try
            {
                ValidShareDirectoryName(logger, shareName);
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToUser(logger, directoryPath, domain, user);
                DirectoryHelper.TestReadWriteAccessToDirectory(logger, directoryPath);
                string shareDescription = string.Format("Shared {0} with {1} on {2}", directoryPath, shareName, DateTime.UtcNow.ToLongDateString());
                if (WindowsShare.GetShareByName(shareName) == null)
                {
                    ShareFolder(logger, directoryPath, shareName, shareDescription);
                }
                SharePermissions(logger, shareName, domain, user, WindowsShare.AccessMaskTypes.FullControl);
                TestReadWriteAccessToShare(logger, Environment.MachineName, shareName);
            }
            catch (Exception ex)
            {
                throw new ShareException("Error sharing folders.", ex);
            }
        }
        
        private static void ShareFolder(ILogger logger, string directoryPath, string shareName, string shareDescription)
        {
            WindowsShare.MethodStatus methodStatus = WindowsShare.Create(directoryPath, shareName, WindowsShare.ShareType.DiskDrive, null, shareDescription, null);
            if (methodStatus != WindowsShare.MethodStatus.Success)
            {
                throw new ShareException(string.Format("Creating share failed for {0} at {1}. Error {2}.", shareName, directoryPath, methodStatus.ToString()));
            }
            logger.LogInfo(string.Format("Share {0} created for {1}.", shareName, directoryPath));
        }

        private static void SharePermissions(ILogger logger, string shareName, string domain, string user, WindowsShare.AccessMaskTypes accessMask)
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
            logger.LogInfo(string.Format("Share permissings set for {0}.", shareName));
        }
    }
}
