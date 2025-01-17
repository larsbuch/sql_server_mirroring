﻿using System;
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
        public static void TestReadWriteAccessToShare(ILogger logger, UncPath uncPath)
        {
            try
            {
                logger.LogInfo(string.Format("Trying to test unc {0}.", uncPath));
                FileCheckHelper.WriteTestFileToDirectory(logger, uncPath);
                FileCheckHelper.ReadTestFileFromDirectoryAndCompare(logger, uncPath);
                FileCheckHelper.DeleteTestFileFromDirectory(logger, uncPath);
                logger.LogInfo(string.Format("Test unc {0} succeeded.", uncPath));
            }
            catch (Exception ex)
            {
                throw new DirectoryException(string.Format("Test write and write fails on unc {0}.", uncPath), ex);
            }
        }

        public static void CreateLocalShareDirectoryIfNotExistingAndGiveEveryoneAccess(ILogger logger, DirectoryPath directoryPath, ShareName shareName)
        {
            CreateLocalShareDirectoryIfNotExisting(logger, directoryPath, shareName, "NT Authority", "Everyone");
        }

        public static void CreateLocalShareDirectoryIfNotExistingAndGiveAuthenticatedUsersAccess(ILogger logger, DirectoryPath directoryPath, ShareName shareName)
        {
            CreateLocalShareDirectoryIfNotExisting(logger, directoryPath, shareName, "NT Authority", "Authenticated Users");
        }

        public static void CreateLocalShareDirectoryIfNotExisting(ILogger logger, DirectoryPath directoryPath, ShareName shareName, string domain, string user)
        {
            try
            {
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveFullControlToUser(logger, directoryPath, domain, user);
                DirectoryHelper.TestReadWriteAccessToDirectory(logger, directoryPath);
                string shareDescription = string.Format("Shared {0} with {1} on {2}", directoryPath, shareName, DateTime.UtcNow.ToLongDateString());
                if (WindowsShare.GetShareByName(shareName.ToString()) == null)
                {
                    logger.LogDebug(string.Format("No share existing. Creating share {0}.", shareName));
                    ShareFolder(logger, directoryPath, shareName, shareDescription);
                    SharePermissions(logger, shareName, domain, user, WindowsShare.AccessMaskTypes.FullControl);
                }
                TestReadWriteAccessToShare(logger, new UncPath(new ServerName(Environment.MachineName), shareName));
            }
            catch (Exception ex)
            {
                throw new ShareException("Error sharing folders.", ex);
            }
        }
        
        private static void ShareFolder(ILogger logger, DirectoryPath directoryPath, ShareName shareName, string shareDescription)
        {
            logger.LogDebug(string.Format("Starts trying to share {0} as {1}", directoryPath, shareName));
            WindowsShare.MethodStatus methodStatus = WindowsShare.Create(directoryPath.PathString, shareName.ToString(), WindowsShare.ShareType.DiskDrive, null, shareDescription, null);
            if (methodStatus != WindowsShare.MethodStatus.Success)
            {
                throw new ShareException(string.Format("Creating share failed for {0} at {1}. Error {2}.", shareName, directoryPath, methodStatus.ToString()));
            }
            logger.LogInfo(string.Format("Share {0} created for {1}.", shareName, directoryPath));
        }

        private static void SharePermissions(ILogger logger, ShareName shareName, string domain, string user, WindowsShare.AccessMaskTypes accessMask)
        {
            logger.LogDebug(string.Format("Trying to set permissions to share {0} for user {1}\\{2}", shareName, domain, user));
            WindowsShare windowsShare = WindowsShare.GetShareByName(shareName.ToString());
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
