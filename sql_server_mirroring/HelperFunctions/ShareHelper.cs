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
        public static bool TestReadWriteAccessToLocalShare(string shareName)
        {
            throw new NotImplementedException();
        }

        public static bool TestReadWriteAccessToRemoteShare(string serverName, string shareName)
        {
            throw new NotImplementedException();
        }

        public static bool ValidShareDirectoryName(string shareName)
        {
            Regex regex = new Regex(@"^\w:\\(\w+\\)*(\w+)?$");
            if (regex.IsMatch(shareName))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static void CreateLocalShareDirectoryIfNotExistingAndGiveEveryoneAccess(string directoryPath, string shareName)
        {
            try
            {
                DirectoryHelper.CreateLocalDirectoryIfNotExistingAndGiveEveryoneAccess(directoryPath);
                if(!DirectoryHelper.TestReadWriteAccessToDirectory(directoryPath))
                {
                    throw new ShareException(string.Format("Read/write test for directory {0} failed for some reason.", directoryPath));
                }
                if(!ValidShareDirectoryName(shareName))
                {
                    throw new ShareException(string.Format("ShareName {0} is not in valid form", shareName));
                }
                QshareFolder(directoryPath, shareName, string.Format("Shared {0} on {1} for everyone",DateTime.UtcNow.ToLongDateString()));
                if(!TestReadWriteAccessToLocalShare(shareName))
                {
                    throw new ShareException(string.Format("Read/write test of share {0} failed for some reason", shareName));
                }
            }
            catch (Exception ex)
            {
                throw new ShareException("Error sharing folders.", ex);
            }
        }

        /*
                 * This method is used to perform the main actions of sharing the folders
                 * It accepts three arguments: -
                 * A path of the folder,
                 * A ShareName by which you would want to share the folder
                 * Description of the folder
                 * You cannot have the first two arguments as empty. They should consist of
                 * data. The third arguments can be an empty string.
                 */
        private static void QshareFolder(string FolderPath, string ShareName, string Description)
        {
            try
            {
                // Create a ManagementClass object
                ManagementClass managementClass = new ManagementClass("Win32_Share");

                // Create ManagementBaseObjects for in and out parameters
                ManagementBaseObject inParams = managementClass.GetMethodParameters("Create");
                ManagementBaseObject outParams;

                // Set the input parameters
                inParams["Description"] = Description;
                inParams["Name"] = ShareName;
                inParams["Path"] = FolderPath;
                inParams["Type"] = 0x0; // Disk Drive
                //Another Type:
                //        DISK_DRIVE = 0x0
                //        PRINT_QUEUE = 0x1
                //        DEVICE = 0x2
                //        IPC = 0x3
                //        DISK_DRIVE_ADMIN = 0x80000000
                //        PRINT_QUEUE_ADMIN = 0x80000001
                //        DEVICE_ADMIN = 0x80000002
                //        IPC_ADMIN = 0x8000003
                inParams["MaximumAllowed"] = null;
                inParams["Password"] = null;
                inParams["Access"] = null; // Make Everyone has full control access.                
                //inParams["MaximumAllowed"] = int maxConnectionsNum;

                // Invoke the method on the ManagementClass object
                outParams = managementClass.InvokeMethod("Create", inParams, null);
                // Check to see if the method invocation was successful
                if ((uint)(outParams.Properties["ReturnValue"].Value) != 0)
                {
                    throw new ShareException("Error sharing MS folders.");
                }

                //user selection
                NTAccount ntAccount = new NTAccount("Everyone");

                //SID
                SecurityIdentifier userSID = (SecurityIdentifier)ntAccount.Translate(typeof(SecurityIdentifier));
                byte[] utenteSIDArray = new byte[userSID.BinaryLength];
                userSID.GetBinaryForm(utenteSIDArray, 0);

                //Trustee
                ManagementObject userTrustee = new ManagementClass(new ManagementPath("Win32_Trustee"), null);
                userTrustee["Name"] = "Everyone";
                userTrustee["SID"] = utenteSIDArray;

                //ACE
                ManagementObject userACE = new ManagementClass(new ManagementPath("Win32_Ace"), null);
                userACE["AccessMask"] = 2032127;                                 //Full access
                userACE["AceFlags"] = AceFlags.ObjectInherit | AceFlags.ContainerInherit;
                userACE["AceType"] = AceType.AccessAllowed;
                userACE["Trustee"] = userTrustee;

                ManagementObject userSecurityDescriptor = new ManagementClass(new ManagementPath("Win32_SecurityDescriptor"), null);
                userSecurityDescriptor["ControlFlags"] = 4; //SE_DACL_PRESENT
                userSecurityDescriptor["DACL"] = new object[] { userACE };
                //can declare share either way, where "ShareName" is the name used to share the folder
                //ManagementPath path = new ManagementPath("Win32_Share.Name='" + ShareName + "'");
                //ManagementObject share = new ManagementObject(path);
                ManagementObject share = new ManagementObject(managementClass.Path + ".Name='" + ShareName + "'");

                share.InvokeMethod("SetShareInfo", new object[] { Int32.MaxValue, Description, userSecurityDescriptor });

            }
            catch (Exception ex)
            {
                throw new ShareException("Error sharing folders", ex);
            }
        }
    }
}
