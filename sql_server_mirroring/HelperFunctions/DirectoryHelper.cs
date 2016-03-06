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
        public static bool TestReadWriteAccessToDirectory(string directoryPath)
        {
            throw new NotImplementedException();
        }

        public static bool ValidDirectoryName(string directoryPath)
        {
            Regex regex = new Regex(@"^\w:\\(\w+\\)*(\w+)?$");
            if(regex.IsMatch(directoryPath))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static void CreateLocalDirectoryIfNotExistingAndGiveEveryoneAccess(string directoryPath)
        {
            try
            {
                if(!ValidDirectoryName(directoryPath))
                {
                    throw new DirectoryException(string.Format("DirectoryPath {0} is not valid", directoryPath));
                }
                if (!Directory.Exists(directoryPath))
                {
                    Directory.CreateDirectory(directoryPath);
                }
                DirectoryInfo directoryInfo = new DirectoryInfo(directoryPath);
                DirectorySecurity dSecurity = directoryInfo.GetAccessControl();
                dSecurity.AddAccessRule(new FileSystemAccessRule("Everyone", FileSystemRights.FullControl, InheritanceFlags.ObjectInherit | InheritanceFlags.ContainerInherit, PropagationFlags.NoPropagateInherit, AccessControlType.Allow));
                directoryInfo.SetAccessControl(dSecurity);
                if (directoryInfo == null)
                {
                    throw new DirectoryException(string.Format("Could not set directory access control for {0}", directoryPath));
                }
            }
            catch (Exception ex)
            {
                throw new DirectoryException("Error sharing folders.", ex);
            }
        }
    }
}
