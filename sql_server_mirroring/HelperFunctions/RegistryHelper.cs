using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Permissions;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace HelperFunctions
{
    public static class RegistryHelper
    {
        private static Regex _validRegistryKeyRegex;

        private static Regex ValidRegistryKeyRegex
        {
            get
            {
                if(_validRegistryKeyRegex == null)
                {
                    _validRegistryKeyRegex = new Regex(@"^(HKEY_CURRENT_USER|HKEY_LOCAL_MACHINE|HKEY_CLASSES_ROOT|HKEY_USERS|HKEY_PERFORMANCE_DATA|HKEY_CURRENT_CONFIG|HKEY_DYN_DATA)\\\w+\\[\w\\]+$", RegexOptions.Compiled);
                }
                return _validRegistryKeyRegex;
            }
        }

        public static object GetRegistryValue(string regKey, string regValue, out RegistryValueKind registryValueKind)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if(registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            registryValueKind = registryKey.GetValueKind(regValue);
            object returnObject = registryKey.GetValue(regValue, null);
            if(returnObject == null)
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} does not contain anything", regKey, regValue));
            }
            return returnObject;
        }

        private static RegistryKey GetRegistryKey(string regKey)
        {
            string[] baseRegistryList = regKey.Split(new char[] { '\\' }, 1);
            if (baseRegistryList.Length == 2)
            {
                string baseRegistry = baseRegistryList[0];
                switch (baseRegistry)
                {
                    case "HKEY_CURRENT_USER":
                        return Registry.CurrentUser.OpenSubKey(baseRegistryList[1]);
                    case "HKEY_LOCAL_MACHINE":
                        return Registry.LocalMachine.OpenSubKey(baseRegistryList[1]);
                    case "HKEY_CLASSES_ROOT":
                        return Registry.ClassesRoot.OpenSubKey(baseRegistryList[1]);
                    case "HKEY_USERS":
                        return Registry.Users.OpenSubKey(baseRegistryList[1]);
                    case "HKEY_PERFORMANCE_DATA":
                        return Registry.PerformanceData.OpenSubKey(baseRegistryList[1]);
                    case "HKEY_CURRENT_CONFIG":
                        return Registry.CurrentConfig.OpenSubKey(baseRegistryList[1]);
                    default:
                        throw new RegistryException(string.Format("Unknown base registry {0}", baseRegistryList[0]));
                }
            }
            else
            {
                return null;
            }
        }

        public static bool Exists(string regKey, string regValue)
        {
            ValidRegistryKey(regKey);
            regKey = Correct64Or32bit(regKey);
            if (Registry.GetValue(regKey, regValue, null) == null)
            {
                //code if key Not Exist
                return false;
            }
            else
            {
                //code if key Exist
                return true;
            }
        }

        public static bool HasReadWriteAccess(string regKey, string regValue)
        {
            ValidRegistryKey(regKey);
            regKey = Correct64Or32bit(regKey);
            try
            {
                RegistryPermission perm1 = new RegistryPermission(RegistryPermissionAccess.Write, regKey + "\\" + regValue);
                perm1.Demand();
                return true;
            }
            catch (System.Security.SecurityException)
            {
                return false;
            }
        }

        public static void ValidRegistryKey(string regKey)
        {
            if (!ValidRegistryKeyRegex.IsMatch(regKey))
            {
                throw new RegistryException(string.Format("Registry key {0} is not in valid format. Regex check |{1}|.", regKey, ValidRegistryKeyRegex.ToString()));
            }
        }

        public static string Correct64Or32bit(string regKey)
        {
            if(Environment.Is64BitOperatingSystem)
            {
                if(Environment.Is64BitProcess)
                {
                    //same so assume everything is peacy
                    return regKey;
                }
                else
                {
                    // 32bit on 64bit so assume WOW correction needed
                    if(RegKeyIsNonWOW(regKey))
                    {
                        throw new RegistryException(string.Format("Registry key {0} does not have Wow6432Node. Please correct with Wow6432Node fx. HKEY_LOCAL_MACHINE\\SOFTWARE\\Wow6432Node\\Microsoft\\Microsoft SQL Server", regKey));
                    }
                    else
                    {
                        return regKey;
                    }    
                }
            }
            else
            {
                if(Environment.Is64BitProcess)
                {
                    throw new RegistryException("The code is not tested in 64bit process on 32bit OS");
                }
                else
                {
                    if(RegKeyIsNonWOW(regKey))
                    {
                        return regKey;
                    }
                    else
                    {
                        throw new RegistryException(string.Format("Registry key {0} does have Wow6432Node but runs on 32bit OS. Please correct by removing Wow6432Node fx. HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Microsoft SQL Server", regKey));
                    }
                }
            }
        }

        private static bool RegKeyIsNonWOW(string regKey)
        {
            if(regKey.ToLower().Contains("Wow6432Node".ToLower()))
            {
                return false;
            }
            else
            {
                return true;
            }
        }

    }
}
