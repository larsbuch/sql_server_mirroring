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

        public static RegistryValueKind GetRegistryValueKind(ILogger logger, string regKey, string regValue)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            RegistryValueKind registryValueKind = registryKey.GetValueKind(regValue);
            logger.LogDebug(string.Format("Registry value {0}/{1} has kind {2}", regKey, regValue, registryValueKind.ToString()));
            return registryValueKind;
        }

        public static byte[] GetRegistryValue_Binary(ILogger logger, string regKey, string regValue)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.Binary)
            {
                byte[] bytes = (byte[])registryKey.GetValue(regValue, null);
                if (bytes == null)
                {
                    throw new RegistryException(string.Format("Registry key {0} value {1} does not contain anything", regKey, regValue));
                }
                return bytes;
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not Binary", regKey, regValue));
            }
        }

        public static UInt32 GetRegistryValue_DWord(ILogger logger, string regKey, string regValue)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.DWord)
            {
                UInt32 returnObject = (UInt32)registryKey.GetValue(regValue, UInt32.MaxValue);
                if (returnObject == UInt32.MaxValue)
                {
                    throw new RegistryException(string.Format("Registry key {0} value {1} does not contain anything", regKey, regValue));
                }
                return returnObject;
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not DWord", regKey, regValue));
            }
        }

        public static string GetRegistryValue_ExpandString(ILogger logger, string regKey, string regValue)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.ExpandString)
            {
                string returnObject = (string)registryKey.GetValue(regValue, null);
                if (returnObject == null)
                {
                    throw new RegistryException(string.Format("Registry key {0} value {1} does not contain anything", regKey, regValue));
                }
                return returnObject;
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not ExpandString", regKey, regValue));
            }
        }

        public static string[] GetRegistryValue_MultiString(ILogger logger, string regKey, string regValue)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.MultiString)
            {
                string[] values = (string[])registryKey.GetValue(regValue, null);
                if (values == null)
                {
                    throw new RegistryException(string.Format("Registry key {0} value {1} does not contain anything", regKey, regValue));
                }
                return values;
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not MultiString", regKey, regValue));
            }
        }

        public static UInt64 GetRegistryValue_QWord(ILogger logger, string regKey, string regValue)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.QWord)
            {
                UInt64 returnObject = (UInt64)registryKey.GetValue(regValue, UInt64.MaxValue);
                if (returnObject == UInt64.MaxValue)
                {
                    throw new RegistryException(string.Format("Registry key {0} value {1} does not contain anything", regKey, regValue));
                }
                return returnObject;
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not QWord", regKey, regValue));
            }
        }

        public static string GetRegistryValue_String(ILogger logger, string regKey, string regValue)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.String)
            {
                string returnObject = (string)registryKey.GetValue(regValue, null);
                if (returnObject == null)
                {
                    throw new RegistryException(string.Format("Registry key {0} value {1} does not contain anything", regKey, regValue));
                }
                return returnObject;
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not String", regKey, regValue));
            }
        }

        public static void SetRegistryValue_Binary(ILogger logger, string regKey, string regValue, byte[] value)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.Binary)
            {
                registryKey.SetValue(regValue,value, RegistryValueKind.Binary);
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not Binary", regKey, regValue));
            }
        }

        public static void SetRegistryValue_DWord(ILogger logger, string regKey, string regValue, UInt32 value)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.DWord)
            {
                registryKey.SetValue(regValue,value, RegistryValueKind.DWord);
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not DWord", regKey, regValue));
            }
        }

        public static void SetRegistryValue_ExpandString(ILogger logger, string regKey, string regValue, string value)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.ExpandString)
            {
                registryKey.SetValue(regValue,value, RegistryValueKind.ExpandString);
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not ExpandString", regKey, regValue));
            }
        }

        public static void SetRegistryValue_MultiString(ILogger logger, string regKey, string regValue, string[] value)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.MultiString)
            {
                registryKey.SetValue(regValue, value, RegistryValueKind.MultiString);
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not MultiString", regKey, regValue));
            }
        }

        public static void SetRegistryValue_QWord(ILogger logger, string regKey, string regValue, UInt64 value)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.QWord)
            {
                registryKey.SetValue(regValue, value, RegistryValueKind.QWord);
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not QWord", regKey, regValue));
            }
        }

        public static void SetRegistryValue_String(ILogger logger, string regKey, string regValue, string value)
        {
            RegistryKey registryKey = GetRegistryKey(regKey);
            if (registryKey == null)
            {
                throw new RegistryException(string.Format("Registry key {0} could not be found", regKey));
            }
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.String)
            {
                registryKey.SetValue(regValue, value, RegistryValueKind.String);
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not String", regKey, regValue));
            }
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

        public static bool Exists(ILogger logger, string regKey, string regValue)
        {
            ValidRegistryKey(logger, regKey);
            regKey = Correct64Or32bit(logger, regKey);
            if (Registry.GetValue(regKey, regValue, null) == null)
            {
                //code if key Not Exist
                logger.LogDebug(string.Format("Registry key {0}/{1} does not exist.", regKey, regValue));
                return false;
            }
            else
            {
                //code if key Exist
                logger.LogDebug(string.Format("Registry key {0}/{1} does exist.", regKey, regValue));
                return true;
            }
        }

        public static bool HasReadWriteAccess(ILogger logger, string regKey, string regValue)
        {
            ValidRegistryKey(logger, regKey);
            regKey = Correct64Or32bit(logger, regKey);
            try
            {
                RegistryPermission perm1 = new RegistryPermission(RegistryPermissionAccess.Write, regKey + "\\" + regValue);
                perm1.Demand();
                logger.LogInfo(string.Format("Has read/write access to registry key {0}.", regKey + "\\" + regValue));
                return true;
            }
            catch (System.Security.SecurityException)
            {
                logger.LogWarning(string.Format("Not correct read/write access to registry key {0}.", regKey + "\\" + regValue));
                return false;
            }
        }

        public static void ValidRegistryKey(ILogger logger, string regKey)
        {
            if (!ValidRegistryKeyRegex.IsMatch(regKey))
            {
                throw new RegistryException(string.Format("Registry key {0} is not in valid format. Regex check |{1}|.", regKey, ValidRegistryKeyRegex.ToString()));
            }
            logger.LogDebug(string.Format("Registry key {0} seems to be in valid format.", regKey));
        }

        public static string Correct64Or32bit(ILogger logger, string regKey)
        {
            if(Environment.Is64BitOperatingSystem)
            {
                if(Environment.Is64BitProcess)
                {
                    //same so assume everything is peacy
                    logger.LogDebug("64 bit process on 64 bit system.");
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
                        logger.LogDebug("32 bit process on 64 bit system trying for key through Wow6432Node.");
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
                        logger.LogDebug("32 bit process on 32 bit system.");
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
