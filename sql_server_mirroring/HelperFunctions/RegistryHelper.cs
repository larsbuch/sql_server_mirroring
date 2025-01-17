﻿using Microsoft.Win32;
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
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
            RegistryValueKind registryValueKind = registryKey.GetValueKind(regValue);
            logger.LogDebug(string.Format("Registry value {0}/{1} has kind {2}", regKey, regValue, registryValueKind.ToString()));
            return registryValueKind;
        }

        public static byte[] GetRegistryValue_Binary(ILogger logger, string regKey, string regValue)
        {
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
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

        public static UInt32 GetRegistryValue_DWord(ILogger logger, string regKey, string regValue, bool throwExceptionOnNullValue)
        {
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.DWord)
            {
                object registryDWordObject = registryKey.GetValue(regValue, null);
                if (registryDWordObject == null)
                {
                    if (throwExceptionOnNullValue)
                    {
                        throw new RegistryException(string.Format("Registry key {0} value {1} does contain |{2}| which is not a valid DWord", regKey, regValue, registryDWordObject.ToString()));
                    }
                    else
                    {
                        return UInt32.MaxValue;
                    }
                }
                return unchecked((UInt32)((Int32)registryDWordObject));
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not DWord", regKey, regValue));
            }
        }


        public static string GetRegistryValue_ExpandString(ILogger logger, string regKey, string regValue)
        {
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
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
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
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

        public static UInt64 GetRegistryValue_QWord(ILogger logger, string regKey, string regValue, bool throwExceptionOnNullValue)
        {
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
            if (registryKey.GetValueKind(regValue) == RegistryValueKind.QWord)
            {
                object registryDWordObject = registryKey.GetValue(regValue, null);
                if (registryDWordObject == null)
                {
                    if (throwExceptionOnNullValue)
                    {
                        throw new RegistryException(string.Format("Registry key {0} value {1} does contain |{2}| which is not a valid QWord", regKey, regValue, registryDWordObject.ToString()));
                    }
                    else
                    {
                        return UInt64.MaxValue;
                    }
                }
                return unchecked((UInt64)((Int64)registryDWordObject));
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not QWord", regKey, regValue));
            }
        }

        public static string GetRegistryValue_String(ILogger logger, string regKey, string regValue)
        {
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
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
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
            if (!RegistryValueExists(logger, regKey, regValue) || registryKey.GetValueKind(regValue) == RegistryValueKind.Binary)
            {
                registryKey.SetValue(regValue,value, RegistryValueKind.Binary);
                logger.LogDebug(string.Format("Registry value {0} for key {1} created/updated with data {2} as Binary.", regValue, regKey, value));
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not Binary", regKey, regValue));
            }
        }

        public static void SetRegistryValue_DWord(ILogger logger, string regKey, string regValue, UInt32 value)
        {
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
            if (!RegistryValueExists(logger, regKey, regValue) || registryKey.GetValueKind(regValue) == RegistryValueKind.DWord)
            {
                registryKey.SetValue(regValue,value, RegistryValueKind.DWord);
                logger.LogDebug(string.Format("Registry value {0} for key {1} created/updated with data {2} as DWord.", regValue, regKey, value));
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not DWord", regKey, regValue));
            }
        }

        public static void SetRegistryValue_ExpandString(ILogger logger, string regKey, string regValue, string value)
        {
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
            if (!RegistryValueExists(logger, regKey, regValue) || registryKey.GetValueKind(regValue) == RegistryValueKind.ExpandString)
            {
                registryKey.SetValue(regValue,value, RegistryValueKind.ExpandString);
                logger.LogDebug(string.Format("Registry value {0} for key {1} created/updated with data {2} as ExpandString.", regValue, regKey, value));
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not ExpandString", regKey, regValue));
            }
        }

        public static void SetRegistryValue_MultiString(ILogger logger, string regKey, string regValue, string[] value)
        {
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
            if (!RegistryValueExists(logger, regKey, regValue) || registryKey.GetValueKind(regValue) == RegistryValueKind.MultiString)
            {
                registryKey.SetValue(regValue, value, RegistryValueKind.MultiString);
                logger.LogDebug(string.Format("Registry value {0} for key {1} created/updated with data {2} as MultiString.", regValue, regKey, value));
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not MultiString", regKey, regValue));
            }
        }

        public static void SetRegistryValue_QWord(ILogger logger, string regKey, string regValue, UInt64 value)
        {
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
            if (!RegistryValueExists(logger, regKey, regValue) || registryKey.GetValueKind(regValue) == RegistryValueKind.QWord)
            {
                registryKey.SetValue(regValue, value, RegistryValueKind.QWord);
                logger.LogDebug(string.Format("Registry value {0} for key {1} created/updated with data {2} as QWord.", regValue, regKey, value));
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not QWord", regKey, regValue));
            }
        }

        public static void SetRegistryValue_String(ILogger logger, string regKey, string regValue, string value)
        {
            RegistryKey registryKey = GetRegistryKey(logger, regKey, true);
            if (!RegistryValueExists(logger, regKey, regValue) || registryKey.GetValueKind(regValue) == RegistryValueKind.String)
            {
                registryKey.SetValue(regValue, value, RegistryValueKind.String);
                logger.LogDebug(string.Format("Registry value {0} for key {1} created/updated with data {2} as String.", regValue, regKey, value));
            }
            else
            {
                throw new RegistryException(string.Format("Registry key {0} value {1} is not String", regKey, regValue));
            }
        }

        public static void DeleteRegistryValue(ILogger logger, string regKey, string regValue)
        {
            if (RegistryValueExists(logger, regKey, regValue))
            {
                GetRegistryKey(logger, regKey, true).DeleteValue(regValue);
                logger.LogDebug(string.Format("Registry key {0} value {1} deleted", regKey, regValue));
            }
            
        }

        private static RegistryKey GetRegistryKey(ILogger logger, string regKey, bool throwExceptionOnMissing)
        {
            int splitPoint = regKey.IndexOf('\\');
            if (splitPoint > 0)
            {
                string baseRegistry = regKey.Substring(0, splitPoint);
                string subKey = regKey.Substring(splitPoint + 1);
                switch (baseRegistry)
                {
                    case "HKEY_CURRENT_USER":
                        return Registry.CurrentUser.OpenSubKey(subKey, true);
                    case "HKEY_LOCAL_MACHINE":
                        return Registry.LocalMachine.OpenSubKey(subKey, true);
                    case "HKEY_CLASSES_ROOT":
                        return Registry.ClassesRoot.OpenSubKey(subKey, true);
                    case "HKEY_USERS":
                        return Registry.Users.OpenSubKey(subKey, true);
                    case "HKEY_PERFORMANCE_DATA":
                        return Registry.PerformanceData.OpenSubKey(subKey, true);
                    case "HKEY_CURRENT_CONFIG":
                        return Registry.CurrentConfig.OpenSubKey(subKey, true);
                    default:
                        throw new RegistryException(string.Format("Unknown base registry {0}", baseRegistry));
                }
            }
            else
            {
                if (throwExceptionOnMissing)
                {
                    throw new RegistryException(string.Format("Registry key {0} does not exist.", regKey));
                }
                return null;
            }
        }

        public static bool RegistryKeyExists(ILogger logger, string regKey)
        {
            if(GetRegistryKey(logger, regKey, false) == null)
            {
                return false;
            }
            else
            {
                return true;
            }
        }


        public static bool RegistryValueExists(ILogger logger, string regKey, string regValue)
        {
            ValidRegistryKey(logger, regKey);
            regKey = Correct64Or32bit(logger, regKey);
            if (Registry.GetValue(regKey, regValue, null) == null)
            {
                //code if key Not Exist
                logger.LogDebug(string.Format("Registry key {0}\\{1} does not exist.", regKey, regValue));
                return false;
            }
            else
            {
                //code if key Exist
                logger.LogDebug(string.Format("Registry key {0}\\{1} does exist.", regKey, regValue));
                return true;
            }
        }

        public static bool HasRegistryKeyAllAccess(ILogger logger, string regKey, string regValue)
        {
            ValidRegistryKey(logger, regKey);
            regKey = Correct64Or32bit(logger, regKey);
            try
            {
                RegistryPermission perm1 = new RegistryPermission(RegistryPermissionAccess.AllAccess, regKey + "\\" + regValue);
                perm1.Demand();
                logger.LogInfo(string.Format("Has all access to registry key {0}.", regKey + "\\" + regValue));
                return true;
            }
            catch (System.Security.SecurityException)
            {
                logger.LogWarning(string.Format("Not correct all access to registry key {0}.", regKey + "\\" + regValue));
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
