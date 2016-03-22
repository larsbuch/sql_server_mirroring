﻿using HelperFunctions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MirrorLib
{
    public class ConfigurationForInstance
    {
        private RemoteServer _remoteServer;
        private DirectoryPath _localBackupDirectory;
        private DirectoryPath _localShareDirectory;
        private DirectoryPath _localRestoreDircetory;
        private ShareName _localShareName;
        private int _endpoint_ListenerPort;
        private double _backupExpiresAfterDays;
        private int _shutDownAfterNumberOfChecksForDatabaseState;
        private int _switchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState;
        private int _shutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState;
        private int _mirrorMonitoringUpdateMinutes;
        private int _remoteServerAccessTimeoutSeconds;
        private string _endpoint_Name;

        public ConfigurationForInstance(
            RemoteServer remoteServer,
            DirectoryPath localDirectoryForBackup,
            DirectoryPath localDirectoryForShare,
            DirectoryPath localDircetoryForRestore,
            ShareName localShareName,
            int endpoint_ListenerPort,
            double backupExpiresAfterDays,
            int shutDownAfterNumberOfChecksForDatabaseState,
            int switchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState,
            int shutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState,
            int mirrorMonitoringUpdateMinutes,
            int remoteServerAccessTimeoutSeconds,
            string endpoint_Name
            )
        {
            _remoteServer = remoteServer;
            _localBackupDirectory = localDirectoryForBackup;
            _localShareDirectory = localDirectoryForShare;
            _localRestoreDircetory = localDircetoryForRestore;
            _localShareName = localShareName;
            _endpoint_ListenerPort = endpoint_ListenerPort;
            _backupExpiresAfterDays = backupExpiresAfterDays;
            _shutDownAfterNumberOfChecksForDatabaseState = shutDownAfterNumberOfChecksForDatabaseState;
            _switchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState = switchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState;
            _shutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState = shutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState;
            MirrorMonitoringUpdateMinutes = mirrorMonitoringUpdateMinutes;
            _remoteServerAccessTimeoutSeconds = remoteServerAccessTimeoutSeconds;
            _endpoint_Name = endpoint_Name;
        }
        public RemoteServer RemoteServer
        {
            get
            {
                return _remoteServer;
            }
        }

        public ShareName LocalShareName
        {
            get
            {
                return _localShareName;
            }
        }

        public DirectoryPath LocalBackupDirectory
        {
            get
            {
                return _localBackupDirectory.Clone();
            }
        }

        public DirectoryPath LocalShareDirectory
        {
            get
            {
                return _localShareDirectory.Clone();
            }
        }

        public DirectoryPath LocalRestoreDirectory
        {
            get
            {
                return _localRestoreDircetory.Clone();
            }
        }

        public int Endpoint_ListenerPort
        {
            get
            {
                return _endpoint_ListenerPort;
            }
        }

        public double BackupExpiresAfterDays
        {
            get
            {
                return _backupExpiresAfterDays;
            }
        }

        public int ShutDownAfterNumberOfChecksForDatabaseState
        {
            get
            {
                return _shutDownAfterNumberOfChecksForDatabaseState;
            }
        }

        public int MirrorMonitoringUpdateMinutes
        {
            get
            {
                return _mirrorMonitoringUpdateMinutes;
            }
            private set
            {
                if (value > 0 && value < 121)
                {
                    _mirrorMonitoringUpdateMinutes = value;
                }
                else
                {
                    throw new SqlServerMirroringException(string.Format("Could not set mirror monitoring update to {0} minutes as it is surposed to be between 1 and 120.", value));
                }
            }
        }

        public int ShutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState
        {
            get
            {
                return _shutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState;
            }
        }

        public int SwitchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState
        {
            get
            {
                return _switchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState;
            }
        }

        public string Endpoint_Name
        {
            get
            {
                return _endpoint_Name;
            }
        }

        public int RemoteServerAccessTimeoutSeconds
        {
            get
            {
                return _remoteServerAccessTimeoutSeconds;
            }
        }

        public string DatabaseBackupSearchPattern
        {
            get
            {
                return "*." + DatabaseBackupFileEnd;
            }
        }

        public string DatabaseBackupFileEnd
        {
            get
            {
                return "bak";
            }
        }

        public string DatabaseLogBackupSearchPattern
        {
            get
            {
                return "*." + DatabaseLogBackupFileEnd;
            }
        }

        public string DatabaseLogBackupFileEnd
        {
            get
            {
                return "log";
            }
        }
    }
}
