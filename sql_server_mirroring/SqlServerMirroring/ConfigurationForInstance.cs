using HelperFunctions;
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
        private int _serviceStartTimeout;
        private int _checkMirroringStateSecondInterval;
        private int _primaryStartupWaitNumberOfChecksForMirroringTimeout;
        private int _secondaryStartupWaitNumberOfChecksForMirroringTimeout;
        private int _backupHourInterval;
        private bool _backupToMirrorServer;
        private BackupTime _backupTime;
        private int _backupDelayEmergencyBackupMin;

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
            string endpoint_Name,
            int serviceStartTimeoutSec,
            int checkMirroringStateSecondInterval,
            int primaryStartupWaitNumberOfChecksForMirroringTimeout,
            int secondaryStartupWaitNumberOfChecksForMirroringTimeout,
            int backupHourInterval,
            bool backupToMirrorServer,
            BackupTime backupTime,
            int backupDelayEmergencyBackupMin
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
            ServiceStartTimeout = serviceStartTimeoutSec * 1000;
            CheckMirroringStateSecondInterval = checkMirroringStateSecondInterval;
            PrimaryStartupWaitNumberOfChecksForMirroringTimeout = primaryStartupWaitNumberOfChecksForMirroringTimeout;
            SecondaryStartupWaitNumberOfChecksForMirroringTimeout = secondaryStartupWaitNumberOfChecksForMirroringTimeout;
            BackupHourInterval = backupHourInterval;
            _backupToMirrorServer = backupToMirrorServer;
            _backupTime = backupTime;
            _backupDelayEmergencyBackupMin = backupDelayEmergencyBackupMin;
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

        public int ServiceStartTimeout
        {
            get
            {
                return _serviceStartTimeout;
            }
            private set
            {
                if (value > 0)
                {
                    _serviceStartTimeout = value;
                }
                else
                {
                    throw new SqlServerMirroringException(string.Format("Could not set Service Start Timeout update to {0} seconds as it is surposed to be 1 or above.", value));
                }
            }
        }

        public int ServiceStartTimeoutStep
        {
            get
            {
                return ServiceStartTimeout / 20;
            }
        }

        public int CheckMirroringStateSecondInterval
        {
            get
            {
                return _checkMirroringStateSecondInterval;
            }
            private set
            {
                if (value > 0)
                {
                    _checkMirroringStateSecondInterval = value;
                }
                else
                {
                    throw new SqlServerMirroringException(string.Format("Could not set Check Mirroring State Interval to every {0} seconds as it is surposed to be 1 or above.", value));
                }
            }
        }

        public int PrimaryStartupWaitNumberOfChecksForMirroringTimeout
        {
            get
            {
                return _primaryStartupWaitNumberOfChecksForMirroringTimeout;
            }
            private set
            {
                if (value > 0)
                {
                    _primaryStartupWaitNumberOfChecksForMirroringTimeout = value;
                }
                else
                {
                    throw new SqlServerMirroringException(string.Format("Could not set Primary Startup Wait to {0} checks as it is surposed to be 1 or above.", value));
                }
            }
        }

        public int SecondaryStartupWaitNumberOfChecksForMirroringTimeout
        {
            get
            {
                return _secondaryStartupWaitNumberOfChecksForMirroringTimeout;
            }
            private set
            {
                if (value > 0)
                {
                    _secondaryStartupWaitNumberOfChecksForMirroringTimeout = value;
                }
                else
                {
                    throw new SqlServerMirroringException(string.Format("Could not set Secondary Startup Wait to {0} checks as it is surposed to be 1 or above.", value));
                }
            }
        }

        public int BackupHourInterval
        {
            get
            {
                return _backupHourInterval;
            }
            private set
            {
                if (value > 0)
                {
                    _backupHourInterval = value;
                }
                else
                {
                    throw new SqlServerMirroringException(string.Format("Could not set Backup Hour Interval to {0} checks as it is surposed to be 1 or above.", value));
                }
            }
        }
        public bool BackupToMirrorServer
        {
            get
            {
                return _backupToMirrorServer;
            }
        }

        public BackupTime BackupTime
        {
            get
            {
                return _backupTime;
            }
        }

        public int BackupDelayEmergencyBackupMin
        {
            get
            {
                return _backupDelayEmergencyBackupMin;
            }
        }
    }
}
