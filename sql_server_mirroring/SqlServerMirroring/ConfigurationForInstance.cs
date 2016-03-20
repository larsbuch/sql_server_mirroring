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
        private int _endpoint_ListenerPort;
        private double _backupExpiresAfterDays;
        private int _shutDownAfterNumberOfChecksForDatabaseState;
        private int _switchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState;
        private int _shutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState;
        private int _mirrorMonitoringUpdateMinutes;

        public ConfigurationForInstance(
            RemoteServer remoteServer,
            int endpoint_ListenerPort,
            double backupExpiresAfterDays,
            int shutDownAfterNumberOfChecksForDatabaseState,
            int switchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState,
            int shutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState,
            int mirrorMonitoringUpdateMinutes
            )
        {
            _remoteServer = remoteServer;
            _endpoint_ListenerPort = endpoint_ListenerPort;
            _backupExpiresAfterDays = backupExpiresAfterDays;
            _shutDownAfterNumberOfChecksForDatabaseState = shutDownAfterNumberOfChecksForDatabaseState;
            _switchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState = switchStateAfterNumberOfChecksInPrimaryRunningNoSecondaryState;
            _shutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState = shutDownAfterNumberOfChecksInSecondaryRunningNoPrimaryState;
            MirrorMonitoringUpdateMinutes = mirrorMonitoringUpdateMinutes;
        }
        public RemoteServer RemoteServer
        {
            get
            {
                return _remoteServer;
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
                return "Mirroring_Endpoint";
            }
        }
    }
}
