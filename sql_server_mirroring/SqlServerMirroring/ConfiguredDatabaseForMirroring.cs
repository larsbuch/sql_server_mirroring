using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlServerMirroring
{
    public class ConfiguredDatabaseForMirroring
    {
        private string _databaseName;
        private string _localDirectoryForBackup;
        private string _localShareForBackup;
        private string _localShareName;
        private string _remoteTempFolderForBackup;
        private string _remoteDeliveryFolderForBackup;
        private string _localDriveForRestore;
        private string _remoteShareForRestore;
        private string _remoteServer;
        private string _remoteShareForBackup;
        private int _endpoint_SslPort;
        private int _endpoint_ListenerPort;

        public ConfiguredDatabaseForMirroring(
            string databaseName, 
            string localDirectoryForBackup,
            string localShareForBackup,
            string localShareName,
            string remoteTempFolderForBackup, 
            string remoteDeliveryFolderForBackup,
            string localDriveForRestore,
            string remoteShareForRestore,
            string remoteServer,
            string remoteShareForBackup,
            int endpoint_SslPort,
            int endpoint_ListenerPort
            )
        {
            _databaseName = databaseName;
            _localDirectoryForBackup = localDirectoryForBackup;
            _localShareForBackup = localShareForBackup;
            _localShareName = localShareName;
            _remoteTempFolderForBackup = remoteTempFolderForBackup;
            _remoteDeliveryFolderForBackup = remoteDeliveryFolderForBackup;
            _localDriveForRestore = localDriveForRestore;
            _remoteShareForRestore = remoteShareForRestore;
            _remoteServer = remoteServer;
            _remoteShareForBackup = remoteShareForBackup;
            _endpoint_SslPort = endpoint_SslPort;
            _endpoint_ListenerPort = endpoint_ListenerPort;
        }

        public string DatabaseName
        {
            get
            {
                return _databaseName;
            }
        }

        public string LocalDirectoryForBackup
        {
            get
            {
                return _localDirectoryForBackup;
            }
        }

        public string LocalShareForBackup
        {
            get
            {
                return _localShareForBackup;
            }
        }

        public string LocalShareName
        {
            get
            {
                return _localShareName;
            }
        }

        public string RemoteTempFolderForBackup
        {
            get
            {
                return _remoteTempFolderForBackup;
            }
        }


        public string RemoteDeliveryFolderForBackup
        {
            get
            {
                return _remoteDeliveryFolderForBackup;
            }
        }

        public string LocalDriveForRestore
        {
            get
            {
                return _localDriveForRestore;
            }
        }

        public string RemoteShareForRestore
        {
            get
            {
                return _remoteShareForRestore;
            }
        }

        public string RemoteServer
        {
            get
            {
                return _remoteServer;
            }
        }

        public string RemoteShareForBackup
        {
            get
            {
                return _remoteShareForBackup;
            }
        }

        public int Endpoint_SslPort
        {
            get
            {
                return _endpoint_SslPort;
            }
        }

        public int Endpoint_ListenerPort
        {
            get
            {
                return _endpoint_ListenerPort;
            }
        }

        public string Endpoint_Name
        {
            get
            {
                return "Mirroring_Endpoint_" + DatabaseName;
            }
        }
    }
}
