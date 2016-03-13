using HelperFunctions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace SqlServerMirroring
{
    public class ConfiguredDatabaseForMirroring
    {
        private DatabaseName _databaseName;
        private DirectoryPath _localDirectoryForBackup;
        private DirectoryPath _localShareForBackup;
        private DirectoryPath _localDircetoryForRestore;
        private ShareName _localShareName;
        private RemoteServer _remoteServer;
        private ShareName _remoteShareName;
        private SubDirectory _remoteTransferSubDircetory;
        private SubDirectory _remoteDeliverySubDirectory;
        private int _endpoint_SslPort;
        private int _endpoint_ListenerPort;
        private double _backupExpirationTime;

        public ConfiguredDatabaseForMirroring(
            DatabaseName databaseName,
            DirectoryPath localDirectoryForBackup,
            DirectoryPath localShareForBackup,
            DirectoryPath localDircetoryForRestore,
            ShareName localShareName,
            RemoteServer remoteServer,
            ShareName remoteShareName,
            SubDirectory remoteTransferSubDircetory,
            SubDirectory remoteDeliverySubDirectory,
            int endpoint_SslPort,
            int endpoint_ListenerPort,
            double backupExpirationTime
            )
        {
            _databaseName = databaseName;
            _localDirectoryForBackup = localDirectoryForBackup;
            _localShareForBackup = localShareForBackup;
            _localDircetoryForRestore = localDircetoryForRestore;
            _localShareName = localShareName;
            _remoteServer = remoteServer;
            _remoteShareName = remoteShareName;
            _remoteTransferSubDircetory = remoteTransferSubDircetory;
            _remoteDeliverySubDirectory = remoteDeliverySubDirectory;
            _endpoint_SslPort = endpoint_SslPort;
            _endpoint_ListenerPort = endpoint_ListenerPort;
            _backupExpirationTime = backupExpirationTime;
        }

        public DatabaseName DatabaseName
        {
            get
            {
                return _databaseName;
            }
        }

        public Path LocalDirectoryForBackup
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
        //uri = new Uri("\\\\" + serverName + "\\" + shareName + "\\");

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

        public double BackupExpirationTime
        {
            get
            {
                return _backupExpirationTime;
            }
        }
    }
}
