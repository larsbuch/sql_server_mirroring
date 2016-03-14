﻿using HelperFunctions;
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
        private DirectoryPath _localBackupDirectory;
        private DirectoryPath _localShareDirectory;
        private DirectoryPath _localRestoreDircetory;
        private ShareName _localShareName;
        private RemoteServer _remoteServer;
        private ShareName _remoteShareName;
        private SubDirectory _localTransferSubDircetory;
        private SubDirectory _remoteTransferSubDircetory;
        private SubDirectory _remoteDeliverySubDirectory;
        private int _endpoint_SslPort;
        private int _endpoint_ListenerPort;
        private double _backupExpirationTime;

        public ConfiguredDatabaseForMirroring(
            DatabaseName databaseName,
            DirectoryPath localDirectoryForBackup,
            DirectoryPath localDirectoryForShare,
            DirectoryPath localDircetoryForRestore,
            ShareName localShareName,
            RemoteServer remoteServer,
            ShareName remoteShareName,
            SubDirectory localTransferSubDircetory,
            SubDirectory remoteTransferSubDircetory,
            SubDirectory remoteDeliverySubDirectory,
            int endpoint_SslPort,
            int endpoint_ListenerPort,
            double backupExpirationTime
            )
        {
            _databaseName = databaseName;
            _localBackupDirectory = localDirectoryForBackup;
            _localShareDirectory = localDirectoryForShare;
            _localRestoreDircetory = localDircetoryForRestore;
            _localShareName = localShareName;
            _remoteServer = remoteServer;
            _remoteShareName = remoteShareName;
            _localTransferSubDircetory = localTransferSubDircetory;
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

        public DirectoryPath LocalBackupDirectory
        {
            get
            {
                return _localBackupDirectory.Clone();
            }
        }

        public DirectoryPath LocalBackupDirectoryWithSubDirectory
        {
            get
            {
                return (_localBackupDirectory.AddSubDirectory(DatabaseName.ToString()));
            }
        }

        public DirectoryPath LocalShareDirectory
        {
            get
            {
                return _localShareDirectory.Clone();
            }
        }

        public SubDirectory LocalTransferSubDircetory
        {
            get
            {
                return _localTransferSubDircetory;
            }
        }

        public DirectoryPath LocalLocalTransferDirectory
        {
            get
            {
                return _localShareDirectory.AddSubDirectory(LocalTransferSubDircetory.ToString());
            }
        }

        public DirectoryPath LocalLocalTransferDirectoryWithSubDirectory
        {
            get
            {
                return LocalLocalTransferDirectory.AddSubDirectory(DatabaseName.ToString());
            }
        }

        public Uri RemoteLocalTransferDirectory
        {
            get
            {
                return RemoteServer.BuildUri(RemoteShareName, LocalTransferSubDircetory);
            }
        }

        public Uri RemoteLocalTransferDirectoryWithSubDirectory
        {
            get
            {
                return RemoteServer.BuildUri(RemoteShareName, LocalTransferSubDircetory, DatabaseName.ToString());
            }
        }

        public SubDirectory RemoteTransferSubDircetory
        {
            get
            {
                return _remoteTransferSubDircetory;
            }
        }

        public DirectoryPath LocalRemoteTransferDirectory
        {
            get
            {
                return _localShareDirectory.AddSubDirectory(LocalTransferSubDircetory.ToString());
            }
        }

        public DirectoryPath LocalRemoteTransferDirectoryWithSubDirectory
        {
            get
            {
                return LocalRemoteTransferDirectory.AddSubDirectory(DatabaseName.ToString());
            }
        }

        public Uri RemoteRemoteTransferDirectory
        {
            get
            {
                return RemoteServer.BuildUri(RemoteShareName, LocalTransferSubDircetory);
            }
        }

        public Uri RemoteRemoteTransferDirectoryWithSubDirectory
        {
            get
            {
                return RemoteServer.BuildUri(RemoteShareName, LocalTransferSubDircetory,DatabaseName.ToString());
            }
        }

        public SubDirectory RemoteDeliverySubDircetory
        {
            get
            {
                return _remoteDeliverySubDirectory;
            }
        }

        public DirectoryPath LocalRemoteDeliveryDirectory
        {
            get
            {
                return _localShareDirectory.AddSubDirectory(RemoteDeliverySubDircetory.ToString());
            }
        }

        public DirectoryPath LocalRemoteDeliveryDirectoryWithSubDirectory
        {
            get
            {
                return LocalRemoteDeliveryDirectory.AddSubDirectory(DatabaseName.ToString());
            }
        }

        public Uri RemoteRemoteDeliveryDirectory
        {
            get
            {
                return RemoteServer.BuildUri(RemoteShareName,RemoteDeliverySubDircetory);
            }
        }

        public Uri RemoteRemoteDeliveryDirectoryWithSubDirectory
        {
            get
            {
                return RemoteServer.BuildUri(RemoteShareName, RemoteDeliverySubDircetory, DatabaseName.ToString());
            }
        }

        public DirectoryPath LocalRestoreDircetory
        {
            get
            {
                return _localRestoreDircetory.Clone();
            }
        }

        public DirectoryPath LocalRestoreDircetoryWithSubDirectory
        {
            get
            {
                return _localRestoreDircetory.AddSubDirectory(DatabaseName.ToString());
            }
        }

        public ShareName LocalShareName
        {
            get
            {
                return _localShareName;
            }
        }

        //public SubDirectory RemoteTransferSubDirectory
        //{
        //    get
        //    {
        //        return _remoteTransferSubDircetory;
        //    }
        //}

        //public SubDirectory RemoteDeliverySubDirectory
        //{
        //    get
        //    {
        //        return _remoteDeliverySubDirectory;
        //    }
        //}

        //public Uri RemoteTransferDirectory
        //{
        //    get
        //    {
        //        return RemoteServer.BuildUri(RemoteShareName, RemoteTransferSubDirectory);
        //    }
        //}

        //public Uri RemoteDeliveryDirectory
        //{
        //    get
        //    {
        //        return RemoteServer.BuildUri(RemoteShareName, RemoteDeliverySubDirectory);
        //    }
        //}


        public RemoteServer RemoteServer
        {
            get
            {
                return _remoteServer;
            }
        }

        public ShareName RemoteShareName
        {
            get
            {
                return _remoteShareName;
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

        public double BackupExpirationTime
        {
            get
            {
                return _backupExpirationTime;
            }
        }
    }
}