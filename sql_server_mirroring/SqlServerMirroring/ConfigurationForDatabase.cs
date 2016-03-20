using HelperFunctions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace SqlServerMirroring
{
    public class ConfigurationForDatabase
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

        public ConfigurationForDatabase(
            DatabaseName databaseName,
            DirectoryPath localDirectoryForBackup,
            DirectoryPath localDirectoryForShare,
            DirectoryPath localDircetoryForRestore,
            ShareName localShareName,
            RemoteServer remoteServer,
            ShareName remoteShareName,
            SubDirectory localTransferSubDircetory,
            SubDirectory remoteTransferSubDircetory,
            SubDirectory remoteDeliverySubDirectory
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

        public UncPath RemoteLocalTransferDirectory
        {
            get
            {
                return new UncPath(RemoteServer, RemoteShareName, LocalTransferSubDircetory);
            }
        }

        public UncPath RemoteLocalTransferDirectoryWithSubDirectory
        {
            get
            {
                return new UncPath(RemoteServer, RemoteShareName, LocalTransferSubDircetory, new SubDirectory(DatabaseName.ToString()));
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

        public UncPath RemoteRemoteTransferDirectory
        {
            get
            {
                return new UncPath(RemoteServer, RemoteShareName, LocalTransferSubDircetory);
            }
        }

        public UncPath RemoteRemoteTransferDirectoryWithSubDirectory
        {
            get
            {
                return new UncPath(RemoteServer, RemoteShareName, LocalTransferSubDircetory, new SubDirectory(DatabaseName.ToString()));
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

        public UncPath RemoteRemoteDeliveryDirectory
        {
            get
            {
                return new UncPath(RemoteServer, RemoteShareName, RemoteDeliverySubDircetory);
            }
        }

        public UncPath RemoteRemoteDeliveryDirectoryWithSubDirectory
        {
            get
            {
                return new UncPath(RemoteServer, RemoteShareName, RemoteDeliverySubDircetory, new SubDirectory(DatabaseName.ToString()));
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
    }
}
