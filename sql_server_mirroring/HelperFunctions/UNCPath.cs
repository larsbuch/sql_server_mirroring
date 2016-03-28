using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HelperFunctions
{
    public class UncPath
    {
        private ServerName _remoteServer;
        private ShareName _shareName;
        private SubDirectory _subDirectory;
        private SubDirectory _subSubDirectory;

        public UncPath()
        {
        }

        public UncPath(ServerName remoteServer, ShareName shareName)
        {
            _remoteServer = remoteServer;
            _shareName = shareName;
        }

        public UncPath(ServerName remoteServer, ShareName shareName, SubDirectory subDirectory)
        {
            _remoteServer = remoteServer;
            _shareName = shareName;
            _subDirectory = subDirectory;
        }

        public UncPath(ServerName remoteServer, ShareName shareName, SubDirectory subDirectory, SubDirectory subSubDirectory)
        {
            _remoteServer = remoteServer;
            _shareName = shareName;
            _subDirectory = subDirectory;
            _subSubDirectory = subSubDirectory;
        }



        public string BuildUncPath()
        {
            if(RemoteServer == null || ShareName == null)
            {
                throw new ShareException(string.Format("Cannot build Unc path as either server {0} or share {1} is not set", _remoteServer, _shareName));
            }
            string returnValue = "\\\\" + RemoteServer.ToString() + "\\" + ShareName.ToString();
            if (SubDirectory != null)
            {
                returnValue += "\\" + SubDirectory.ToString();
            }
            if (SubSubDirectory != null)
            {
                returnValue += "\\" + SubSubDirectory.ToString();
            }
            return returnValue;
        }

        public override string ToString()
        {
            return BuildUncPath();
        }

        public UncPath Clone()
        {
            return new UncPath(RemoteServer, ShareName, SubDirectory, SubSubDirectory);
        }

        public ServerName RemoteServer
        {
            get
            {
                return _remoteServer;
            }
            set
            {
                _remoteServer = value;
            }
        }

        public ShareName ShareName
        {
            get
            {
                return _shareName;
            }
            set
            {
                _shareName = value;
            }
        }

        public SubDirectory SubDirectory
        {
            get
            {
                return _subDirectory;
            }
            set
            {
                _subDirectory = value;
            }
        }

        public SubDirectory SubSubDirectory
        {
            get
            {
                return _subSubDirectory;
            }
            set
            {
                _subSubDirectory = value;
            }
        }
    }
}
