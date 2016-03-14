using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HelperFunctions
{
    public class RemoteServer
    {
        private string _remoteServerName;
        public RemoteServer(string remoteServerName)
        {
            ValidServerName(remoteServerName);
            _remoteServerName = remoteServerName;
        }


        private void ValidServerName(string remoteServerName)
        {
            try
            {
                Uri uri = new Uri("\\\\" + remoteServerName + "\\test");
                if (!uri.IsUnc)
                {
                    throw new ShareException(string.Format("Remote server name {0} is not valid", remoteServerName));
                }
            }
            catch (ShareException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new ShareException(string.Format("Remote server name {0} is not valid.", remoteServerName), ex);
            }
        }
        public Uri BuildUri(ShareName remoteShareName)
        {
            Uri uri = new Uri("\\\\" + _remoteServerName + "\\" + remoteShareName.ToString());
            return uri;
        }

        public Uri BuildUri(ShareName remoteShareName, SubDirectory subDirectory)
        {
            Uri uri = new Uri("\\\\" + _remoteServerName + "\\" + remoteShareName.ToString() + "\\" + subDirectory.ToString());
            return uri;
        }

        public Uri BuildUri(ShareName remoteShareName, SubDirectory subDirectory, string subSubDirectory)
        {
            Uri uri = new Uri("\\\\" + _remoteServerName + "\\" + remoteShareName.ToString() + "\\" + subDirectory.ToString() + "\\" + subSubDirectory);
            return uri;
        }


        public override string ToString()
        {
            return _remoteServerName;
        }
    }
}
