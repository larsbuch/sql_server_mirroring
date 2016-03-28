using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HelperFunctions
{
    public class ServerName
    {
        private string _serverName;
        public ServerName(string serverName)
        {
            ValidServerName(serverName);
            _serverName = serverName;
        }


        private void ValidServerName(string serverName)
        {
            try
            {
                Uri uri = new Uri("\\\\" + serverName + "\\test");
                if (!uri.IsUnc)
                {
                    throw new ShareException(string.Format("Server name {0} is not valid", serverName));
                }
            }
            catch (ShareException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new ShareException(string.Format("Server name {0} is not valid.", serverName), ex);
            }
        }

        public override string ToString()
        {
            return _serverName;
        }
    }
}
