using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace HelperFunctions
{
    public class ShareName
    {
        private string _shareName;

        public ShareName(string shareName)
        {
            ValidShareName(shareName);
            _shareName = shareName;
        }

        private void ValidShareName(string shareName)
        {
            // name between 1 and 80 characters and not including pipe or mailslot
            Regex regex = new Regex(@"^(?!pipe|mailslot)\w{1,80}$");
            if (!regex.IsMatch(shareName))
            {
                throw new ShareException(string.Format("Sharename {0} does not conform to word between 1 and 80 characters and not \"pipe\" or \"mailslot\"", shareName));
            }
        }

        public override string ToString()
        {
            return _shareName;
        }
    }
}
