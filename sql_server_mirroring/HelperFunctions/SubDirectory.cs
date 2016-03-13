using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace HelperFunctions
{
    public class SubDirectory
    {
        private string _subDirectoryName;

        public SubDirectory(string subDirectoryName)
        {
            ValidateSubDirectory(subDirectoryName);
            _subDirectoryName = subDirectoryName;
        }

        private void ValidateSubDirectory(string subDirectoryName)
        {
            Regex regex = new Regex(@"^[\w_]+$");
            if(!regex.IsMatch(subDirectoryName))
                {
                throw new DirectoryException(string.Format("The sub-directory name {0} is not valid.", subDirectoryName));
            }
        }

        public override string ToString()
        {
            return _subDirectoryName;
        }
    }
}
