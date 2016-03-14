using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace HelperFunctions
{
    public class DirectoryPath
    {
        string _pathString;
        public DirectoryPath(string pathString)
        {
            ValidDirectoryName(pathString);
            _pathString = pathString;
        }

        #region Properties

        public string PathString
        {
            get
            {
                return _pathString;
            }
        }

        public bool Exists
        {
            get
            {
                return Directory.Exists(_pathString);
            }
        }


        #endregion

        #region Public

        public DirectoryPath AddSubDirectory(string subDirectory)
        {
            return new DirectoryPath( Path.Combine(_pathString, subDirectory));
        }

        public void CreateDirectory()
        {
            Directory.CreateDirectory(_pathString);
        }

        public override string ToString()
        {
            return _pathString;
        }

        public DirectoryPath Clone()
        {
            return new DirectoryPath(_pathString);
        }

        #endregion

        #region Private

        private void ValidDirectoryName(string pathString)
        {
            try
            {
                Path.GetDirectoryName(pathString);
            }
            catch (Exception ex)
            {
                throw new DirectoryException(string.Format("DirectoryPath pathString {0} is not valid directory", pathString), ex);
            }
        }

        #endregion
    }
}
