using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlServerMirroring
{
    public enum ServerRole
    {
        NotSet,
        Primary,
        Secondary,
        MainlyPrimary,
        MainlySecondary,
        Neither
    }
}
