using ConsoleTester;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerMirroringTester
{
    class Program
    {
        static void Main(string[] args)
        {
            ConsoleTest.AddTest("SQL Server Connection", "Try to connect to server with SMO", ()=>TestConnectToSMO());

            ConsoleTest.Run();
        }

        private static void TestConnectToSMO()
        {

        }
    }
}
