using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Timers;

namespace MirrorLib
{
    public class SqlServerInstanceSynchronizeInvoke: ISynchronizeInvoke
    {
        private object SyncObject = new Object();
        private delegate object InvokeDelegate(Delegate method, object[] args);

        public IAsyncResult BeginInvoke(Delegate method, object[] args)
        {
            ElapsedEventHandler handler = (ElapsedEventHandler)method;
            InvokeDelegate D = Invoke;
            return D.BeginInvoke(handler, args, CallbackMethod, null);
        }

        private void CallbackMethod(IAsyncResult ar)
        {
            AsyncResult result = ar as AsyncResult;
            if (result != null)
                ((InvokeDelegate)result.AsyncDelegate).EndInvoke(ar);
        }

        public object EndInvoke(IAsyncResult result)
        {
            result.AsyncWaitHandle.WaitOne();
            return null;
        }

        public object Invoke(Delegate method, object[] args)
        {
            lock (SyncObject)
            {
                ElapsedEventHandler handler = (ElapsedEventHandler)method;
                handler(args[0], (ElapsedEventArgs)args[1]);
                return null;
            }
        }

        public bool InvokeRequired
        {
            get { return true; }
        }
    }
}
