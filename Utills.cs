using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ReplicationWinService
{

    class Utills
    {

        private static ILog logger = LogManager.GetLogger(typeof(Utills));

        public static bool IsPortOpen(string host, int port, int timeout, out string error)
        {
            error = "";
            try
            {
                using (var client = new System.Net.Sockets.TcpClient())
                {
                    var result = client.BeginConnect(host, port, null, null);
                    var success = result.AsyncWaitHandle.WaitOne(timeout);
                    if (!success)
                    {
                        return false;
                    }

                    client.EndConnect(result);
                }

            }
            catch (Exception ex)
            {
                error = ex.Message;
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
                return false;
            }
            return true;
        }


    }
}
