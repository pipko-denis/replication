using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using log4net;
using MySql.Data.MySqlClient;
using ReplicationWinService.model;

namespace ReplicationWinService
{
    public partial class ServiceMain : ServiceBase
    {

        private static ILog logger = LogManager.GetLogger("ServiceMain");

        private static int maxReplThreads = 5;

        public static String mainConnString = null;

        private static List<TableThread> listThreads = new List<TableThread>();

        private Thread replMainThread;

        public static bool stopService = false;

        public ServiceMain()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            logger.Info("Service started "+DateTime.Now);
            stopService = false;

            replMainThread = new Thread(doReplWork);
            replMainThread.Start();
        }


        private void doReplWork(object arg)
        {

            ReplTableExt table = null;

            while (true)
            {
                if (stopService) { break; }

                Thread.Sleep(10000);

                if (listThreads.Count >= maxReplThreads) {
                    continue;
                }

                table = DBConn.getReplicationTableExt();

                try
                {
                    if (table != null) new TableThread(table);
                }
                catch (Exception ex)
                {
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                }

            }


        }

        [Obsolete("Depricated")]
        private void doReplWorkOld(object arg)
        {

            ReplTable table = null;

            while (true){
                if (stopService) { break; }

                table = DBConn.getReplicationTable();

                try {
                    if (table != null) new ReplTableThread(table);
                }catch (Exception ex) {
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                }

                Thread.Sleep(5000);

            }


        }


        protected override void OnStop()
        {
            stopService = true;

            DBConn.saveParamsOnServiceStop();

            logger.Info("Service stopped");
        }
    }
}
