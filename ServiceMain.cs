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


        public static String mainConnString = null;
        //private static List<int> listCurrentReplTables = new List<int>();

        private static List<TableThread> listThreads = new List<TableThread>();

        //private Thread checkStateThread;

        private Thread replMainThread;

        private bool stopService = false;

        public ServiceMain()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            logger.Info("Service started "+DateTime.Now);
            stopService = false;

            //checkStateThread = new Thread(doScanWork);
            //checkStateThread.Start();

            replMainThread = new Thread(doReplWork);
            replMainThread.Start();
        }


        private void doReplWork(object arg)
        {

            ReplTable table = null;

            while (true)
            {
                if (stopService) { break; }

                if (listThreads.Count < )

                table = DBConn.getReplicationTable();

                try
                {
                    if (table != null) new ReplTableThread(table);
                }
                catch (Exception ex)
                {
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                }

                Thread.Sleep(5000);

            }


        }


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


        private void doScanWork(object arg)
        {

            MySqlConnection conn = null;
            int interval = 60000;
            List<Station> stations = null;
            String intervalStr = "";
            String error = null;
            bool isOpen = false;
            int curStationId = 0;

            while (true)
            {
                if (stopService) { break; }

                if (stations == null) stations = new List<Station>();
                else stations.Clear();
                MySqlCommand mySqlCommand = null;
                
                try
                {
                    stations = DBConn.getStations(true);                    

                    if (stations != null)
                    {
                        conn = new MySqlConnection(mainConnString);
                        conn.Open();
                        logger.Info("DoScanWork: Подключение к серверу для сохранения результатов сканирования открыто.");
                        
                        foreach (Station station in stations)
                        {
                            curStationId = station.Id;
                            isOpen = Utills.IsPortOpen(station.Host, station.Port, 5000, out error);
                            mySqlCommand = new MySqlCommand("INSERT INTO pauk_kdc.t_scan_results (station_id, result_code, error_message) VALUES ("+ station.Id+ ", "+ isOpen + ", '"+ error + "')", conn);
                            mySqlCommand.ExecuteNonQuery();
                        }
                    }

                }
                catch (Exception ex)
                {
                    logger.Error("DoScanWork: Не удалось созранить данные по результату сканирования (" + curStationId + ", " + isOpen + ", '" + error + "')");
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                }
                finally
                {
                    if (mySqlCommand != null) mySqlCommand.Dispose();
                    if (conn != null) conn.Close();
                }



                try
                {
                    intervalStr = DBConn.getProperty(mainConnString, "ScanInterval");
                    if (!Int32.TryParse(intervalStr, out interval))
                    {
                        interval = 60000;
                        logger.Error("DoScanWork: Не удалось конвертировать значение параметра ScanInterval, будет использоваться значение по умолчанию:" + interval);
                    }
                    else {
                        logger.Info("DoScanWork: Полученное из БД значение параметра ScanInterval:" + interval);
                        
                    }
                }
                catch (Exception ex)
                {
                    logger.Error("DoScanWork: Не удалось получить значение параметра ScanInterval");
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                }

                Thread.Sleep(interval);

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
