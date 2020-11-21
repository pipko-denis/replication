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

        private static int maxReplThreads = 10;

        public static bool showScripts = false;

        private static int mainThreadSleepMs = 5000;

        private static int checkThreadSleepMs = 600000;

        private static int deltaCheckTime = 15;

        private static List<TableThread> listThreads = new List<TableThread>();

        private Thread replMainThread;

        private Thread checkThread;

        public static bool stopService = false;

        public ServiceMain()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            logger.Info("Service started "+DateTime.Now);
            logger.Info(ConnString.getMainConnectionString());
            stopService = false;

            //На всякий пожарный обнуляем в БД состояние репликации
            DBConn.saveParamsOnServiceStop();

            //Получаем настройки
            if (!Int32.TryParse(System.Configuration.ConfigurationManager.AppSettings.Get("maxReplThreads"), out maxReplThreads))
            {

                maxReplThreads = 10;
            }
            else {
                if (maxReplThreads > 20) maxReplThreads = 20;
            }

            if (!Boolean.TryParse(System.Configuration.ConfigurationManager.AppSettings.Get("showScripts"), out showScripts))
            {
                showScripts = false;
            }

            if (!Int32.TryParse(System.Configuration.ConfigurationManager.AppSettings.Get("mainThreadSleepMs"), out mainThreadSleepMs))
            {
                mainThreadSleepMs = 1000;
            }
            else {
                if (mainThreadSleepMs < 100) mainThreadSleepMs = 100;
            }


            if (!Int32.TryParse(System.Configuration.ConfigurationManager.AppSettings.Get("deltaCheckTime"), out deltaCheckTime))
            {
                deltaCheckTime = 20;
            }
            else
            {
                if (deltaCheckTime < 10) deltaCheckTime = 20;
            }

            if (!Int32.TryParse(System.Configuration.ConfigurationManager.AppSettings.Get("checkThreadSleepMs"), out checkThreadSleepMs))
            {
                checkThreadSleepMs = 600000;
            }
            else
            {
                if (checkThreadSleepMs < 300000) checkThreadSleepMs = 600000;
            }


            logger.Info("maxReplThreads: " + maxReplThreads + "; showScripts: " + showScripts + "; mainThreadSleepMs: " + mainThreadSleepMs);


            replMainThread = new Thread(doReplWork);
            replMainThread.Start();

            checkThread = new Thread(doCheckState);
            checkThread.Start();            
        }


        private void doReplWork(object arg)
        {

            ReplTableExt table = null;     
            
            while (true)
            {
                if (stopService) { break; }

                Thread.Sleep(mainThreadSleepMs);

                //проверяем не пора ли прибить какой-то поток, т.к. он слишком долго работает - начало
                TimeSpan ts;
                DateTime currentTime = DateTime.Now;
                List<TableThread> listDelThreads = null;
                foreach (TableThread thread in listThreads) {
                    ts = currentTime - thread.dateStart;
                    logger.Info("Thread for table " + thread.table.LocalName + ", state " + thread.thread.ThreadState);

                    //Проверки
                    if (ts.TotalMinutes > 3) {
                        logger.Error("Stopping thread for table (3 minutes timeout)" + thread.table.LocalName + ", started at " + thread.dateStart);
                        if (listDelThreads == null) listDelThreads = new List<TableThread>();
                        listDelThreads.Add(thread);
                    } else if (thread.thread.ThreadState == System.Threading.ThreadState.Stopped) {
                        logger.Info("Stopping thread for table " + thread.table.LocalName + ", because  it is not alive");
                        if (listDelThreads == null) listDelThreads = new List<TableThread>();
                        listDelThreads.Add(thread);                        
                    }


                }

                //Удаляем потоки из списка текущих потоков
                if ( (listDelThreads != null) && (listThreads != null) ) {
                    listThreads = listThreads.Except(listDelThreads).ToList();
                    foreach (TableThread thread in listDelThreads)
                    {
                        if ( (thread == null) || (thread.thread == null) ) continue;
                        try
                        {
                            thread.thread.Abort();
                        }
                        catch (Exception ex)
                        {
                            logger.Error("Thread abort error " + thread.table.LocalName + " " + ex.Message);
                            logger.Error(ex.StackTrace);
                            logger.Error(ex.InnerException);
                        }
                    }
                }

                //проверяем не пора ли прибить какой-то поток, т.к. он слишком долго работает - конец


                //проверяем, если потоков уже максимальное кол-во, то больше пока что не создаём                
                try
                {
                    if (listThreads.Count >= maxReplThreads)
                    {
                        continue;
                    }
                    logger.Info("listThreads.Count = " + listThreads.Count + ", max threads " + maxReplThreads);
                }
                catch (Exception ex) {
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                    logger.Error(ex.InnerException);
                }
                //получение из БД таблицы которую мы слишком долго не реплицировали
                table = DBConn.getReplicationTableExt();
                //создание нового потока, добавление его в список действующих потоков
                try
                {
                    if (table != null)
                    {
                        TableThread thread = new TableThread(table);
                        listThreads.Add(thread);
                    }

                }
                catch (Exception ex)
                {
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                }

            }


        }


        private void doCheckState(object arg)
        {

            while (true)
            {
                if (stopService) { break; }

                Thread.Sleep(checkThreadSleepMs);

                try
                {
                    logger.Error("Начинаем проверку старых статусов! Статус основного потока "+replMainThread.ThreadState);
                    int cntUpdated = DBConn.updateOldStates(deltaCheckTime);
                    logger.Error("СТАРЫЕ СТАТУСЫ ОБНОВЛЕНЫ (" + cntUpdated + " ШТ)");
                }
                catch (Exception ex) {
                    logger.Error("Ошибка при проверке старых статусов репликации");
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                }
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
