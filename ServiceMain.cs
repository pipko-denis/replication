﻿using System;
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

        private static int maxReplThreads = 2;

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

        private static bool threadIsAlive(TableThread tableThread)
        {
            return tableThread.thread.IsAlive;
        }


        private void doReplWork(object arg)
        {

            ReplTableExt table = null;

            while (true)
            {
                if (stopService) { break; }

                Thread.Sleep(5000);

                //проверяем не пора ли прибить какой-то поток, т.к. он слишком долго работает - начало
                TimeSpan ts;
                DateTime currentTime = DateTime.Now;
                List<TableThread> listDelThreads = null;
                bool addingToRemoveList = false;
                foreach (TableThread thread in listThreads) {
                    addingToRemoveList = false;
                    ts = currentTime - thread.dateStart;
                    logger.Info("Thread for table " + thread.table.LocalName + ", state " + thread.thread.ThreadState);

                    //Проверки
                    if (ts.TotalMinutes > 15) {
                        logger.Info("Stopping thread for table "+ thread.table.LocalName+ ", started at " + thread.dateStart);                        
                        addingToRemoveList = true;
                    } else if (thread.thread.ThreadState == System.Threading.ThreadState.Stopped) {
                        logger.Info("Stopping thread for table " + thread.table.LocalName + ", because  it is not alive");
                        addingToRemoveList = true;
                    }

                    //добавление в список удаляемых потоков и отклчение
                    if (addingToRemoveList)
                    {
                        if (listDelThreads == null) listDelThreads = new List<TableThread>();
                        listDelThreads.Add(thread);
                        thread.thread.Abort();                        
                    }
/**/

                }
                if (listDelThreads != null) {
                    listThreads = listThreads.Except(listDelThreads).ToList();
                }



                //TableThread[] list = Array.FindAll(listThreads, elem => elem.thread.isAlive);//(listThreads.ToArray<TableThread>, threadIsAlive);
                //listThreads.RemoveAll()

                //проверяем не пора ли прибить какой-то поток, т.к. он слишком долго работает - конец

                //проверяем, если потоков уже максимальное кол-во, то больше пока что не создаём                
                if (listThreads.Count >= maxReplThreads) {
                    continue;
                }
                logger.Info("listThreads.Count = " + listThreads.Count + ", max threads " + maxReplThreads);

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
