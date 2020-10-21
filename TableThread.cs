using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using ReplicationWinService.model;
using log4net;

namespace ReplicationWinService
{
    class TableThread
    {
        private static ILog logger = LogManager.GetLogger("TableThread");

        public DateTime dateStart
        { get; set; }

        public Thread thread
        { get; set; }

        public ReplTableExt table
        { get; set; }

        public TableThread(ReplTableExt table)
        {
            this.dateStart = DateTime.Now;
            this.thread = new Thread(doTableReplication);
            this.table = table;
            logger.Error("Запускаем репликацию " + this.table.LocalName + " (" + this.table.Id + "), хост:" + this.table.StationName);
            this.thread.Start();
        }


        private void saveListToDatabase(List<String> listInsScripts) {
            String insertStrBeg = table.getLocalInsertScriptBeg();
            string str = insertStrBeg;
            int incr = 0;
            //foreach (String script in listInsScripts)
            int countMinOne = listInsScripts.Count - 1;
            for (int i = 0; i < listInsScripts.Count; i++)
            {
                if (incr > 4)
                {
                    if (ServiceMain.showScripts) logger.Info(str);
                    DBConn.replicationInsert(str);
                    incr = 0;
                    str = insertStrBeg;
                }
                str += listInsScripts[i];
                if ((incr < 4) && (i < countMinOne)) str += ", ";
                incr++;
            }
            if (incr > 0)
            {
                if (ServiceMain.showScripts) logger.Info(str);
                //ОЧень важная проверка, если в последнем блоке было не 5 зписей, то последняя запятая должна быть удалена
                //if ( (incr < 4) && (str!=null) && (str.Length > 1)) str = str.Substring(0, str.Length);
                DBConn.replicationInsert(str);
            }
        }

        private void doTableReplication()
        {
            bool error = false;
            try
            {
                //Thread.Sleep(1000);
                //ВЫПОЛНЕНИЕ РЕПЛИКАЦИИ

                //получаем последнюю среплицированную запись
                int maxId = DBConn.getLocalMaxReplId(this.table, out error);

                if (!error)
                {
                    List<String> listInsScripts = DBConn.getReplicationScripts(this.table, maxId, out error);
                    saveListToDatabase(listInsScripts);
                }
                
                //logger.Info("RECORDCOUNT: " + listInsScripts.Count);
            }
            catch (Exception ex) {
                logger.Error("Не удалось репликация таблицы " + this.table.LocalName + " ("+ this.table.Id + "), хост:" + this.table.StationName );
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
                error = true;
            }

            int cntr = 0;
            int cntRecStaions = 0;
            int cntRecReplTables = 0;
            while (true)  {
                try
                {
                    if  (cntr > 15) break; // if we can't save results more than 5 min (Thread.Sleep(20000);)
                    if (DBConn.updateLastReplDateExt(this.table.StationId,this.table.Id, error, out cntRecReplTables, out cntRecStaions) )
                    {
                        logger.Error("Репликация завершена " + this.table.LocalName + " (" + this.table.Id + "), хост:" + this.table.StationName
                            + "( кол. зап:"+ cntRecReplTables+","+ cntRecStaions);
                        break;
                    }
                    cntr++;
                }
                catch (Exception ex)
                {
                    logger.Error("Не удалось сохранить результат репликации " + this.table.LocalName + " (" + this.table.Id + "), хост:" + this.table.StationName );
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                    Thread.Sleep(20000);
                }
            }

        }

    }
}
