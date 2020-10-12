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

        private void doTableReplication()
        {
            bool error = false;
            try
            {
                //logger.Info(this.table.LocalName + " replication ");
                Thread.Sleep(30000);// ПАУЗУ ПОЗЖЕ УБЕРЁМ, А МОЖЕТ И НЕТ

                //ВЫПОЛНЕНИЕ РЕПЛИКАЦИИ БУДЕТ ТУТ
                //получаем последнюю среплицированную запись
                int maxId = DBConn.getLocalMaxReplId(this.table);

                List<String> listInsScripts = DBConn.getReplicationScripts(this.table, maxId, out error);

                foreach (String insertScript in listInsScripts) {
                    logger.Info("First one from "+ listInsScripts.Count+" is: "+insertScript);
                    break;
                }

            }
            catch (Exception ex) {
                logger.Error("Не удалось репликация таблицы " + this.table.LocalName + " ("+ this.table.Id + "), хост:" + this.table.StationName );
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
                error = true;
            }
            while (true)  {
                try
                {
                    if (ServiceMain.stopService) break;
                    if (DBConn.updateLastReplDateExt(this.table.Id, error) > 0)
                    {
                        logger.Error("Репликация завершена " + this.table.LocalName + " (" + this.table.Id + "), хост:" + this.table.StationName);
                        break;
                    }
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
