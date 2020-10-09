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

        DateTime dateStart
        { get; set; }

        Thread thread
        { get; set; }

        ReplTableExt table
        { get; set; }

        public TableThread(ReplTableExt table)
        {
            this.dateStart = DateTime.Now;
            this.thread = new Thread(doTableReplication);
            this.table = table;
            this.thread.Start();
        }

        private void doTableReplication()
        {
            bool error = false;
            try
            {
                logger.Info(this.table.LocalName + " replication ");
                Thread.Sleep(5000);// ПАУЗ ПОЗЖЕ УБЕРЁМ, А МОЖЕТ И НЕТ

                //ВЫПОЛНЕНИЕ РЕПЛИКАЦИИ БУДЕТ ТУТ

            }
            catch (Exception ex) {
                logger.Error("Не удалось выполнить репликацию таблицы " + this.table.RemoteName+ " ("+ this.table.Id + "), хост:" + this.table.StationName + "(" + this.table.StationId + ")");
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
                error = true;
            }
            while (true)  {
                try
                {
                    if (ServiceMain.stopService) break;
                    if (DBConn.updateLastReplDateExt(this.table.Id, error) > 0) break;
                }
                catch (Exception ex)
                {
                    logger.Error("Не удалось сохранить результат репликации " + this.table.RemoteName + " (" + this.table.Id + "), хост:" + this.table.StationName + "(" + this.table.StationId + ")");
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                    Thread.Sleep(20000);
                }
            }

        }
    }
}
