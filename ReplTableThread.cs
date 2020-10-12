using log4net;
using MySql.Data.MySqlClient;
using ReplicationWinService.model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ReplicationWinService
{
    class ReplTableThread
    {
        private static ILog logger = LogManager.GetLogger("ReplTAbleThread");

        private Thread thread;

        private ReplTable table; 

        public ReplTableThread(ReplTable table)
        {            
            this.table = table;
            this.thread = new Thread(doTableReplication);
            this.thread.Start();
        }

        [Obsolete("Deprecated")]
        private void doTableReplication() {
            logger.Info(this.table.LocalName + " replication ");


            List<Station> lstStations = DBConn.getStations(false);

            String insertStrBeg = table.getLocalInsertScript();

            List<String> listInsScripts = null;

            bool error = false;

            foreach (Station station in lstStations) {
                int maxId = DBConn.getLocalMaxReplId(station, this.table);

                listInsScripts = getReplicationScripts(station, table, maxId,out error); 

                logger.Info("Станция " + station.Host +"("+ station.Id+ ")" + " таблица " + table.LocalName + " кол. записей " + listInsScripts.Count);

                string str = "";
                int incr = 0;
                foreach (String script in listInsScripts) {
                    if (incr > 4) {
                        //logger.Info(str);
                        DBConn.replicationInsert(str);
                        //DBConn.updateLastReplDate(station.Id);
                        incr = 0;
                        str = "";
                    }
                    str += insertStrBeg + script;
                    incr++;
                    //logger.Info(str);
                    
                    //logger.Info("INCR="+incr);
                }
                if (incr > 0) {
                    DBConn.replicationInsert(str);
                    //DBConn.updateLastReplDate(station.Id);
                }
                logger.Info("DBConn.updateLastReplDate");
                DBConn.updateLastReplDate(station.Id, error);

            }

            //Thread.Sleep(120000);
            //logger.Info(this.table.LocalName + " replication done ");
            logger.Info("DBConn.saveReplDoneResult");
            saveReplDoneResult();
        }



        public List<String> getReplicationScripts(Station station, ReplTable table, int startId, out bool error){
            error = false;
            List<String> result = new List<String>();

            MySqlCommand mySqlCommand = null;
            MySqlConnection conn = null;

            try
            {
                conn = new MySqlConnection("Server="+ station.Host + ";Port="+ station.Port+ ";Database="+ station.Db + ";Uid="+ station.Login+ ";Pwd="+ station.Pass+ ";Connection Timeout=30;default command timeout=20;");
                conn.Open();
                mySqlCommand = new MySqlCommand(table.getRemoteSelectScript(startId), conn);
                mySqlCommand.CommandTimeout = 30;
                logger.Info(table.getRemoteSelectScript(startId));
                MySqlDataReader reader = mySqlCommand.ExecuteReader();
                String values = "";

                while (reader.Read())
                {
                    values = "";
                    for(int i = 0; i < table.localFields.Count(); i++) {
                        if (reader.IsDBNull(i))
                        {
                            values += " NULL,";
                        }
                        else
                        {
                            if (table.localFields[i].DataType.Equals(ReplField.FieldDataTypes.FtFloat))
                            {
                                values += " " + reader.GetString(table.localFields[i].Name).Replace(",", ".") + ",";
                            }
                            else if (table.localFields[i].DataType.Equals(ReplField.FieldDataTypes.FtDateTime))
                            {
                                values += " '" + reader.GetDateTime(table.localFields[i].Name).ToString("yyyy-MM-dd HH:mm:ss") + "',";
                            }
                            else
                            {
                                values += " '" + reader.GetString(table.localFields[i].Name) + "',";
                            }

                        }
                    }

                    values += station.Id + ");";

                    result.Add(values);

                }
                
            }
            catch (Exception ex)
            {
                logger.Error("Не удалось получить данные репликации для таблицы " + table.RemoteName + ", хост:" + station.Host+"("+ station.Id+ ")");
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
                error = true;
            }
            finally
            {
                if (mySqlCommand != null) mySqlCommand.Dispose();
                if (conn != null) conn.Close();
            }
            return result;
        }




        private bool saveReplDoneResult() {

            bool result = false;

            MySqlCommand mySqlCommand = null;
            MySqlConnection conn = null;

            try
            {
                conn = new MySqlConnection(ServiceMain.mainConnString);
                conn.Open();
                mySqlCommand = new MySqlCommand("UPDATE t_repl_tables Set repl_state = 0 Where id = " + this.table.Id + ";", conn);
                mySqlCommand.ExecuteNonQuery();
                logger.Info("Состояние repl_state = 0 для таблицы " + this.table.LocalName + " сохранено!");
                result = true;
            }
            catch (Exception ex)
            {
                logger.Error("Не удалось сохранить repl_state = 0 для таблицы " + this.table.LocalName);
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
            }
            finally
            {
                if (mySqlCommand != null) mySqlCommand.Dispose();
                if (conn != null) conn.Close();
            }

            return result;
        }

    }
}
