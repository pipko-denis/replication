using log4net;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReplicationWinService.model
{
    class DBConn
    {
        private static ILog logger = LogManager.GetLogger("DBConn");




        public static List<Station> getStations( bool scan)
        {
            List<Station> stations = new List<Station>();

            MySqlConnection conn = null;
            MySqlDataReader reader = null;
            MySqlCommand mySqlCommand = null;
            try
            {
                conn = new MySqlConnection(ConnString.getMainConnectionString());
                conn.Open();                
                if (scan)
                {
                    mySqlCommand = new MySqlCommand("SELECT id, host, port, login, passwd, db FROM t_stations Where not((host is null) or (port is null)) and (scan_enab = 1);", conn);
                }
                else
                {
                    mySqlCommand = new MySqlCommand("SELECT id, host, port, login, passwd, db FROM t_stations Where not((host is null) or (port is null)) and (repl_enab = 1);", conn);
                }
                reader = mySqlCommand.ExecuteReader();

                while (reader.Read())
                {
                    stations.Add(new Station(reader.GetInt32(0), reader.GetString(1), reader.GetInt32(2), reader.GetString(3), reader.GetString(4), reader.GetString(5)));
                }

                logger.Info("Получение станций для "+( scan ? "сканирования" : "репликации" )+":"+ stations.Count+ " шт.");

            }
            catch (Exception ex)
            {
                logger.Error("DoScanWork: Не удалось подлючиться к серверу с настройками");
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
            }
            finally
            {
                if (reader != null) { reader.Close(); reader.Dispose(); }
                if (mySqlCommand != null) mySqlCommand.Dispose();
                if (conn != null) { conn.Close(); }
            }

            return stations;

        }

        public static int updateLastReplDateExt(Int32 tableId, bool error)
        {
            int result = 0;
            MySqlCommand mySqlCommand = null;
            MySqlConnection conn = null;
            try
            {
                conn = new MySqlConnection(ServiceMain.mainConnString);
                conn.Open();
                mySqlCommand = new MySqlCommand("UPDATE `spider_cdc`.`t_stations_tables_replication` SET `repl_state` = 0, `last_repl_date` = now(), `last_repl_error` = "+
                    ((error) ? "1" : "0") +" WHERE `id` = "+tableId+";", conn);
                result = mySqlCommand.ExecuteNonQuery();
                //logger.Info("Скрипт \"" + script + "\" выполнен!");
            }
            catch (Exception ex)
            {
                logger.Error("Не удалось обновить дату последней репликации \"" + tableId + "\"");
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

        [Obsolete("Depricated")]
        public static int updateLastReplDate(Int32 stationId, bool error)
        {
            int result = 0;
            MySqlCommand mySqlCommand = null;
            MySqlConnection conn = null;
            try
            {
                conn = new MySqlConnection(ServiceMain.mainConnString);
                conn.Open();
                logger.Info("updateLastReplDate conn.Open");
                if (error) mySqlCommand = new MySqlCommand("update `t_stations` Set `t_stations`.`repl_error` = 1  Where `id` = " + stationId + ";", conn); 
                else mySqlCommand = new MySqlCommand("update `t_stations` Set `t_stations`.`last_repl_date` = current_timestamp(), `t_stations`.`repl_error` = 0 Where `id` = " + stationId + ";", conn);
                result = mySqlCommand.ExecuteNonQuery();
                //logger.Info("Скрипт \"" + script + "\" выполнен!");
            }
            catch (Exception ex)
            {
                logger.Error("Не удалось обновить дату последней репликации \"" + stationId + "\"");
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


        public static int replicationInsert(String script)
        {
            int result = 0;
            MySqlCommand mySqlCommand = null;
            MySqlConnection conn = null;
            try
            {
                conn = new MySqlConnection(ServiceMain.mainConnString);
                conn.Open();
                mySqlCommand = new MySqlCommand(script, conn);

                result = mySqlCommand.ExecuteNonQuery();
                //logger.Info("Скрипт \"" + script + "\" выполнен!");
            }
            catch (Exception ex)
            {
                logger.Error("Не удалось выполнить скрипт \"" + script + "\"");
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


        public static int getLocalMaxReplId(Station station, ReplTable table)
        {
            int maxId = 0;
            MySqlCommand mySqlCommand = null;
            MySqlConnection conn = null;
            MySqlDataReader reader = null;
            try
            {
                //logger.Info(ServiceMain.mainConnString);
                conn = new MySqlConnection(ServiceMain.mainConnString);
                conn.Open();
                String script = table.getLocalMaxIdScript(station.Id);
                //logger.Info(script);                
                mySqlCommand = new MySqlCommand(script, conn);

                reader = mySqlCommand.ExecuteReader();    
                
                while (reader.Read()) {                    
                    if (!reader.IsDBNull(0)) {
                        maxId = reader.GetInt32(0);
                    }                    
                } 
                logger.Info("Таблица " + table.LocalName +"(tableid="+table.Id+ "), станция id=:" + station.Id + " последняя запись с id = "+ maxId);
            }
            catch (Exception ex)
            {
                logger.Error("Не получить макисмальный идентификатор для репликации для таблицы " + table.LocalName + ", хост:" + station.Host);
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
            }
            finally
            {
                if (mySqlCommand != null) mySqlCommand.Dispose();
                if (conn != null) conn.Close();
            }
            return maxId;
        }


        public static String getProperty(String connstring, String propName)
        {
            String result = null;
            MySqlConnection conn = null;
            MySqlDataReader reader = null;
            MySqlCommand mySqlCommand = null;
            try
            {
                conn = new MySqlConnection(ConnString.getMainConnectionString());
                conn.Open();
                logger.Info("DoScanWork: Подключение к серверу с параметрами открыто, получение " + propName);
                mySqlCommand = new MySqlCommand("SELECT value FROM t_properties Where name = '" + propName + "'", conn);
                reader = mySqlCommand.ExecuteReader();

                while (reader.Read())
                {
                    result = reader.GetString(0);
                }
                logger.Info("Получен параметр "+ propName + ", значение " + result);
            }
            catch (Exception ex)
            {
                logger.Error("DoScanWork: Не удалось подлючиться к серверу с параметрами!");
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
            }
            finally
            {
                if (reader != null) { reader.Close(); reader.Dispose(); }
                if (mySqlCommand != null) mySqlCommand.Dispose();
                if (conn != null) { conn.Close(); }
            }

            return result;


        }

        public static ReplTableExt getReplicationTableExt()
        {
            ReplTableExt result = null;

            MySqlConnection conn = null;
            MySqlCommand mySqlCommand = null;
            MySqlCommand mySqlCommandFields = null;
            try
            {

                conn = new MySqlConnection(ConnString.getMainConnectionString());
                //mySqlCommand = new MySqlCommand("CALL `sp_repl_tables_get_one_for_repl`(@out_id,@out_local_name,@out_remote_name);", conn);// select @out_id, @out_local_name, @out_remote_name;", conn);
                mySqlCommand = new MySqlCommand("sp_repl_tables_get_one_for_repl_ext", conn);

                mySqlCommand.CommandType = CommandType.StoredProcedure;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_table_id", MySqlDbType.Int32));
                mySqlCommand.Parameters["@_table_id"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_local_tbl_name", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_local_tbl_name"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_remote_tbl_name", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_remote_tbl_name"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_key_col_name", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_key_col_name"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_repl_rec_count", MySqlDbType.Int32));
                mySqlCommand.Parameters["@_repl_rec_count"].Direction = ParameterDirection.Output;

                mySqlCommand.Parameters.Add(new MySqlParameter("@_station_id", MySqlDbType.Int32));
                mySqlCommand.Parameters["@_station_id"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_host", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_host"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_port", MySqlDbType.Int32));
                mySqlCommand.Parameters["@_port"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_login", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_login"].Direction = ParameterDirection.Output;

                mySqlCommand.Parameters.Add(new MySqlParameter("@_passwd", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_passwd"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_db", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_db"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_display_name", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_display_name"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_last_repl_date", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_last_repl_date"].Direction = ParameterDirection.Output;

                conn.Open();
                mySqlCommand.ExecuteNonQuery();


                if (!mySqlCommand.Parameters["@_table_id"].Value.Equals(DBNull.Value))
                {
                    result = new ReplTableExt(
                        mySqlCommand.Parameters["@_table_id"].Value,
                        mySqlCommand.Parameters["@_local_tbl_name"].Value,
                        mySqlCommand.Parameters["@_remote_tbl_name"].Value,
                        mySqlCommand.Parameters["@_key_col_name"].Value,
                        mySqlCommand.Parameters["@_repl_rec_count"].Value,

                        mySqlCommand.Parameters["@_station_id"].Value,
                        mySqlCommand.Parameters["@_host"].Value,
                        mySqlCommand.Parameters["@_port"].Value,
                        mySqlCommand.Parameters["@_login"].Value,
                        mySqlCommand.Parameters["@_passwd"].Value,
                        mySqlCommand.Parameters["@_db"].Value,
                        mySqlCommand.Parameters["@_display_name"].Value,
                        mySqlCommand.Parameters["@_last_repl_date"].Value
                    );

                    mySqlCommandFields = new MySqlCommand("SELECT COLUMN_NAME,columns.DATA_TYPE " +
                                         "FROM information_schema.columns WHERE table_schema='spider_cdc' AND table_name='" + result.LocalName + "' " +
                                         "AND NOT COLUMN_NAME IN ('id_repl','station_id') ORDER BY ORDINAL_POSITION;", conn);

                    MySqlDataReader reader = mySqlCommandFields.ExecuteReader();

                    if (result.localFields == null) result.localFields = new List<ReplField>();

                    while (reader.Read())
                    {
                        result.localFields.Add(new ReplField(reader.GetString(0), reader.GetString(1)));
                    }

                }
                else
                {
                    logger.Info("REPALLRRUN. Все таблицы уже реплицируются.");
                }

            }
            catch (Exception ex)
            {
                logger.Error("Не удалось получить таблицу для репликации(getReplicationTable):" + ex.Message);
                logger.Error(ex.StackTrace);
            }
            finally
            {
                if (mySqlCommand != null) mySqlCommand.Dispose();
                if (mySqlCommandFields != null) mySqlCommandFields.Dispose();
                if (conn != null) { conn.Close(); }
            }

            return result;
        }

        [Obsolete("Depricated")]
        public static ReplTable getReplicationTable()
        {
            ReplTable result = null;

            MySqlConnection conn = null;
            MySqlCommand mySqlCommand = null;
            MySqlCommand mySqlCommandFields = null;
            try
            {

                conn = new MySqlConnection(ConnString.getMainConnectionString());
                //mySqlCommand = new MySqlCommand("CALL `sp_repl_tables_get_one_for_repl`(@out_id,@out_local_name,@out_remote_name);", conn);// select @out_id, @out_local_name, @out_remote_name;", conn);
                mySqlCommand = new MySqlCommand("sp_repl_tables_get_one_for_repl", conn);

                mySqlCommand.CommandType = CommandType.StoredProcedure;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_table_id", MySqlDbType.Int32));
                mySqlCommand.Parameters["@_table_id"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_local_tbl_name", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_local_tbl_name"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_remote_tbl_name", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_remote_tbl_name"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_key_col_name", MySqlDbType.VarChar));
                mySqlCommand.Parameters["@_key_col_name"].Direction = ParameterDirection.Output;
                mySqlCommand.Parameters.Add(new MySqlParameter("@_repl_rec_count", MySqlDbType.Int32));
                mySqlCommand.Parameters["@_repl_rec_count"].Direction = ParameterDirection.Output;

                conn.Open();
                mySqlCommand.ExecuteNonQuery();


                if (!mySqlCommand.Parameters["@_table_id"].Value.Equals(DBNull.Value))
                {
                    result = new ReplTable(mySqlCommand.Parameters["@_table_id"].Value,
                    mySqlCommand.Parameters["@_local_tbl_name"].Value,
                    mySqlCommand.Parameters["@_remote_tbl_name"].Value,
                    mySqlCommand.Parameters["@_key_col_name"].Value,
                    mySqlCommand.Parameters["@_repl_rec_count"].Value
                    );

                    mySqlCommandFields = new MySqlCommand("SELECT COLUMN_NAME,columns.DATA_TYPE " +
                                         "FROM information_schema.columns WHERE table_schema='spider_cdc' AND table_name='" + result.LocalName +"' "+
                                         "AND NOT COLUMN_NAME IN ('id_repl','station_id') ORDER BY ORDINAL_POSITION;", conn);

                    MySqlDataReader reader = mySqlCommandFields.ExecuteReader();

                    if (result.localFields == null) result.localFields = new List<ReplField>();

                    while (reader.Read())
                    {
                        result.localFields.Add(new ReplField( reader.GetString(0), reader.GetString(1)));
                    }

                }
                else
                {
                    logger.Info("REPALLRRUN. Все таблицы уже реплицируются.");
                }

            }
            catch (Exception ex)
            {
                logger.Error("Не удалось получить таблицу для репликации(getReplicationTable):" + ex.Message);
                logger.Error(ex.StackTrace);
            }
            finally
            {
                if (mySqlCommand != null) mySqlCommand.Dispose();
                if (mySqlCommandFields != null) mySqlCommandFields.Dispose();
                if (conn != null) { conn.Close(); }
            }

            return result;
        }


        public static void saveParamsOnServiceStop() {
            MySqlCommand mySqlCommand = null;
            MySqlConnection conn = null;

            try
            {
                conn = new MySqlConnection(ConnString.getMainConnectionString());
                conn.Open();
                mySqlCommand = new MySqlCommand("UPDATE t_stations_tables_replication Set repl_state = 0, `last_repl_date` = now(), `last_repl_error` = 1 Where repl_state = 1;", conn);
                mySqlCommand.ExecuteNonQuery();
                logger.Info("Состояние repl_state = 0 для таблицы всех таблиц сохранено!");
            }
            catch (Exception ex)
            {
                logger.Error("Не удалось сохранить repl_state = 0 для всех таблиц.");
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
            }
            finally
            {
                if (mySqlCommand != null) mySqlCommand.Dispose();
                if (conn != null) conn.Close();
            }
        }


    }
}
