using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReplicationWinService.model
{
    class ReplTable
    {
        private static ILog logger = LogManager.GetLogger("ReplTable");

        public Int32 Id
         { get; set;}

        public String LocalName
        { get; set; }

        public String RemoteName
        { get; set; }

        public List<ReplField> localFields
        { get; set; }

        public String IdColName 
        { get; set; }

        public String ReplRecCnt
        { get; set; }

        public ReplTable(int id, string localName, string remoteName)
        {
            Id = id;
            LocalName = localName;
            RemoteName = remoteName;
            IdColName = "ID";
        }


        public ReplTable(object value1, object value2, object value3, object value4, object value5)
        {
            try
            {
                this.Id = (int)value1;
            }
            catch (Exception ex)
            {
                logger.Error("Конструктор ReplTable");
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);
            }
            this.LocalName = value2.ToString();
            this.RemoteName = value3.ToString();
            this.IdColName = value4.ToString();
            this.ReplRecCnt = value5.ToString();
        }

        public String getRemoteSelectScript(int startid) {
            return " SELECT * from `"+this.RemoteName+"` Where `"+this.IdColName + "` > "+ startid + " order by `"+ this.IdColName + "` limit "+ ReplRecCnt+ " ;";
        }

        public String getLocalMaxIdScript(int station_id){
            return " SELECT COALESCE(MAX("+ this.IdColName +"),0) as maxId from " + this.LocalName +
                    " Where station_id = "+ station_id + ";";
        }

        public String getLocalInsertScript()
        {
            String result = " INSERT `" + this.LocalName + "` (";
            for (int i = 0; i < this.localFields.Count(); i++) {
                    result += "`" + this.localFields[i].Name + "`,";
            }

            result += " `station_id` )VALUES(";
            return result;
        }

    }
}
