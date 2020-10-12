using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReplicationWinService.model
{
    class ReplTableExt : ReplTable
    {
        public Int32 StationId
        { get; set; }

        public String Host
        { get; set; }

        public Int32 Port
        { get; set; }

        public String Login
        { get; set; }

        public String Pass
        { get; set; }

        public String Db
        { get; set; }

        public Int32 MaxCalcedId
        { get; set; }

        public String StationName
        { get; set; }

        public String LastReplDate
        { get; set; }

        public ReplTableExt() { }

        public ReplTableExt(object id, object localName, object remoteName, object idColName, object replRecCnt
            , object stationId, object host, object port, object login, object pass, object db, object maxCalcedId, object stationName, object lastReplDate)
            : base(id, localName, remoteName, idColName, replRecCnt)
        {
            try
            {
                if (stationId != null) this.StationId = (int)stationId;
                if (port != null) this.Port = (int)port;
                logger.Error("maxCalcedId: " + maxCalcedId);
                if (maxCalcedId != null) this.MaxCalcedId = (int)maxCalcedId;
            }
            catch (Exception ex)
            {
                logger.Error("Конструктор ReplTableExt: " + ex.Message);
                logger.Error(ex.StackTrace);
            }

            this.Host = host.ToString();
            this.Login = login.ToString();
            this.Pass = pass.ToString();
            this.Db = db.ToString();
            this.StationName = stationName.ToString();
            this.LastReplDate = lastReplDate.ToString();
        }

        //[Override]
        public String getMaxIdScript()
        {
            return " SELECT COALESCE(MAX(" + this.IdColName + "),0) as maxId from " + this.LocalName +";";
        }
    }
}
