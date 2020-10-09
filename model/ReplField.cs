using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReplicationWinService.model
{
    class ReplField
    {
        public enum FieldDataTypes {
            FtInt, FtStr, FtFloat, FtDateTime
        }

        private static List<String> lstFloatDataTypes = new List<String> { "float", "double","decimal" };
        private static List<String> lstDateTimeDataTypes = new List<String> { "datetime", "timestamp", "date" };




        public String Name
        { get; set; }

        public FieldDataTypes DataType
        { get; set; }

        public ReplField(string name, string dataType)
        {
            Name = name;
            if (lstFloatDataTypes.Contains(dataType)) {
                DataType = FieldDataTypes.FtFloat;
            } else if (lstDateTimeDataTypes.Contains(dataType)) {
                DataType = FieldDataTypes.FtDateTime;
            }
            else{
                DataType = FieldDataTypes.FtStr;
            }
        }
    }
}
