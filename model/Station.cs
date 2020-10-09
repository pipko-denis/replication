using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReplicationWinService.model
{
    public class Station
    {
        public Int32 Id 
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

        public Station(Int32 id, String host, Int32 port)
        {
            this.Id = id;
            this.Host = host;
            this.Port = port;
        }

        public Station(int id, string host, int port, String login, String pass, String db) : this(id, host, port)
        {
            Login = login;
            Pass = pass;
            Db = db;
        }

        public Station()
        {
        }

        public override string ToString()
        {
            return  " [id]: "+this.Id +", " + ((this.Host != null) ? " [host]: "+ this.Host + "," : "") + " [port]: " + this.Port;
        }
    }
}
