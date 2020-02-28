using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using MongoDB.Bson;
using MongoDB.Driver;
using Npgsql;

namespace GarbageGen
{
    public class GarbageGenerator
    {
        public readonly string Instructions = "### INSTRUCTIONS ###,,,,,,,,,,,,,,,\n\"# IMPORTANT: Remember to set the TimeZone value in the \"\"parameters\"\" row and/or in your Conversion Time column\",,,,,,,,,,,,,,,\n\"# For instructions on how to set your timezones, visit http://goo.gl/T1C5Ov\",,,,,,,,,,,,,,,\n,,,,,,,,,,,,,,,\n### TEMPLATE ###,,,,,,,,,,,,,,,";
        public readonly string HeaderRow = "Email,Email,Email,First Name,Last Name,City,State,Zip,Country,Phone Number,Phone Number,Phone Number,Conversion Name,Conversion Time,Conversion Value,Conversion Currency";
        public readonly string TimeZonePart = "Parameters:TimeZone=";
        public readonly string LoyaltyRatePart = ";LoyaltyRate=";
        public readonly string TransactionUploadRatePart = ";TransactionUploadRate=";
        public readonly string ParameterRowEnding = ";,,,,,,,,,,,,,,,,";
        public readonly string Entropy = "abcdefghijklmnopqrstuvwxyz";
        public readonly string State = "CA,";
        public readonly string Country = "US";
        public string ConversionAction = "";
        public string fName = "";
        public string TimeZone;
        OfflineDataContainer.CSV_PARAMETRS Parameters;
        Random rnd = new Random();
        public GarbageGenerator()
        {
            var dt = DateTime.Now;
            rnd = new Random((int)dt.Ticks);
        }

        public void PostgresOut(string dbConnectionString, int NumberOfDataRows)
        {
            var connString = "Host=192.168.199.130;Username=postgres;Password=x;Database=offlineuploaddb";
            //C:\projects\Test-Trash\GarbageGen\GarbageGen\bin\Debug\auto_gen_data.csv
            var conn = new NpgsqlConnection(dbConnectionString);
            conn.Open();
         
            var cmd = new NpgsqlCommand("DROP TABLE \"OfflineDataUploadData\"", conn);
            try
            {
                cmd.Prepare();
                cmd.ExecuteNonQuery();
            }
            catch(Exception Ex)
            {
                Console.WriteLine("Exception: SQL Table already dropped");
            }
           
            cmd = new NpgsqlCommand("CREATE TABLE public.\"OfflineDataUploadData\"( email_1 text , email_2 text , email_3 text , first_name text ,  last_name text , city text , state text , zip text , country text,  phone_number_1 text , phone_number_2 text ,  phone_number_3 text ,  conversion_name text , conversion_time text ,  conversion_value text ,   conversion_currency text , time_zone text ,  loyalty_rate text , transaction_upload_rate text )", conn);

            cmd.Prepare();
            cmd.ExecuteNonQuery();

            cmd.Dispose();

            using (var insCmd = new NpgsqlCommand("INSERT INTO \"OfflineDataUploadData\"(Email_1, Email_2, Email_3, First_Name, Last_Name, City, State, Zip, Country, Phone_Number_1, Phone_Number_2, Phone_Number_3, Conversion_Name, Conversion_Time, Conversion_Value, Conversion_Currency, Time_Zone, Loyalty_Rate, Transaction_Upload_Rate) "
                                    + "VALUES(:Email_1, :Email_2, :Email_3, :First_Name, :Last_Name, :City, :State, :Zip, :Country, :Phone_Number_1, :Phone_Number_2, :Phone_Number_3, :Conversion_Name, :Conversion_Time, :Conversion_Value, :Conversion_Currency, :Time_Zone, :Loyalty_Rate, :Transaction_Upload_Rate)", conn))
            {
                insCmd.Parameters.Add(new NpgsqlParameter("Email_1", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Email_2", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Email_3", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("First_Name", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Last_Name", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("City", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("State", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Zip", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Country", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Phone_Number_1", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Phone_Number_2", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Phone_Number_3", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Conversion_Name", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Conversion_Time", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Conversion_Value", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Conversion_Currency", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Time_Zone", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Loyalty_Rate", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Parameters.Add(new NpgsqlParameter("Transaction_Upload_Rate", NpgsqlTypes.NpgsqlDbType.Text));
                insCmd.Prepare();
                for (int count = 0; count < NumberOfDataRows; count++)
                {
                    string OffLineDataRow = GenRow();
                    var columns = OffLineDataRow.Split(',');
                    insCmd.Parameters[0].Value = columns[0];
                    insCmd.Parameters[1].Value = columns[1];
                    insCmd.Parameters[2].Value = columns[2];
                    insCmd.Parameters[3].Value = columns[3];
                    insCmd.Parameters[4].Value = columns[4];
                    insCmd.Parameters[5].Value = columns[5];
                    insCmd.Parameters[6].Value = columns[6];
                    insCmd.Parameters[7].Value = columns[7];
                    insCmd.Parameters[8].Value = columns[8];
                    insCmd.Parameters[9].Value = columns[9];
                    insCmd.Parameters[10].Value = columns[10];
                    insCmd.Parameters[11].Value = columns[11];
                    insCmd.Parameters[12].Value = columns[12];
                    insCmd.Parameters[13].Value = columns[13];
                    insCmd.Parameters[14].Value = columns[14];
                    insCmd.Parameters[15].Value = columns[15];
                    insCmd.Parameters[16].Value = Parameters.TimeZone;
                    insCmd.Parameters[17].Value = Parameters.LoyaltyRate;
                    insCmd.Parameters[18].Value = Parameters.TransactionUploadRate;
  
                    insCmd.ExecuteNonQuery();
                }
            }
            conn.Close();
            conn.Dispose();
        }
        public void MongoOut(string dbConnectionString, int NumberOfDataRows)
        {
            //C:\projects\Test-Trash\GarbageGen\GarbageGen\bin\x64\Debug\auto_gen_data.csv
            // mongodb://192.168.199.130:27017 
            try
            {
                var client = new MongoClient(dbConnectionString);
                var database = client.GetDatabase("AdwordsIntegrationDB");
                database.DropCollection("OfflineDataUploadData");
                database.CreateCollection("OfflineDataUploadData");
                var collection = database.GetCollection<OfflineDataContainer>("OfflineDataUploadData");
               
                for (int count = 0; count < NumberOfDataRows; count++)
                {
                    string OffLineDataRow = GenRow();

                    OfflineDataContainer offLineDataRow = OfflineDataContainer.Create(OffLineDataRow, Parameters);
                    if (offLineDataRow != null)
                    {
                        collection.InsertOne(offLineDataRow);
                        //collection.InsertMany(Name);
                    }
                    else
                    {
                        Console.WriteLine("Bad row in CSV");
                    }          

                }
                
            }
            catch(Exception Ex)
            {
                Console.WriteLine("MONGODB PRBLEM:\n" + Ex.ToString());
            }
        }
        public void Start(string[] args)
        {

            string outputFileName = args[0];
            TimeZone = args[1];
            if((double.TryParse(args[2], out double LoyaltyRate)) == false)
            {
                Console.WriteLine("LoaltyRate must be a double format N.nn.");
                return;
            }
            if ((double.TryParse(args[3], out double TransactionUploadRate)) == false)
            {
                Console.WriteLine("TransactionUploadRate must be a double format N.nn.");
                return;
            }
            if ((int.TryParse(args[4], out int NumberOfDataRows)) == false)
            {
                Console.WriteLine(" NumberOfDataRows must be an integer");
                return;
            }
            ConversionAction = args[5];
            if (args[0].Contains("mongodb://"))
            {
                Parameters.TransactionUploadRate = TransactionUploadRate.ToString();
                Parameters.LoyaltyRate = LoyaltyRate.ToString();
                Parameters.TimeZone = TimeZone;
                MongoOut(args[0], NumberOfDataRows);
                return;

            }
            else if(args[0].Contains("Host="))
            {
                Parameters.TransactionUploadRate = TransactionUploadRate.ToString();
                Parameters.LoyaltyRate = LoyaltyRate.ToString();
                Parameters.TimeZone = TimeZone;
                PostgresOut(args[0], NumberOfDataRows);
                return;
            }
            fName = args[0];
            try
            {
                using (StreamWriter writetext = new StreamWriter(fName))
                {
                    writetext.WriteLine(Instructions);
                    writetext.WriteLine(GenParams(LoyaltyRate, TransactionUploadRate, TimeZone));
                    writetext.WriteLine(HeaderRow);
                    for (int count = 0; count < NumberOfDataRows; count++)
                    {
                        writetext.WriteLine(GenRow());
                    } 
                }
            }
            catch(Exception Ex)
            {
                Console.WriteLine(Ex.ToString());
                return;
            }
        }
        private string GenRow()
        {
            int count = 0;
            int counter = 0;
            int emailColumns = 3;
            int firstNameChars = 5;
            int lastNameChars = 7;
            int cityChars = 8;
            int zipDigits = 5;
            int phoneColumns = 3;
            int phoneDigits = 10;

            int userLen = 8; //8 chars long
            int domainLen = 6;
            
            
            StringBuilder sb = new StringBuilder();
            for (counter = 0; counter < emailColumns; counter++)
            {
                for (count = 0; count < userLen; count++)
                {
                    int rndChar = rnd.Next(0, 25);
                    sb.Append(Entropy[rndChar]);
                }
                sb.Append("@");
                for (count = 0; count < domainLen; count++)
                {
                    int rndChar = rnd.Next(0, 25);
                    sb.Append(Entropy[rndChar]);
                }
                sb.Append(".com,");
            }
            for (count = 0; count < firstNameChars; count++)
            {
                int rndChar = rnd.Next(0, 25);
                sb.Append(Entropy[rndChar]);
            }
            sb.Append(",");
            for (count = 0; count < lastNameChars; count++)
            {
                int rndChar = rnd.Next(0, 25);
                sb.Append(Entropy[rndChar]);
            }
            sb.Append(",");
            for (count = 0; count < cityChars; count++)
            {
                int rndChar = rnd.Next(0, 25);
                sb.Append(Entropy[rndChar]);
            }
            sb.Append(",");
            sb.Append(State);
            for (count = 0; count < zipDigits; count++)
            {
                int rndChar = rnd.Next(0, 9);
                sb.Append(rndChar.ToString());
            }
            sb.Append(",");
            sb.Append(Country);
            sb.Append(",");
           
            for (counter = 0; counter < phoneColumns; counter++)
            {
                sb.Append("+1");
                for (count = 0; count < phoneDigits; count++)
                {
                    int rndChar = rnd.Next(0, 9);
                    sb.Append(rndChar.ToString());
                }
                sb.Append(",");
            }
            sb.Append(ConversionAction);
            sb.Append(",");
            DateTime dt = DateTime.Now;

            sb.Append(dt.ToString("yyyyMMdd HHmmss").Replace(":", "").Replace("AM", "").Replace("PM", "").Replace("-", "").Replace("/", ""));
            sb.Append(" " + TimeZone);
            sb.Append(",");

            sb.Append(rnd.Next(1, 9).ToString());
            sb.Append(",");

            sb.Append("USD");

            return sb.ToString();
        }
        private string GenParams(double LoyaltyRate, double TransactionUploadRate, string TimeZone)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(TimeZonePart + TimeZone);
            sb.Append(LoyaltyRatePart + LoyaltyRate);
            sb.Append(TransactionUploadRatePart + TransactionUploadRate);
            sb.Append(ParameterRowEnding);
            return sb.ToString();
        }
    }
}
