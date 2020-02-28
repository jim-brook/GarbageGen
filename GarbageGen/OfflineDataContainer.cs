using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
namespace GarbageGen
{
    public class OfflineDataContainer
    {
        [MongoDB.Bson.Serialization.Attributes.BsonId]
        public ObjectId Id { get; set; }
        public string TimeZone { get; set; }
        public string LoyaltyRate { get; set; }
        public string TransactionUploadRate { get; set; }
        public string Email1 { get; set; }
        public string Email2 { get; set; }
        public string Email3 { get; set; }
        public string PhoneNumber1 { get; set; }
        public string PhoneNumber2 { get; set; }
        public string PhoneNumber3 { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string Zip { get; set; }
        public string Country { get; set; }
        public string ConversionName { get; set; }
        public string ConversionTime { get; set; }//DateTime?
        public string ConversionValue { get; set; }//Decimal?
        public string ConversionCurrency { get; set; }
        private static int columnLength = 16; //We should have a row with 16 columns in the csv
        public struct CSV_PARAMETRS
        {
            internal string TimeZone;
            internal string LoyaltyRate;
            internal string TransactionUploadRate;
        }
        public static OfflineDataContainer Create(string csvLine, CSV_PARAMETRS Parameters)
        {
            string[] columns = csvLine.Split(',');
            if (columns.Length != columnLength)
            {
                return null;
            }
            return new OfflineDataContainer(columns, Parameters);

        }
        private OfflineDataContainer(string[] columns, CSV_PARAMETRS Parameters)
        {
            int columnNumber = 0;
            LoyaltyRate = Parameters.LoyaltyRate;
            TimeZone = Parameters.TimeZone;
            TransactionUploadRate = Parameters.TransactionUploadRate;
            Email1 = columns[columnNumber++];
            Email2 = columns[columnNumber++];
            Email3 = columns[columnNumber++];
            FirstName = columns[columnNumber++];
            LastName = columns[columnNumber++];
            City = columns[columnNumber++];
            State = columns[columnNumber++];
            Zip = columns[columnNumber++];
            Country = columns[columnNumber++];
            PhoneNumber1 = columns[columnNumber++];
            PhoneNumber2 = columns[columnNumber++];
            PhoneNumber3 = columns[columnNumber++];
            ConversionName = columns[columnNumber++];
            ConversionTime = columns[columnNumber++]; //Optionally append the TZ parameter here if not specified in ConversionTime
            ConversionValue = columns[columnNumber++];
            ConversionCurrency = columns[columnNumber];

        }
    }
}
