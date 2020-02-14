using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace TestProject
{
    public class CsvService
    {
        readonly string[] Headers = new[] { "PHONE_NUMBER", "ACTIVATION_DATE", "DEACTIVATION_DATE" };

        public void ProcessCsv()
        {
            List<Dictionary<string, List<DatePair>>> cache = new List<Dictionary<string, List<DatePair>>>();
            // partition by 10 to be run on 10 threads
            for (int i = 0; i < 10; i++)
            {
                cache.Add(new Dictionary<string, List<DatePair>>());
            }

            using (var reader = new StreamReader("../../../File/test.csv"))
            {
                var headerRow = reader.ReadLine();
                if (!AllHeadersExist(headerRow))
                {
                    throw new Exception("Missing headers or headers order is not correct.");
                }

                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(',');

                    var phoneNumber = values[0];
                    var activationDate = string.IsNullOrWhiteSpace(values[1]) ? DateTime.Now.Date : DateTime.ParseExact(values[1], "yyyy-MM-dd", null).Date;
                    var deactivationDate = string.IsNullOrWhiteSpace(values[2]) ? DateTime.Now.Date : DateTime.ParseExact(values[2], "yyyy-MM-dd", null).Date;

                    int partition = int.Parse(phoneNumber.Substring(phoneNumber.Length - 1));
                    DatePair startEndPair = new DatePair(activationDate, deactivationDate);

                    if (!cache[partition].ContainsKey(phoneNumber))
                    {
                        cache[partition].Add(phoneNumber, new List<DatePair>() { startEndPair });
                    }
                    cache[partition][phoneNumber].Add(startEndPair);
                }

                var tasks = new List<Task<List<PhoneNumberDate>>>();
                foreach (var dict in cache)
                {
                    tasks.Add(ParsePartition(dict));
                }

                var res = Task.WhenAll(tasks);
                ExportCsvFile(res.Result, DateTime.Now.ToString("yyyy-MM-dd hhmmss") + "res.csv");
            }
        }

        private void ExportCsvFile(List<PhoneNumberDate>[] result, string filePath)
        {
            // Write to csv file after N number of records
            int rowsToWrite = 1000000;

            int count = 0;
            string csvBuilder = string.Empty;

            foreach (var partition in result)
            {
                foreach (var phoneNumber in partition)
                {
                    csvBuilder += $"{phoneNumber.PhoneNumber},{phoneNumber.ActivationDate.ToString("yyyy-MM-dd")}\n";
                    count++;
                    if (count == rowsToWrite)
                    {
                        WriteToFile(csvBuilder, filePath);

                        csvBuilder = string.Empty;
                        count = 0;
                    }
                }
            }

            WriteToFile(csvBuilder, filePath);
        }

        private void WriteToFile(string csvBuilder, string filePath)
        {
            try
            {
                using (System.IO.StreamWriter file = new StreamWriter(@filePath, true))
                {
                    file.WriteLine(csvBuilder);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Something happens during exporting CSV file.", ex);
                // Log exception
            }
        }

        // Process each partition and return a list of PhoneNumber vs ActivationDate
        // Task might take a long time so I use async
        private async Task<List<PhoneNumberDate>> ParsePartition(Dictionary<string, List<DatePair>> dict)
        {
            List<PhoneNumberDate> res = new List<PhoneNumberDate>();
            foreach (var entry in dict)
            {
                // Sort by start date time
                if (entry.Value.Count > 0)
                {
                    var startEndPair = GetMergedStartEndPair(entry.Value);
                    res.Add(new PhoneNumberDate(entry.Key, startEndPair.ActivationDate));
                }
            }
            return res;
        }

        // get the latest interval Activation-Deactivation Date
        public DatePair GetMergedStartEndPair(List<DatePair> pairs)
        {
            pairs.Sort((a, b) => a.ActivationDate.CompareTo(b.ActivationDate));
            var tempPair = pairs[0];
            for (int i = 1; i < pairs.Count; i++)
            {
                if (tempPair.DeactivationDate < pairs[i].ActivationDate)
                {
                    tempPair = pairs[i];
                }
                else
                {
                    tempPair.DeactivationDate = tempPair.DeactivationDate > pairs[i].DeactivationDate ?
                        tempPair.DeactivationDate : pairs[i].DeactivationDate;
                }
            }
            return tempPair;
        }

        // check if all headers exist and in the right order
        public bool AllHeadersExist(string headerRow)
        {
            string[] headersFromCsv = headerRow.Split(',');
            if (Headers.Length != headersFromCsv.Length)
            {
                return false;
            }

            for (int i = 0; i < Headers.Length; i++)
            {
                if (!headersFromCsv[i].ToUpper().Contains(Headers[i]))
                {
                    return false;
                }
            }

            return true;
        }
    }

    // We can use Tuple<string, DateTime> instead of creating a new class here
    public class PhoneNumberDate
    {
        public PhoneNumberDate()
        {

        }
        public PhoneNumberDate(string phoneNumber, DateTime activationDate)
        {
            PhoneNumber = phoneNumber;
            ActivationDate = activationDate;
        }
        public string PhoneNumber { get; set; }
        public DateTime ActivationDate { get; set; }
    }

    // We can use Tuple<DateTime, DateTime> instead of creating a new class here
    public class DatePair
    {
        public DatePair(DateTime activationDate, DateTime deactivationDate)
        {
            ActivationDate = activationDate;
            DeactivationDate = deactivationDate;
        }
        public DateTime ActivationDate { get; set; }
        public DateTime DeactivationDate { get; set; }
    }
}

