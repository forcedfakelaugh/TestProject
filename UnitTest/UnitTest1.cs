using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using TestProject;

namespace UnitTest
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void Find_Activation_Date()
        {
            CsvService service = new CsvService();
            List<DatePair> pairs = new List<DatePair>()
            {
                new DatePair(new DateTime(2010,04,01), new DateTime(2010,06,01)),
                new DatePair(new DateTime(2019,04,01), new DateTime(2019,06,01)),
                new DatePair(new DateTime(2019,06,01), new DateTime(2019,09,01)),
            };

            var result = service.GetMergedStartEndPair(pairs);

            Assert.AreEqual(result.ActivationDate.Date, new DateTime(2019, 04, 01).Date);
        }
    }
}
