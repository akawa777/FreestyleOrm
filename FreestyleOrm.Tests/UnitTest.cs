using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FreestyleOrm;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.Sqlite;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;

namespace FreestyleOrm.Tests
{
    public class D_TEST_TABLE
    {
        public int ID { get; set; }
        public string COL_ONE { get; set; }
        public string COL_TWO_ONE { get; set; }
    }

    public class MyQueryDefine : QueryDefine
    {
        private string ToPascal(string text)
        {            
            text = Regex.Replace(
                text.ToLower(),
                @"_\w", 
                match => match.Value.ToUpper().Replace("_", string.Empty));

            return Regex.Replace(
                text,
                @"^\w", 
                match => match.Value.ToUpper());
        }

        private string ToSnake(string text)
        {            
            return "D" + Regex.Replace(
                text,
                @"[A-Z]", 
                match => $"_{match.Value.ToUpper()}").ToUpper();
        }

        public override string GetFormatPropertyName(Type rootEntityType, IMapRule mapRule, string column)
        {
            if (rootEntityType == typeof(TestTable))
            {
                var pascalColumn = ToPascal(column);
                return pascalColumn;
            }
            else
            {
                return base.GetFormatPropertyName(rootEntityType, mapRule, column);
            }
        }

        public override string GetTable(Type rootEntityType, IMapRule mapRule)
        {
            if (rootEntityType == typeof(TestTable))
            {
                var snakeTable = ToSnake(rootEntityType.Name);
                return snakeTable;
            }
            else
            {
                return base.GetTable(rootEntityType, mapRule);
            }
        }
    }

    public class TestTable
    {
        public int Id { get; set; }
        public string ColOne { get; set; }
        public string ColTwoOne { get; set; }    
    }

    [TestClass]
    public class UnitTest : UnitTestByManual
    {
        [TestMethod]
        public void Test1()
        {
            var table = new D_TEST_TABLE();
            table.ID = 1;
            table.COL_ONE = "xxx";
            table.COL_TWO_ONE = "yyy";

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();    
                
                using (var tran = connection.BeginTransaction())
                {
                    var query = connection
                        .Query<D_TEST_TABLE>("select * from D_TEST_TABLE where Id = @id")
                        .Params(p => p["@id"] = table.ID)                        
                        .Transaction(tran);

                    query.Insert(table);

                    var registerdTable = query.Fetch().Single();   

                    Assert.AreEqual("xxx", registerdTable.COL_ONE);
                    Assert.AreEqual("yyy", registerdTable.COL_TWO_ONE);

                    tran.Commit();
                }
                
                connection.Close();
            }
        }
        
        [TestMethod]
        public void Test2()
        {
            var table = new TestTable();
            table.Id = 1;
            table.ColOne = "xxx";
            table.ColTwoOne = "yyy";

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();    
                
                using (var tran = connection.BeginTransaction())
                {
                    var query = connection
                        .Query<TestTable>("select * from D_TEST_TABLE where Id = @id", new MyQueryDefine())
                        .Params(p => p["@id"] = table.Id)
                        .Transaction(tran);

                    query.Insert(table);

                    var registerdTable = query.Fetch().Single();   

                    Assert.AreEqual("xxx", registerdTable.ColOne);
                    Assert.AreEqual("yyy", registerdTable.ColTwoOne);

                    tran.Commit();
                }
                
                connection.Close();
            }
        }
    }
}
