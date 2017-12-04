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

        public override string GetFormatPropertyName(IMapRule mapRule, string column)
        {
            if (mapRule.EntityType == typeof(TestTable))
            {
                var pascalColumn = ToPascal(column);
                return pascalColumn;
            }
            else
            {
                return base.GetFormatPropertyName(mapRule, column);
            }
        }

        public override string GetTable(IMapRule mapRule)
        {
            if (mapRule.EntityType == typeof(TestTable))
            {
                var snakeTable = ToSnake(mapRule.EntityType.Name);
                return snakeTable;
            }
            else
            {
                return base.GetTable(mapRule);
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

            var table2 = new D_TEST_TABLE();
            table2.ID = 2;
            table2.COL_ONE = "aaa";
            table2.COL_TWO_ONE = "bbb";

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();    
                
                using (var tran = connection.BeginTransaction())
                {
                    var query = connection
                        .Query<D_TEST_TABLE>("select * from D_TEST_TABLE where ID = @id")
                        .Params(p => p["@id"] = table.ID)                        
                        .Transaction(tran);

                    query.Insert(table);
                    query.Insert(table2);

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
                        .Query<TestTable>("select * from D_TEST_TABLE where ID = @id", new MyQueryDefine())
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

        [TestMethod]
        public void Test3()
        {
            Test1();
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();    
                
                var testTables = connection
                    .Query("select * from D_TEST_TABLE")                         
                    .Fetch()
                    .Select(x => new
                    {   
                        Id = x.Get<int>("ID"), 
                        ColOne = x.Get<string>("COL_ONE"), 
                        ColTwoOne = x.Get<string>("COL_TWO_ONE") 
                    })
                    .ToArray();

                Assert.AreEqual("xxx", testTables[0].ColOne);
                Assert.AreEqual("yyy", testTables[0].ColTwoOne);
                
                connection.Close();
            }
        }

        public class NodeBsae
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int Level { get; set; }
            public int ParentId { get; set; }
        }

        public class Node : NodeBsae
        {
            public List<Node> Chilrdren { get; set; } = new List<Node>();
        }

        [TestMethod]
        public void TestTree()
        {            
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var nodeBases = connection
                    .Query<NodeBsae>(@"
                        select *  from Node
                    ")
                    .Fetch().ToArray();

                List<Node> nodes = new List<Node>();
               
                Dictionary<int, int> parentMap = new Dictionary<int, int>();
                Dictionary<int, Node> nodeMap = new Dictionary<int, Node>();

                foreach (var nodeBase in nodeBases)
                {
                    var node = new Node
                    {
                        Id = nodeBase.Id,
                        Name = nodeBase.Name,
                        Level = nodeBase.Level,
                        ParentId = nodeBase.ParentId
                    };

                    nodeMap[nodeBase.Id] = node;
                    parentMap[nodeBase.Id] = nodeBase.ParentId;                    
                }

                foreach(var node in nodeMap.Values)
                {
                    int parentId = parentMap[node.Id];

                    if (nodeMap.ContainsKey(parentId))
                    {
                        Node parentNode = nodeMap[parentId];
                        parentNode.Chilrdren.Add(node);
                    }
                    else
                    {
                        nodes.Add(node);
                    }
                }

                connection.Close();
            }
        }
    }
}
