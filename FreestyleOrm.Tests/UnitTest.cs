using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FreestyleOrm;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.Sqlite;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using System.Linq.Expressions;

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
            var table = new D_TEST_TABLE
            {
                ID = 1,
                COL_ONE = "xxx",
                COL_TWO_ONE = "yyy"
            };

            var table2 = new D_TEST_TABLE
            {
                ID = 2,
                COL_ONE = "aaa",
                COL_TWO_ONE = "bbb"
            };

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
            var table = new TestTable
            {
                Id = 1,
                ColOne = "xxx",
                ColTwoOne = "yyy"
            };

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
                    .Query<D_TEST_TABLE>("select * from D_TEST_TABLE")                         
                    .Map(m =>
                    {
                        m.To()
                            .UniqueKeys("ID")
                            .SetEntity((row, root, entity) => { })
                            .ClearRule(x => nameof(x.SetEntity))
                            .SetEntity((row, root, entity) => row.BindEntity(entity));
                    })
                    .Fetch()
                    .ToArray();

                Assert.AreEqual("xxx", testTables[0].COL_ONE);
                Assert.AreEqual("yyy", testTables[0].COL_TWO_ONE);
                
                connection.Close();
            }
        }

        [TestMethod]
        public void Test4()
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

        public class Node
        {
            public int Id { get; set; }
            public string Name { get; set; }            
            public int? ParentId { get; set; }
            public List<Node> Chilrdren { get; set; }
        }

        [TestMethod]
        public void TestTree()
        {            
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var nodes = connection
                    .Query<Node>(@"
                        select * from Node
                    ")
                    .Map(m => m.To().UniqueKeys("Id, ParentId").ReNest(x => x.Chilrdren, x => x.Id, x => x.ParentId))
                    .Fetch().ToArray();                

                connection.Close();
            }
        }

        public class Customer
        {
            public int CustomerId { get; set; }            
            public List<Node> Nodes { get; set; }
        }

        [TestMethod]
        public void TestTree2()
        {            
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var customers = connection
                    .Query<Customer>(@"
                        select * from Customer, Node
                    ")
                    .Map(m => 
                    {
                        m.To().UniqueKeys("CustomerId");
                        m.ToMany(x => x.Nodes).UniqueKeys("Id, ParentId").ReNest(x => x.Chilrdren, x => x.Id, x => x.ParentId);
                    })
                    .Fetch().ToArray();                

                connection.Close();
            }
        }

        public class Product
        {
            public int ProductId { get; set; }            
            public Node Node { get; set; }
        }

        [TestMethod]
        public void TestTree3()
        {            
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var customers = connection
                    .Query<Product>(@"                    
                        select 
                            p.*, 
                            n.*,
                            n2.Id n2_Id, 
                            n2.Name n2_Name, 
                            n2.ParentId n2_ParentId                            
                        from (select *, 1000 nodeId from Product) p
                        left join Node n on p.NodeId = n.Id
                        left join Node n2 on n.Id = n2.ParentId
                    ")
                    .Map(m => 
                    {
                        m.To().UniqueKeys("ProductId");
                        m.ToOne(x => x.Node).UniqueKeys("Id");
                        m.ToMany(x => x.Node.Chilrdren)
                            .UniqueKeys("n2_Id, n2_ParentId")
                            .IncludePrefix("n2_").ReNest(x => x.Chilrdren, x => x.Id, x => x.ParentId);
                    })
                    .Fetch().ToArray();                

                connection.Close();
            }
        }

        public class CustomerFilter
        {
            public bool Any { get; set; }
            public List<int> CustomerIds { get; set; }
            public string LikeCustomerName { get; set; }
            public List<string> SortColumns { get; set; }
            public bool Desc { get; set; }
        }

        [TestMethod]
        public void Test5()
        {
            var filter = new CustomerFilter
            {
                Any = false,
                CustomerIds = new List<int> { 1, 2 },
                LikeCustomerName = "Customer",
                SortColumns = new List<string> { "CustomerId", "CustomerName " },
                Desc = false
            };

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var customers = connection
                    .Query<Customer>(@"
                        select 
                            * 
                        from 
                            Customer 
                        {{filter}} 
                        order by 
                            {{sortColumns}}
                    ")
                    .Spec(s =>
                    {
                        var symbol = filter.Any ? LogicalSymbol.Or : LogicalSymbol.And;

                        s.Predicate("filter", x => $"where {x}")
                            .Expression(symbol, "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)
                            .Expression(symbol, "CustomerName like @LikeCustomerName + '%'", p => p["@LikeCustomerName"] = filter.LikeCustomerName);

                        s.Predicate("sortColumns")
                            .Sort(filter.SortColumns, (x, i) => i == 0 && filter.Desc, defaultValue: "CustomerId");                         
                    })                    
                    .Fetch().ToArray();

                connection.Close();
            }
        }

        [TestMethod]
        public void Test6()
        {
            var filter = new CustomerFilter
            {
                Any = false,                                
                Desc = false
            };

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var customers = connection
                    .Query<Customer>(@"
                        select 
                            * 
                        from 
                            Customer 
                        {{filter}} 
                        order by 
                            {{sortColumns}}
                    ")
                    .Spec(s =>
                    {
                        var symbol = filter.Any ? LogicalSymbol.Or : LogicalSymbol.And;

                        s.Predicate("filter", x => $"where {x}")
                            .Expression(symbol, "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)
                            .Expression(symbol, "CustomerName like @LikeCustomerName + '%'", p => p["@LikeCustomerName"] = filter.LikeCustomerName);

                        s.Predicate("sortColumns")
                            .Sort(filter.SortColumns, (x, i) => i == 0 && filter.Desc, defaultValue: "CustomerId");
                    })
                    .Fetch().ToArray();

                connection.Close();
            }
        }

        [TestMethod]
        public void Test7()
        {
            var filter = new CustomerFilter
            {
                Any = false,
                CustomerIds = new List<int> { 1, 2 },
                LikeCustomerName = "Customer",
                SortColumns = new List<string> { "CustomerId", "CustomerName " },
                Desc = false
            };

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var customers = connection
                    .Query<Customer>(@"
                        select 
                            * 
                        from 
                            Customer 
                        {{filter}} 
                        order by 
                            {{sortColumns}}
                    ")
                    .Spec(s =>
                    {
                        var symbol = filter.Any ? LogicalSymbol.Or : LogicalSymbol.And;

                        s.Predicate("filter", x => $"where {x}")
                            .Expression(symbol, "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)
                            .Expression(symbol, "CustomerName like @LikeCustomerName + '%'", p => p["@LikeCustomerName"] = filter.LikeCustomerName)
                            .Expression(symbol, sp =>
                            {
                                sp
                                    .Expression(LogicalSymbol.Or, "CustomerId in (@OrCustomerIds)", p => p["@OrCustomerIds"] = filter.CustomerIds)
                                    .Expression(LogicalSymbol.Or, "CustomerName like @OrLikeCustomerName + '%'", p => p["@OrLikeCustomerName"] = filter.LikeCustomerName);
                            });

                        s.Predicate("sortColumns")
                            .Sort(filter.SortColumns, (x, i) => i == 0 && filter.Desc, defaultValue: "CustomerId");
                    })
                    .Fetch().ToArray();

                connection.Close();
            }
        }
    }
}
