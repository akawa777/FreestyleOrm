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
using System.Collections;

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
        public void TestTree1()
        {            
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var nodes = connection
                    .Query<Node>(@"
                        select * from Node
                    ")
                    .Map(m => m.To().UniqueKeys("Id, ParentId").ReNest(x => x.Chilrdren, x => new { x.Id }, x => new { x.ParentId }))
                    .Fetch().ToArray();

                ValidationNodes(nodes);

                connection.Close();
            }
        }

        private void ValidationNodes(Node[] nodes)
        {
            Assert.AreEqual(2, nodes.Count());
            Assert.AreEqual(nodes[0].Id, nodes[0].Chilrdren[0].ParentId);
            Assert.AreEqual(nodes[0].Chilrdren[0].Id, nodes[0].Chilrdren[0].Chilrdren[0].ParentId);
            Assert.AreEqual(nodes[0].Id, nodes[0].Chilrdren[1].ParentId);
            Assert.AreEqual(nodes[1].Id, nodes[1].Chilrdren[0].ParentId);            
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

                var customersCount = connection.Query("select * from Customer").Fetch().Count();

                var customers = connection
                    .Query<Customer>(@"
                        select * from Customer, Node order by CustomerId
                    ")
                    .Map(m => 
                    {
                        m.To().UniqueKeys("CustomerId");
                        m.ToMany(x => x.Nodes).UniqueKeys("Id, ParentId").ReNest(x => x.Chilrdren, x => x.Id, x => x.ParentId);
                    })
                    .Fetch().ToArray();

                Assert.AreEqual(customersCount, customers.Count());                

                foreach (var customer in customers)
                {
                    var nodes = customer.Nodes;
                    ValidationNodes(nodes.ToArray());
                }

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
                        from (select *, 1000 NodeId from Product) p
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

                Assert.AreEqual(1000, customers[0].Node.Id);

                foreach (var node in customers[0].Node.Chilrdren)
                {
                    Assert.AreEqual(customers[0].Node.Id, node.ParentId);
                }

                connection.Close();
            }
        }

        public class CustomerFilter
        {
            public bool Any { get; set; }
            public List<int> CustomerIds { get; set; }
            public string CustomerName { get; set; }
            public List<string> SortColumns { get; set; }
            public bool Desc { get; set; }
        }

        [TestMethod]
        public void TestSpec1()
        {
            var filter = new CustomerFilter
            {
                Any = false,
                CustomerIds = new List<int> { 1, 2 },
                CustomerName = "CustomerName_1",
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
                        {{filters}} 
                        order by {{sortColumns}}
                    ")
                    .Spec(s =>
                    {
                        var symbol = filter.Any ? LogicalSymbol.Or : LogicalSymbol.And;

                        s.Predicate("filters", x => $"where {x}", prettySpace: "                            ")                        
                            .Satify(symbol, $"CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)
                            .Satify(symbol, $"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName);

                        s.Predicate("sortColumns", prettySpace: "                            ")                        
                            .Sort(filter.SortColumns, (x, i) => i == 0 && filter.Desc, defaultSql: "CustomerId");                         
                    })                    
                    .Fetch().ToArray();

                Assert.AreEqual(1, customers.Length);

                connection.Close();
            }
        }

        [TestMethod]
        public void TestSpec2()
        {
            var filter = new CustomerFilter
            {
                Any = false,                                
                Desc = false
            };

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var count = connection.Query("select * from Customer").Fetch().Count();

                var customers = connection
                    .Query<Customer>(@"
                        select 
                            * 
                        from 
                            Customer 
                        {{filters}} 
                        order by 
                            {{sortColumns}}
                    ")
                    .Spec(s =>
                    {
                        var symbol = filter.Any ? LogicalSymbol.Or : LogicalSymbol.And;

                        s.Predicate("filters", x => $"where {x}")
                            .Satify(symbol, "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)
                            .Satify(symbol, "CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName);

                        s.Predicate("sortColumns")
                            .Sort(filter.SortColumns, (x, i) => i == 0 && filter.Desc, defaultSql: "CustomerId");
                    })
                    .Fetch().ToArray();

                Assert.AreEqual(count, customers.Length);

                connection.Close();
            }
        }

        [TestMethod]
        public void TestSpec3()
        {
            var filter = new CustomerFilter
            {
                Any = false,
                CustomerIds = new List<int> { 1, 2 },                
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
                        {{filters}} 
                        order by {{sortColumns}}
                    ")
                    .Spec(s =>
                    {
                        var symbol = filter.Any ? LogicalSymbol.Or : LogicalSymbol.And;

                        s.Predicate("filters", x => $"where {x}", prettySpace: "                            ")
                            .Satify(symbol, "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)
                            .Satify(symbol, "CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)
                            .Satify(LogicalSymbol.Or, sp =>
                            {
                                sp
                                    .Satify(LogicalSymbol.Or, "CustomerId in (@OrCustomerIds)", p => p["@OrCustomerIds"] = filter.CustomerIds)
                                    .Satify(symbol, "CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)
                                    .Satify(LogicalSymbol.Or, spp =>
                                    {
                                        spp
                                            .Satify(LogicalSymbol.Or, "CustomerId in (@OrCustomerIds)", p => p["@OrCustomerIds"] = filter.CustomerIds)
                                            .Satify(symbol, "CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName);
                                    })
                                    .Satify(LogicalSymbol.And, spp =>
                                    {
                                        spp
                                            .Satify(LogicalSymbol.Or, "CustomerId in (@OrCustomerIds)", p => p["@OrCustomerIds"] = filter.CustomerIds)
                                            .Satify(symbol, "CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName);
                                    });
                            })
                            .Satify(symbol, "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)
                            .Satify(symbol, "CustomerName = @CustomerNam", p => p["@CustomerName"] = filter.CustomerName);

                        s.Predicate("sortColumns", prettySpace: "                            ")
                            .Sort(filter.SortColumns, (x, i) => i == 0 && filter.Desc, defaultSql: "CustomerId");
                    })
                    .Fetch().ToArray();

                Assert.AreEqual(2, customers.Length);

                connection.Close();
            }
        }

        public class SqlExpression
        {
            public virtual string Subject { get; set; }
            public virtual string Symbol { get; set; }
            public virtual List<object> Values { get; set; } = new List<object>();
        }

        public class SqlExpressionProxy : SqlExpression
        {
            public SqlExpressionProxy(SqlExpression sqlExpression, int paramNo)
            {
                foreach (var prop in sqlExpression.GetType().GetProperties())
                {
                    prop.SetValue(this, prop.GetValue(sqlExpression));
                }

                var sql = string.Empty;
                var paramMap = new Dictionary<string, object>();

                if (Symbol == "=")
                {
                    sql = $"{Subject} = @{Subject}_{paramNo}";
                    paramMap[$"@{Subject}_{paramNo}"] = Values[0];
                }                
                else if (Symbol == "in")
                {
                    sql = $"{Subject} in (@{Subject}_{paramNo})";
                    paramMap[$"@{Subject}_{paramNo}"] = Values;
                }
                else if (Symbol == "between")
                {
                    sql = $"{Subject} between @{Subject}_{paramNo}_1 and @{Subject}_{paramNo}_2";
                    paramMap[$"@{Subject}_{paramNo}_1"] = Values[0];
                    paramMap[$"@{Subject}_{paramNo}_2"] = Values[1];
                }

                Sql = sql;
                Params = paramMap;
            }

            public string Sql { get; }
            public Dictionary<string, object> Params { get; }
        }

        public class SqlExpressionResolver : List<SqlExpressionProxy>
        {
            public SqlExpressionResolver()
            {

            }
            
            public SqlExpressionResolver(IEnumerable<SqlExpression> sqlExpressions)
            {
                ReSet(sqlExpressions);
            }

            private int _no = 1;

            public void ReSet(IEnumerable<SqlExpression> sqlExpressions)
            {
                this.Clear();
                
                foreach (var item in sqlExpressions)
                {
                    this.Add(new SqlExpressionProxy(item, _no));
                    _no++;
                }
            }
        }

        public class Filter2
        {
            public List<SqlExpression> Filters { get; set;} = new List<SqlExpression>();
            public List<SqlExpression> UnionFilters { get; set; } = new List<SqlExpression>();
        }

        [TestMethod]
        public void TestSpec4()
        {
            var filter2 = new Filter2();

            filter2.Filters.Add(new SqlExpression{ Subject = "CustomerId", Symbol = "=", Values = { 1 } });            
            filter2.Filters.Add(new SqlExpression{ Subject = "CustomerId", Symbol = "between", Values = { 1, 2 } });
            filter2.UnionFilters.Add(new SqlExpression{ Subject = "CustomerId", Symbol = "in", Values = { 1, 2, 3 } });

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();                

                var customers = connection
                    .Query<Customer>(@"
                        select 
                            {{selectColumns}} 
                        from 
                            {{table}} 
                        {{filters}}
                        order by
                            {{sortColumns}}
                    ")
                    .Spec(s =>
                    {
                        s.Predicate("table")
                            .Text("Customer");

                        s.Predicate("selectColumns" , prettySpace: "                            ")
                            .Comma(new string[] { "CustomerId", "CustomerName", "RecordVersion" });

                        var sp = s.Predicate("filters", x => $"where {x}", prettySpace: "                            ");

                        var sqlExpressionResolver = new SqlExpressionResolver();

                        sp.Satify(LogicalSymbol.And, spp =>
                        {
                            sqlExpressionResolver.ReSet(filter2.Filters);
                            foreach (var sqlExpression in sqlExpressionResolver)
                            {
                                spp.Satify(LogicalSymbol.And, sqlExpression.Sql, p => p.AddMap(sqlExpression.Params));
                            }
                        });

                        sp.Satify(LogicalSymbol.Or, spp =>
                        {
                            sqlExpressionResolver.ReSet(filter2.UnionFilters);
                            foreach (var sqlExpression in sqlExpressionResolver)
                            {
                                spp.Satify(LogicalSymbol.And, sqlExpression.Sql, p => p.AddMap(sqlExpression.Params));
                            }
                        });   

                        s.Predicate("sortColumns")                     
                            .Sort(new string[] { "CustomerId", "CustomerName" });
                    })
                    .Fetch().ToArray();

                Assert.AreEqual(3, customers.Length);

                connection.Close();
            }
        }

        public void TestSpec5()
        {
            var filter = new CustomerFilter
            {
                Any = false,
                CustomerIds = new List<int> { 1, 2 },
                CustomerName = "CustomerName_1",
                SortColumns = new List<string> { "CustomerId", "CustomerName " },
                Desc = false
            };

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var customers = connection
                    .Query<Customer>($@"
                        select 
                            * 
                        from 
                            Customer                             
                        where
                            ID > 0
                            {(filter.CustomerIds.Count > 0 ? "and CustomerId in (@CustomerIds)" : string.Empty)}
                            {(!string.IsNullOrEmpty(filter.CustomerName) ? "and CustomerName = @CustomerName" : string.Empty)}
                        order by
                            {(filter.SortColumns.Count > 0 ? string.Join(",", filter.SortColumns) : "CustomerId")}
                    ")
                    .Fetch().ToArray();

                Assert.AreEqual(1, customers.Length);

                connection.Close();
            }
        }

        public void TestSpec6()
        {
            var filter = new CustomerFilter
            {
                Any = false,
                CustomerIds = new List<int> { 1, 2 },
                CustomerName = "CustomerName_1",
                SortColumns = new List<string> { "CustomerId", "CustomerName " },
                Desc = false
            };

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                Spec spec = null;

                var customers = connection
                    .Query<Customer>($@"
                        select 
                            * 
                        from 
                            Customer                             
                        where
                            ID > 0
                            and CustomerId in (@CustomerIds)
                            {spec.Satisfy("and", "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)}
                            {spec.Satisfy("and", "CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                            {spec.Satisfy("and", "CustomerId between @CustomerName and @CustomerName", p => { p["@CustomerName"] = filter.CustomerName; p["@CustomerName"] = filter.CustomerName; })}
                            {spec.Satisfy("and", "CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                            {spec.Satisfy("and", "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)}
                            {spec.Satisfy("and", "CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                            {spec.Satisfy("and", "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)}
                            {spec.Satisfy("and", "CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                            {spec.Satisfy("and", innerSpec => 
                            {
                                innerSpec
                                    .Satisfy("and", "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds)
                                    .Satisfy("and", "CustomerId between @CustomerName and @CustomerName", p => { p["@CustomerName"] = filter.CustomerName; p["@CustomerName"] = filter.CustomerName; })
                                    .Satisfy("and", inner2Spec =>
                                    {
                                        inner2Spec.Satisfy("and", "CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds);
                                    });
                            })}
                        order by
                            CustomerId                         
                            
                    ")                    
                    .Fetch().ToArray();

                Assert.AreEqual(1, customers.Length);

                connection.Close();
            }
        }

        public interface Spec
        {
            string Satisfy(string symbol, string sql, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null, string defaultSql = null);
            string Satisfy(string symbol, Action<InnerSpec> innerSpec);
        }

        public interface InnerSpec
        {
            InnerSpec Satisfy(string symbol, string sql, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null, string defaultSql = null);
            InnerSpec Satisfy(string symbol, Action<InnerSpec> innerSpec);
        }

        public void TestSpec7()
        {
            var filter = new CustomerFilter
            {
                Any = false,
                CustomerIds = new List<int> { 1, 2 },
                CustomerName = "CustomerName_1",
                SortColumns = new List<string> { "CustomerId", "CustomerName " },
                Desc = false
            };

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                ISqlBuilder sqlBuilder = null;

                var customers = connection
                    .Query<Customer>(sqlBuilder.Write(spec => $@"
                        select 
                            * 
                        from 
                            {spec.If("Customer")}    

                        {spec.Begin("where")}                            
                            {spec.If($"CustomerName = {spec.Param(() => filter.CustomerName)}")}
                            {spec.If("and", $"CustomerName = {spec.Param(() => filter.CustomerName)}")}                            

                            {spec.BeginScpoe("and")}
                                {spec.If($"CustomerName = {spec.Param(() => filter.CustomerName)}")}
                                {spec.If("and", $"CustomerName = {spec.Param(() => filter.CustomerName)}")}

                                {spec.BeginScpoe("or")}
                                    {spec.If($"CustomerName = {spec.Param(() => filter.CustomerName)}")}
                                    {spec.If("and", $"CustomerName = {spec.Param(() => filter.CustomerName)}")}
                                {spec.End}                            
                            {spec.End}                            

                            {spec.If("and", $"CustomerName = {spec.Param(() => filter.CustomerName)}")}
                            {spec.If("and", $"CustomerName = {spec.Param(() => filter.CustomerName)}")}
                        {spec.End}
                        
                        order by
                            {spec.Comma(filter.SortColumns, defaultText: "CustomerId")}
                    "))
                    .Fetch().ToArray();

                Assert.AreEqual(1, customers.Length);

                connection.Close();
            }
        }

        public void TestSpec8()
        {
            var filter = new CustomerFilter
            {
                Any = false,
                CustomerIds = new List<int> { 1, 2 },
                CustomerName = "CustomerName_1",
                SortColumns = new List<string> { "CustomerId", "CustomerName " },
                Desc = false
            };

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                ISqlBuilder sqlBuilder = null;

                var customers = connection
                    .Query<Customer>(sqlBuilder.Write(spec => $@"
                        select 
                            * 
                        from 
                            {spec.If("Customer")}    

                        {spec.Begin("where")}                            
                            {spec.If($"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                            {spec.If("and", $"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                            {spec.If("and", $"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                            {spec.If("and", $"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                            {spec.If("and", $"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                            {spec.If("and", $"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}

                            {spec.BeginScpoe("and")}
                                {spec.If($"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                                {spec.If("and", $"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}

                                {spec.BeginScpoe("or")}
                                    {spec.If($"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                                    {spec.If("and", $"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                                {spec.End}                            
                            {spec.End}                            

                            {spec.If($"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                            {spec.If("and", $"CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName)}
                        {spec.End}
                        
                        order by
                            {spec.Comma(filter.SortColumns, defaultText: "CustomerId")}
                    "))
                    .Fetch().ToArray();

                Assert.AreEqual(1, customers.Length);

                connection.Close();
            }
        }

        public interface ISqlBuilder
        {
            string Write(Func<ISqlSpec, string> text);            
        }

        public interface ISqlSpec
        {
            string If(string split, string text, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null, string defaultText = null);
            string If(string text, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null, string defaultText = null);
            string Param(Expression<Func<object>> value, Func<bool> validation = null, object defaultValue = null);                        
            string Begin(string split = null);
            string BeginScpoe(string split = null);            
            string End { get; }
            string Comma(IEnumerable<string> columns, string defaultText = null);
            
        }

        //public class TextBuilder : ITextBuilder
        //{
        //    public string Write(Func<ITextSpec, string> setText)
        //    {
        //        TextSpec textSpec = new TextSpec();
        //        string text = setText(textSpec);
        //        return text;
        //    }
        //}

        //public class TextSpec : ITextSpec
        //{
        //    public TextSpec()
        //    {
        //        GetNewLevelComment();
        //    }
        //    private Dictionary<int, string> _levelMap = new Dictionary<int, string>();
        //    private bool _enableSplit = false;
        //    private Dictionary<string, object> _paramMap = new Dictionary<string, object>();
        //    private string GetNewLevelComment()
        //    {
        //        int level = _levelMap.Count == 0 ? 1 : _levelMap.Keys.Max(x => x) + 1;
        //        _levelMap[level] = $"---level{level}---{Guid.NewGuid().ToString()}---";

        //        return _levelMap[level];
        //    }
        //    private bool TrySetParams(string text, Action<Dictionary<string, object>> setParams, Func<bool> validation, string defaultText, out string formatedText, out Dictionary<string, object> paramMap)
        //    {
        //        formatedText = null;
        //        paramMap = new Dictionary<string, object>();

        //        if (string.IsNullOrEmpty(text) && string.IsNullOrEmpty(defaultText)) return false;

        //        if (string.IsNullOrEmpty(text)) formatedText = defaultText;
        //        else formatedText = text;            

        //        if (validation != null)
        //        {
        //            if (validation()) 
        //            {
        //                setParams(paramMap);
        //                return true;
        //            }
        //            else
        //            {
        //                return false;
        //            }
        //        }

        //        try
        //        {
        //            setParams(paramMap);

        //            if (validation != null) return true;

        //            foreach (var entry in paramMap)
        //            {
        //                if (entry.Value == null) return false;
        //                if (entry.Value.GetType() == typeof(string) && entry.Value.ToString() == string.Empty) return false;
        //                if (entry.Value is IEnumerable elements)
        //                {
        //                    bool hasElement = false;
        //                    foreach (var element in elements)
        //                    {                                
        //                        hasElement = true;
        //                        break;
        //                    }

        //                    if (!hasElement) return false;
        //                }
        //            }
        //        }
        //        catch
        //        {
        //            return false;
        //        }

        //        return true;
        //    }
        //    public string If(string split, string text, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null, string defaultText = null)
        //    {
        //        if (split == null) throw new Exception("split is required.");

        //        string formattedText = null;
        //        Dictionary<string, object> paramMap = new Dictionary<string, object>();

        //        if (!TrySetParams(text, setParams, validation, defaultText, out formattedText, out paramMap))                
        //        {
        //            return string.Empty;
        //        }

        //        if (_enableSplit && split != null) return split + formattedText;
        //        else
        //        {
        //            if (split != null) _enableSplit = true;
        //            return formattedText;
        //        }
        //    }

        //    public string If(string text, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null, string defaultText = null)
        //    {
        //        return If(null, text, setParams, validation, defaultText);
        //    }

        //    public string Begin(string split, string text)
        //    {
        //        if (split == null) throw new Exception("split is required.");
        //        if (text == null) throw new Exception("text is required.");

        //        string levelComment = GetNewLevelComment();

        //        if (_enableSplit && split != null) return levelComment + split + text;
        //        else
        //        {
        //            if (split != null) _enableSplit = true;
        //            return levelComment + text;
        //        }
        //    }

        //    public string Begin(string text = null)
        //    {
        //        string levelComment = GetNewLevelComment();

        //        if (string.IsNullOrEmpty(text)) return levelComment;
        //        else return levelComment + text;
        //    }

        //    public string End(string text = null)
        //    {
        //        int level = _levelMap.Keys.Max(x => x);
        //        string levelComment = _levelMap[level];
        //        _levelMap.Remove(level);

        //        if (string.IsNullOrEmpty(text)) return levelComment;
        //        else return text + levelComment;                
        //    }
        //}
    }
}
