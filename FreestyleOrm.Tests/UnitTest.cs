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
using FreestyleOrm.Core;

namespace FreestyleOrm.Tests
{

    [TestClass]
    public class UnitTest : UnitTestBase
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

            public override void EndModifingRecord(ModifingEntry modifingEntry)
            {
                
            }
        }

        public class TestTable
        {
            public int Id { get; set; }
            public string ColOne { get; set; }
            public string ColTwoOne { get; set; }
        }

        [TestMethod]
        public void Test_SnakeToPascal1()
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
                        .Map(m => m.ToRoot().Writable())
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
        public void Test_SnakeToPascal2()
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
                        .Map(m => m.ToRoot().Writable())
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
        public void Test_SnakeToPascal3()
        {
            Test_SnakeToPascal1();
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var testTables = connection
                    .Query<D_TEST_TABLE>("select * from D_TEST_TABLE")
                    .Map(m =>
                    {
                        m.ToRoot()
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
        public void Test_SnakeToPascal4()
        {
            Test_SnakeToPascal1();
            using (var connection = _testInitializer.CreateConnection())
            {                
                connection.Open();

                var testTables = connection
                    .Query("select * from D_TEST_TABLE")
                    .Fetch()
                    .Select(x => new
                    {
                        Id = x["ID"].ToString(),
                        ColOne = x["COL_ONE"].ToString(),
                        ColTwoOne = x["COL_TWO_ONE"].ToString()
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
        public void Test_Tree1()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var nodes = connection
                    .Query<Node>(@"
                        select * from Node
                    ")
                    .Map(m =>
                    {
                        m.ToRoot()
                            .UniqueKeys("Id, ParentId")
                            .ReNest(x => x.Chilrdren, x => new { x.Id }, x => new { x.ParentId });
                    })
                    .Fetch()
                    .ToArray();

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

        [TestMethod]
        public void Test_Tree2()
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
                        m.ToRoot()
                            .UniqueKeys("CustomerId");

                        m.ToMany(x => x.Nodes)
                            .UniqueKeys("Id, ParentId")
                            .ReNest(x => x.Chilrdren, x => x.Id, x => x.ParentId);
                    })
                    .Fetch()
                    .ToArray();

                Assert.AreEqual(customersCount, customers.Count());

                foreach (var customer in customers)
                {
                    var nodes = customer.Nodes;
                    ValidationNodes(nodes.ToArray());
                }

                connection.Close();
            }
        }

        [TestMethod]
        public void Test_Tree3()
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
                        m.ToRoot()
                            .UniqueKeys("ProductId");

                        m.ToOne(x => x.Node)
                            .UniqueKeys("Id");

                        m.ToMany(x => x.Node.Chilrdren)
                            .UniqueKeys("n2_Id, n2_ParentId")
                            .IncludePrefix("n2_")
                            .ReNest(x => x.Chilrdren, x => x.Id, x => x.ParentId);
                    })
                    .Fetch()
                    .ToArray();

                Assert.AreEqual(1000, customers[0].Node.Id);

                foreach (var node in customers[0].Node.Chilrdren)
                {
                    Assert.AreEqual(customers[0].Node.Id, node.ParentId);
                }

                connection.Close();
            }
        }

        public class Customer
        {
            public int CustomerId { get; set; }
            public List<Node> Nodes { get; set; }
        }

        public class Product
        {
            public int ProductId { get; set; }
            public Node Node { get; set; }
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
        public void Test_Spec()
        {
            var filter = new CustomerFilter
            {
                Any = false,
                CustomerIds = new List<int> { 1, 2 },
                CustomerName = "CustomerName_1",
                SortColumns = new List<string> { "CustomerId", "CustomerName " },
                Desc = false
            };

            var spec1 = new IfSpec("CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds);
            var spec2 = new IfSpec("CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName);

            var and1Spec = new AndSpec(spec1, spec2);
            var and2Spec = new AndSpec(spec1, spec2);

            var orSpec = new OrSpec(and1Spec, and2Spec);

            var whereSpec = new WhereSpec(orSpec, "                        ");            

            var selelctSpec = new CommaSpec(new string[] { "CustomerId", "CustomerName", "@param1 as param1" }, p => p["@param1"] = 1);
            var sortSpec = new CommaSpec(filter.SortColumns, defaultPredicate: "CustomerId");

            string sql = $@"
                        select 
                            {selelctSpec}
                        from 
                            Customer
                        {whereSpec}
                        order by
                            {sortSpec} ";

            Console.Write(sql);

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var customersCount = connection.Query("select * from Customer").Fetch().Count();

                var customers = connection
                    .Query<Customer>(sql)
                    .Params(p =>
                    {
                        p.AddMap(whereSpec);
                        p.AddMap(selelctSpec);
                    })
                    .Fetch()
                    .ToArray();

                Assert.AreEqual(1, customers.Count());

                connection.Close();
            }
        }

        [TestMethod]
        public void Test_Spec2()
        {
            var filter = new CustomerFilter
            {
                Any = false,
                SortColumns = new List<string> { "CustomerId", "CustomerName " },
                Desc = false
            };

            var spec1 = new IfSpec("CustomerId in (@CustomerIds)", p => p["@CustomerIds"] = filter.CustomerIds);
            var spec2 = new IfSpec("CustomerName = @CustomerName", p => p["@CustomerName"] = filter.CustomerName);

            var and1Spec = new AndSpec(spec1, spec2);
            var and2Spec = new AndSpec(spec1, spec2);

            var orSpec = new OrSpec(and1Spec, and2Spec);

            var whereSpec = new WhereSpec(orSpec);

            var selelctSpec = new CommaSpec(new string[] { "CustomerId", "CustomerName", "@param1 as param1" }, p => p["@param1"] = 1);
            var sortSpec = new CommaSpec(filter.SortColumns, defaultPredicate: "CustomerId");

            string sql = $@"
                        select 
                            {selelctSpec}
                        from 
                            Customer
                        {whereSpec}
                        order by
                            {sortSpec} ";

            Console.Write(sql);

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var customersCount = connection.Query("select * from Customer").Fetch().Count();

                var customers = connection
                    .Query<Customer>(sql)
                    .Params(p =>
                    {
                        p.AddMap(whereSpec);
                        p.AddMap(selelctSpec);
                    })
                    .Fetch()
                    .ToArray();

                Assert.AreEqual(customersCount, customers.Count());

                connection.Close();
            }
        }


        public abstract class Entity
        {
            public string LastUpdate { get; private set; }
        }

        public abstract class AggregateRoot : Entity
        {
            public virtual void Register()
            {
                var property = this.GetType().GetProperty("LastUpdate").DeclaringType.GetProperty("LastUpdate");

                property.SetValue(this, DateTime.Now.ToString("yyyyMMddmmss"));
            }
        }

        public class Root : AggregateRoot
        {
            public int RootId { get; set; }
            public string Text { get; set; }
            public List<Many> ManyList { get; private set; } = new List<Many>();
            public List<Many2> Many2List { get; private set; } = new List<Many2>();
            public One One { get; set; }
        }

        public class Many : Entity
        {
            public int RootId { get; set; }
            public int ManyId { get; set; }
            public string Text { get; set; }
            public int AttrFlg { get; set; }
            public List<Attr> AttrList { get; set; }
        }

        public class One : Entity
        {
            public int RootId { get; set; }
            public string Text { get; set; }
        }

        public class Many2 : Entity
        {
            public int RootId { get; set; }
            public int Many2Id { get; set; }
            public string Text { get; set; }
            public Many2One Many2One { get; set; }
            public int AttrFlg { get; set; }
            public List<Attr> AttrList { get; set; }
        }

        public class Many2One : Entity
        {
            public int RootId { get; set; }
            public int Many2Id { get; set; }
            public string Text { get; set; }
        }

        public class Attr : Entity
        {
            public int AttrId { get; set; }
            public int AttrFlg { get; set; }
        }

        public class RelationQueryDefine : QueryDefine
        {
            public override void SetRow(IMapRule mapRule, object entity, object rootEntity, IRow row)
            {
                base.SetRow(mapRule, entity, rootEntity, row);
                row["LastUpdate"] = (rootEntity as AggregateRoot).LastUpdate;
            }
        }

        [TestMethod]
        public void Test_Relation()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                using (var tran = connection.BeginTransaction())
                {
                    var root = new Root();
                    root.RootId = 1;
                    root.Text = $"{nameof(Root)}";
                    root.Register();

                    var query = CreateQueryOfRoot(root.RootId, connection);

                    query.Transaction(tran);

                    query.Insert(root);

                    tran.Commit();
                }

                connection.Close();
            }

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var query = CreateQueryOfRoot(1, connection);

                var root = query.Fetch().Single();

                Assert.AreEqual(0, root.ManyList.Count);
                Assert.AreEqual(0, root.Many2List.Count);
                Assert.AreEqual(true, root.One == null);

                connection.Close();
            }

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                using (var tran = connection.BeginTransaction())
                {
                    var query = CreateQueryOfRoot(1, connection);

                    query.Transaction(tran);

                    var root = query.Fetch().Single();

                    root.One = new One { RootId = root.RootId, Text = $"{nameof(One)}" };

                    for (var i = 0; i < 3; i++)
                    {
                        var many = new Many { RootId = root.RootId, ManyId = i + 1, Text = $"{nameof(Many)}_{i + 1}" };
                        many.AttrFlg = 1;
                        root.ManyList.Add(many);
                    }

                    for (var i = 0; i < 5; i++)
                    {
                        var many2 = new Many2 { RootId = root.RootId, Many2Id = i + 1, Text = $"{nameof(Many2)}_{i + 1}" };
                        many2.Many2One = new Many2One { RootId = root.RootId, Many2Id = i + 1, Text = $"{nameof(Many2One)}_{i + 1}" };
                        many2.AttrFlg = 2;
                        root.Many2List.Add(many2);
                    }

                    query.Update(root);

                    tran.Commit();
                }

                connection.Close();
            }

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var query = CreateQueryOfRoot(1, connection);

                var root = query.Fetch().Single();

                Assert.AreEqual(3, root.ManyList.Count);
                Assert.AreEqual(true, root.ManyList.Any(x => x.AttrList.Count == 2));
                Assert.AreEqual(true, root.ManyList.Any(x => x.AttrList.Any(y => y.AttrId == 1 || y.AttrId == 2)));                

                Assert.AreEqual(5, root.Many2List.Count);
                Assert.AreEqual(true, root.Many2List.All(x => x.Many2One != null));
                Assert.AreEqual(true, root.Many2List.Any(x => x.AttrList.Count == 2));
                Assert.AreEqual(true, root.Many2List.Any(x => x.AttrList.Any(y => y.AttrId == 3 || y.AttrId == 4)));

                Assert.AreEqual(false, root.One == null);

                connection.Close();
            }

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                using (var tran = connection.BeginTransaction())
                {
                    var query = CreateQueryOfRoot(1, connection);

                    query.Transaction(tran);

                    var root = query.Fetch().Single();

                    root.ManyList.ForEach(x => x.Text += "_many_update");
                    root.Many2List.ForEach(x => x.Text += "_many2_update");

                    query.Update(root);

                    tran.Commit();
                }

                connection.Close();
            }

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var query = CreateQueryOfRoot(1, connection);

                var root = query.Fetch().Single();

                Assert.AreEqual(3, root.ManyList.Count);
                Assert.AreEqual(true, root.ManyList.All(x => x.Text.EndsWith("_many_update")));
                Assert.AreEqual(true, root.ManyList.Any(x => x.AttrList.Count == 2));
                Assert.AreEqual(true, root.ManyList.Any(x => x.AttrList.Any(y => y.AttrId == 1 || y.AttrId == 2)));

                Assert.AreEqual(5, root.Many2List.Count);
                Assert.AreEqual(true, root.Many2List.All(x => x.Text.EndsWith("_many2_update")));
                Assert.AreEqual(true, root.Many2List.Any(x => x.AttrList.Count == 2));
                Assert.AreEqual(true, root.Many2List.Any(x => x.AttrList.Any(y => y.AttrId == 3 || y.AttrId == 4)));

                Assert.AreEqual(false, root.One == null);

                connection.Close();
            }
        }

        private IQuery<Root> CreateQueryOfRoot(int rootId, IDbConnection connection)
        {

            var query = connection.Query<Root>(
                @"
                    select 
                        Root.RootId
                        ,Root.Text Text
                        ,Root.LastUpdate
                        
                        ,Many.ManyId Many_ManyId 
                        ,Many.Text Many_Text
                        ,Many.AttrFlg Many_AttrFlg
                        ,Many.LastUpdate Many_LastUpdate

                        ,ManyAttr.AttrId ManyAttr_AttrId
                        ,ManyAttr.AttrFlg ManyAttr_AttrFlg
                        ,ManyAttr.LastUpdate ManyAttr_LastUpdate
                        
                        ,One.Text One_Text              
                        ,One.LastUpdate One_LastUpdate
                        
                        ,Many2.Many2Id Many2_Many2Id 
                        ,Many2.Text Many2_Text
                        ,Many2.AttrFlg Many2_AttrFlg
                        ,Many2.LastUpdate Many2_LastUpdate
                        
                        ,Many2One.Many2Id Many2One_Many2Id                         
                        ,Many2One.Text Many2One_Text
                        ,Many2One.LastUpdate Many2One_LastUpdate

                        ,Many2Attr.AttrId Many2Attr_AttrId
                        ,Many2Attr.AttrFlg Many2Attr_AttrFlg
                        ,Many2Attr.LastUpdate Many2Attr_LastUpdate
                    from 
                        Root

                    left join 
                        Many 
                    on 
                        Root.RootId = Many.RootId   
                    left join
                        Attr ManyAttr
                    on
                        Many.AttrFlg = ManyAttr.AttrFlg

                    left join 
                        One 
                    on 
                        Root.RootId = One.RootId 

                    left join 
                        Many2
                    on 
                        Root.RootId = Many2.RootId
                    left join
                        Many2One
                    on
                        Many2.RootId = Many2One.RootId
                        and Many2.Many2Id = Many2One.Many2Id
                    left join
                        Attr Many2Attr
                    on
                        Many2.AttrFlg = Many2Attr.AttrFlg

                    where 
                        Root.RootId = @RootId

                    order by
	                    Root.RootId	                    
                        ,Many.ManyId
	                    ,ManyAttr_AttrId
                        ,Many2.Many2Id
	                    ,Many2Attr_AttrId",
                new RelationQueryDefine()
                )
                .Map(m =>
                {
                    m.ToRoot()
                        .UniqueKeys("RootId")
                        .Writable();

                    m.ToMany(x => x.ManyList)
                        .UniqueKeys("RootId, Many_ManyId")
                        .Writable()
                        .IncludePrefix("Many_");

                    m.ToMany(x => x.ManyList[0].AttrList)
                        .UniqueKeys("RootId, Many_ManyId, ManyAttr_AttrId")                        
                        .IncludePrefix("ManyAttr_");

                    m.ToOne(x => x.One)
                        .UniqueKeys("RootId, One_LastUpdate")
                        .Writable()
                        .IncludePrefix("One_");

                    m.ToMany(x => x.Many2List)
                         .UniqueKeys("RootId, Many2_Many2Id")
                         .Writable()
                         .IncludePrefix("Many2_");

                    m.ToOne(x => x.Many2List[0].Many2One)
                       .UniqueKeys("RootId, Many2One_Many2Id")
                       .Writable()
                       .IncludePrefix("Many2One_");

                    m.ToMany(x => x.Many2List[0].AttrList)
                        .UniqueKeys("RootId, Many2_Many2Id, Many2Attr_AttrId")
                        .IncludePrefix("Many2Attr_");

                })
                .Params(p =>
                {
                    p["@RootId"] = rootId;
                });

            return query;
        }

        public class ManyAggregate
        {
            public ManyCollection ManyCollection { get; set; }
        }

        public class ManyCollection
        {
            public List<Many> List { get; set; }
        }

        [TestMethod]
        public void Test_CustomCollection()
        {
            var root = new Root();
            root.RootId = 1;
            root.Text = $"{nameof(Root)}";

            for (var i = 0; i < 10; i++)
            {
                var many = new Many
                {
                    RootId = root.RootId,
                    ManyId = i + 1,
                    Text = $"{i + 1}_text"
                };

                root.ManyList.Add(many);
            }

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                using (var tran = connection.BeginTransaction())
                {   
                    root.Register();

                    var query = CreateQueryOfRoot(root.RootId, connection);

                    query.Transaction(tran);

                    query.Insert(root);

                    tran.Commit();
                }

                connection.Close();
            }

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var query = connection.Query<ManyAggregate>(
                    $@"select * from Many where RootId = @rootId order by ManyId")
                    .Map(m =>
                    {
                        m.ToRoot().UniqueKeys("RootId");
                        m.ToOne(r => r.ManyCollection).UniqueKeys("RootId");
                        m.ToMany(r => r.ManyCollection.List).UniqueKeys("RootId, ManyId");
                    })
                    .Params(p =>
                    {
                        p["@rootId"] = root.RootId;
                    });

                var manyAggregate = query.Fetch().ToArray();

                Assert.AreEqual(root.ManyList.Count, manyAggregate[0].ManyCollection.List.Count);

                connection.Close();
            }
        }

        internal class InternalTable
        {
            public int Id { get; set; }
            public string Text { get; set; }
        }

        [TestMethod]
        public void Test_InternalTableTest()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                using (var tran = connection.BeginTransaction())
                {
                    var table = new InternalTable();
                    table.Id = 1;
                    table.Text = "xxx";

                    var query = connection.Query<InternalTable>(
                        $@"
                            select * from InternalTable where Id = 0
                        ")
                        .Map(m => m.ToRoot().Writable());

                    query.Transaction(tran);

                    query.Insert(table);

                    tran.Commit();
                }

                connection.Close();
            }

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var query = connection.Query<InternalTable>(
                    $@"
                        select * from InternalTable 
                    ");

                var tables = query.Fetch().ToArray();

                connection.Close();
            }
        }
    }
}
