using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FreestyleOrm;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.Sqlite;
using System.Data.SqlClient;
using System.Linq;


namespace FreestyleOrm.Tests
{
    [TestClass]
    public class UnitTest
    {
        protected virtual bool ShouldManualQuery => true;
        protected virtual int PurchaseOrderNo => 100;        
        private IDbConnection _connection;
        private IDbTransaction _transaction;

        private string _sql = @"
			select
                PurchaseOrder.*,

                CustomerName,
                Customer.RecordVersion Customer_RecordVersion,

                PurchaseItemNo,
                Number,
                PurchaseItem.RecordVersion PurchaseItems_RecordVersion,

                PurchaseItem.ProductId,
                ProductName,
                Product.RecordVersion Product_RecordVersion
            from
                PurchaseOrder            
            left join
                Customer
            on
                PurchaseOrder.CustomerId = Customer.CustomerId
            left join
                PurchaseItem
            on
                PurchaseOrder.PurchaseOrderId = PurchaseItem.PurchaseOrderId
            left join
                Product
            on
                PurchaseItem.ProductId = Product.ProductId
            {{where}}
            order by
                PurchaseOrder.PurchaseOrderId,
                PurchaseItem.PurchaseItemNo
        ";

        [TestInitialize]
        public void Init()
        {
            _connection = CreateConnection();
            _connection.Open();
            _transaction = _connection.BeginTransaction();

            CreateTable(_connection);
            InsertCustomers(CreateCustomers(10));
            InsertProducts(CreateProducts(10));
            InsertPurchaseOrders(CreatePurchaseOrders(PurchaseOrderNo, 10, 10, 10));
        }

        [TestCleanup]
        public void Cleanup()
        {
            _transaction.Commit();
            _transaction.Dispose();
            _connection.Close();
            _connection.Dispose();
        }


        [TestMethod]
        public void Test_Init()
        {

        }

        [TestMethod]
        public void Test_FetchList()
        {
            var query = CreatePurchaseOrderQuery();

            query
                .Formats(f => f["where"] = string.Empty);                

            var orders = query.Fetch().ToList();
        }

        [TestMethod]
        public void Test_FlatFetch()
        {
            var customers = _connection.Query<Customer>("select * from Customer").Transaction(_transaction).Fetch().ToList();

            Assert.AreEqual(10, customers.Count);        }

        [TestMethod]
        public void Test_Fetch()
        {
            var query = CreatePurchaseOrderQueryByManual();

            query
                .Formats(f => f["where"] = "where PurchaseOrder.PurchaseOrderId not in (@PurchaseOrderIds)")
                .Parametes(p => p["@PurchaseOrderIds"] = new object[] { PurchaseOrderNo + 1, PurchaseOrderNo + 2 });

            var ordersByManual = query.Fetch().ToList();

            query = CreatePurchaseOrderQueryByAuto();

            query
                .Formats(f => f["where"] = "where PurchaseOrder.PurchaseOrderId not in (select OrderId from #OrderIds)")
                .TempTables(t =>
                {
                    t["#OrderIds"] = new TempTable
                    {
                        Columns = "OrderId int, KeyWord nvarchar(100)",
                        IndexSet = new string[] { "OrderId", "OrderId, KeyWord" },
                        Values = new object[] 
                        {
                            new { OrderId = PurchaseOrderNo + 1, KeyWord = "xxx" },
                            new { OrderId = PurchaseOrderNo + 2, KeyWord = "yyy" }
                        }
                    };
                });

            var ordersByAuto = query.Fetch().ToList();

            Assert.AreEqual(PurchaseOrderNo, ordersByManual.Count);
            for (int i = 0; i < ordersByManual.Count; i++) AssertEqualPurchaseOrder(ordersByManual[i], ordersByAuto[i], false);
        }

        [TestMethod]
        public void Test_Page()
        {
            var query = CreatePurchaseOrderQuery();

            query
                .Formats(f => f["where"] = string.Empty);

            int size = 5;
            int page = PurchaseOrderNo / size;            

            var orders = query.Fetch(page, size).ToList();

            Assert.AreEqual(size, orders.Count);
            Assert.AreEqual(size * page, orders.Last().PurchaseOrderId);
        }

        [TestMethod]
        public void Test_Skip()
        {
            var query = CreatePurchaseOrderQuery();

            query
                .Formats(f => f["where"] = string.Empty);

            var orders = query.Fetch();

            var ordersByTake = orders.Take(PurchaseOrderNo / 2);
            var orderListByTake = ordersByTake.ToList();

            var ordersBySkip = orders.Skip(PurchaseOrderNo / 2);
            var ordersBySkipList = ordersBySkip.ToList();

            var ordersList = query.Fetch().Skip(PurchaseOrderNo / 2).ToList();
            
            for (int i = 0; i < ordersList.Count; i++) AssertEqualPurchaseOrder(ordersList[i], ordersBySkipList[i], false);
        }

        [TestMethod]
        public void Test_Update()
        {
            var query = CreatePurchaseOrderQuery();

            query
                .Formats(f => f["where"] = "where PurchaseOrder.PurchaseOrderId = @PurchaseOrderId")
                .Parametes(p => p["@PurchaseOrderId"] = 1);

            var order = query.Fetch().Single();

            EditPurchaseOrder(order);

            query.Update(order);

            var updatedOrder = query.Fetch().Single();

            AssertEqualPurchaseOrder(order, updatedOrder, true);
        }

        [TestMethod]
        public void Test_Delete()
        {
            var query = CreatePurchaseOrderQuery();

            query
                .Formats(f => f["where"] = "where PurchaseOrder.PurchaseOrderId = @PurchaseOrderId")
                .Parametes(p => p["@PurchaseOrderId"] = 1);

            var order = query.Fetch().Single();

            query.Delete(order);

            var count = query.Fetch().Count();

            Assert.AreEqual(0, count);
        }

        private void EditPurchaseOrder(PurchaseOrder order)
        {
            order.Title += "_update";
            order.Customer = Customer.Create(10);            

            foreach (var item in order.PurchaseItems)
            {
                item.Number *= 10;
                item.Product = Product.Create(10);                
            }

            var newItem = PurchaseItem.Create(order.GetNewItemNo());
            var firstItem = order.PurchaseItems.First();

            newItem.Number = firstItem.Number;
            newItem.Product = firstItem.Product;

            order.AddItem(newItem);
            order.RemoveItem(firstItem);
        }

        private void AssertEqualPurchaseOrder(PurchaseOrder srcOrder, PurchaseOrder destOrder, bool updated)
        {
            Assert.AreEqual(false, string.IsNullOrEmpty(destOrder.Title));
            Assert.AreEqual(srcOrder.Title, destOrder.Title);

            Assert.AreEqual(srcOrder.Customer.CustomerId, destOrder.Customer.CustomerId);
            Assert.AreEqual(false, string.IsNullOrEmpty(destOrder.Customer.CustomerName));

            if (updated) Assert.AreEqual(srcOrder.RecordVersion + 1, destOrder.RecordVersion);
            else Assert.AreEqual(srcOrder.RecordVersion, destOrder.RecordVersion);

            foreach (var srcItem in srcOrder.PurchaseItems)
            {
                var destItem = destOrder.PurchaseItems.Single(x => x.PurchaseItemNo == srcItem.PurchaseItemNo);

                Assert.AreEqual(srcItem.PurchaseItemNo, destItem.PurchaseItemNo);
                Assert.AreEqual(srcItem.Number, destItem.Number);
                Assert.AreEqual(srcItem.Product.ProductId, destItem.Product.ProductId);
                Assert.AreEqual(false, string.IsNullOrEmpty(destItem.Product.ProductName));

                if (updated) Assert.AreEqual(srcItem.RecordVersion == null ? 1 : srcItem.RecordVersion + 1, destItem.RecordVersion);
                else Assert.AreEqual(srcItem.RecordVersion, destItem.RecordVersion);
            }
        }

        private IDbConnection CreateConnection()
        {
            return new SqlConnection(@"Data Source=akawawin8\sqlserver2016;Initial Catalog=cplan_demo;Integrated Security=True");
        }

        public void CreateTable(IDbConnection connection)
        {
            var command = connection.CreateCommand();
            command.Transaction = _transaction;

            try
            {
                command.CommandText = GetDropTableSql();
                command.ExecuteNonQuery();
            }
            catch
            {

            }

            command.CommandText = GetCreateTableSql();
            command.ExecuteNonQuery();
        }

        private string GetDropTableSql()
        {
            return @"
                drop table Customer;
                drop table Product;
                drop table PurchaseOrder;                
                drop table PurchaseItem;
            ";
        }

        private string GetCreateTableSql()
        {
            return @"
				CREATE TABLE [dbo].[Customer](
	                [CustomerId] [int] NOT NULL,
	                [CustomerName] [nvarchar](100) NULL,
	                [RecordVersion] [int] NULL,
                PRIMARY KEY CLUSTERED 
                (
	                [CustomerId] ASC
                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ) ON [PRIMARY]

                CREATE TABLE [dbo].[Product](
	                [ProductId] [int] NOT NULL,
	                [ProductName] [nvarchar](100) NULL,
	                [RecordVersion] [int] NULL,
                PRIMARY KEY CLUSTERED 
                (
	                [ProductId] ASC
                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ) ON [PRIMARY]

                CREATE TABLE [dbo].[PurchaseOrder](
	                [PurchaseOrderId] [int] IDENTITY(1,1) NOT NULL,
	                [Title] [nvarchar](100) NULL,
	                [CustomerId] [int] NULL,
	                [RecordVersion] [int] NULL,
                PRIMARY KEY CLUSTERED 
                (
	                [PurchaseOrderId] ASC
                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ) ON [PRIMARY]

                CREATE TABLE [dbo].[PurchaseItem](
	                [PurchaseOrderId] [int] NOT NULL,
	                [PurchaseItemNo] [int] NOT NULL,
	                [ProductId] [int] NULL,
	                [Number] [int] NULL,
	                [RecordVersion] [int] NULL,
                PRIMARY KEY CLUSTERED 
                (
	                [PurchaseOrderId] ASC,
	                [PurchaseItemNo] ASC
                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ) ON [PRIMARY]
            ";
        }

        private void InsertCustomers(IEnumerable<Customer> models)
        {
            var query = _connection
                .Query<Customer>("select * from Customer")
                .Map(m => m.To().UniqueKeys("CustomerId").Refer(Refer.Write).OptimisticLock("RecordVersion", x => x.RecordVersion == null ? 1 : x.RecordVersion + 1))
                .Transaction(_transaction);

            foreach (var model in models) query.Insert(model);
        }

        private void InsertProducts(IEnumerable<Product> models)
        {
            var query = _connection
                .Query<Product>("select * from Product")
                .Map(m => m.To().UniqueKeys("ProductId").Refer(Refer.Write).OptimisticLock("RecordVersion", x => x.RecordVersion == null ? 1 : x.RecordVersion + 1))
                .Transaction(_transaction);

            foreach (var model in models) query.Insert(model);
        }

        private void InsertPurchaseOrders(IEnumerable<PurchaseOrder> models)
        {
            var query = CreatePurchaseOrderQuery().Formats(f => f["where"] = string.Empty);                

            foreach (var model in models) query.Insert(model);
        }

        private IEnumerable<Customer> CreateCustomers(int number)
        {
            for (int i = 0; i < number; i++)
            {
                Customer customer = Customer.Create(i + 1);

                customer.CustomerName = $"{nameof(customer.CustomerName)}_{customer.CustomerId}";

                yield return customer;
            }
        }

        private IEnumerable<Product> CreateProducts(int number)
        {
            for (int i = 0; i < number; i++)
            {
                Product product = Product.Create(i + 1);

                product.ProductName = $"{nameof(product.ProductName)}_{product.ProductId}";

                yield return product;
            }
        }

        private IEnumerable<PurchaseOrder> CreatePurchaseOrders(int number, int maxItemNumber, int maxCutomerId, int maxProductId)
        {
            for (int i = 0; i < number; i++)
            {
                PurchaseOrder order = PurchaseOrder.Create(0);

                order.Title = $"{nameof(order.Title)}_{i + 1}";
                order.Customer = Customer.Create((i % maxCutomerId) + 1);

                for (int ii = 0; ii < maxItemNumber; ii++)
                {
                    var orderItem = PurchaseItem.Create(order.GetNewItemNo());
                    orderItem.Product = Product.Create((ii % maxProductId) + 1);
                    orderItem.Number = orderItem.PurchaseItemNo;

                    order.AddItem(orderItem);
                }

                yield return order;
            }
        }

        private IQuery<PurchaseOrder> CreatePurchaseOrderQuery()
        {
            if (ShouldManualQuery) return CreatePurchaseOrderQueryByManual();
            else return CreatePurchaseOrderQueryByAuto();
        }

        private IQuery<PurchaseOrder> CreatePurchaseOrderQueryByManual()
        {
            IQuery<PurchaseOrder> query = _connection.Query<PurchaseOrder>(_sql);

            query.Map(m =>
            {
                m.To()
                    .UniqueKeys("PurchaseOrderId")
                    .FormatPropertyName(x => x)
                    .Refer(Refer.Write)
                    .GetEntity((row, rootEntity) =>
                    {
                        PurchaseOrder entity = PurchaseOrder.Create((int)row[nameof(entity.PurchaseOrderId)]);
                        entity.Title = (string)row[nameof(entity.Title)];
                        entity.RecordVersion = (int)row[nameof(entity.RecordVersion)];
                        return entity;
                    })
                    .SetRow((entity, root, row) =>
                    {
                        row[nameof(entity.PurchaseOrderId)] = entity.PurchaseOrderId;
                        row[nameof(entity.Title)] = entity.Title;
                        row[nameof(entity.Customer.CustomerId)] = entity.Customer.CustomerId;
                        row[nameof(entity.RecordVersion)] = entity.RecordVersion;
                    })
                    .Table("PurchaseOrder")
                    .AutoId(true)
                    .OptimisticLock("RecordVersion", x => x.RecordVersion == null ? 1 : x.RecordVersion + 1);

                m.ToOne(x => x.Customer)
                    .UniqueKeys("CustomerId");

                m.ToMany(x => x.PurchaseItems)
                    .UniqueKeys("PurchaseOrderId, PurchaseItemNo")
                    .FormatPropertyName(x => x)
                    .IncludePrefix("PurchaseItems_")
                    .Refer(Refer.Write)
                    .GetEntity((row, rootEntity) =>
                    {
                        PurchaseItem entity = PurchaseItem.Create((int)row[nameof(entity.PurchaseItemNo)]);
                        entity.Number = (int)row[nameof(entity.Number)];
                        entity.RecordVersion = (int)row[$"PurchaseItems_{nameof(entity.RecordVersion)}"];
                        return entity;
                    })
                    .SetRow((entity, root, row) =>
                    {
                        row[nameof(root.PurchaseOrderId)] = root.PurchaseOrderId;
                        row[nameof(entity.PurchaseItemNo)] = entity.PurchaseItemNo;
                        row[nameof(entity.Product.ProductId)] = entity.Product.ProductId;
                        row[nameof(entity.Number)] = entity.Number;
                        row[nameof(entity.RecordVersion)] = entity.RecordVersion;
                    })
                    .Table("PurchaseItem")
                    .RelationId("PurchaseOrderId", x => x)
                    .OptimisticLock("RecordVersion", x => x.RecordVersion == null ? 1 : x.RecordVersion + 1);

                m.ToOne(x => x.PurchaseItems.First().Product)
                    .UniqueKeys("ProductId");
            })
            .Transaction(_transaction); 

            return query;
        }   
        
        public IQuery<PurchaseOrder> CreatePurchaseOrderQueryByAuto()
        {
            IQuery<PurchaseOrder> query = _connection.Query<PurchaseOrder>(_sql);

            query.Map(m =>
            {
                m.To()
                    .UniqueKeys("PurchaseOrderId")
                    .Refer(Refer.Write)
                    .SetRow((entity, root, row) =>
                    {
                        row.BindRow(entity);
                        row[nameof(entity.Customer.CustomerId)] = entity.Customer.CustomerId;
                    })
                    .AutoId(true)
                    .OptimisticLock("RecordVersion", x => x.RecordVersion == null ? 1 : x.RecordVersion + 1);

                m.ToOne(x => x.Customer)
                    .UniqueKeys("CustomerId");

                m.ToMany(x => x.PurchaseItems)
                    .UniqueKeys("PurchaseOrderId, PurchaseItemNo")
                    .Refer(Refer.Write)
                    .SetRow((entity, root, row) =>
                    {
                        row.BindRow(entity);
                        row[nameof(root.PurchaseOrderId)] = root.PurchaseOrderId;                        
                        row[nameof(entity.Product.ProductId)] = entity.Product.ProductId;
                    })
                    .RelationId("PurchaseOrderId", x => x)
                    .OptimisticLock("RecordVersion", x => x.RecordVersion == null ? 1 : x.RecordVersion + 1);

                m.ToOne(x => x.PurchaseItems.First().Product)
                    .UniqueKeys("ProductId");
            })
            .Transaction(_transaction);

            return query;
        }

        public class Customer
        {
            protected Customer()
            {

            }

            public static Customer Create()
            {
                return new Customer();
            }

            public static Customer Create(int customerId)
            {
                return new Customer() { CustomerId = customerId };
            }

            public int CustomerId { get; private set; }

            public string CustomerName { get; set; }

            public int? RecordVersion { get; set; }
        }

        public class Product
        {
            protected Product()
            {

            }

            public static Product Create()
            {
                return new Product();
            }

            public static Product Create(int productId)
            {
                return new Product() { ProductId = productId };
            }

            public int ProductId { get; private set; }

            public string ProductName { get; set; }

            public int? RecordVersion { get; set; }
        }

        public partial class PurchaseOrder
        {
            protected PurchaseOrder()
            {

            }

            public static PurchaseOrder Create(int purchaseOrderId)
            {
                PurchaseOrder order = new PurchaseOrder
                {
                    PurchaseOrderId = purchaseOrderId
                };

                return order;
            }

            public int PurchaseOrderId { get; set; }

            public Customer Customer { get; set; }

            public string Title { get; set; }

            private List<PurchaseItem> _purchaseItemList = new List<PurchaseItem>();

            public IEnumerable<PurchaseItem> PurchaseItems
            {
                get
                {
                    return _purchaseItemList;
                }
                private set
                {
                    _purchaseItemList = value.ToList();
                }
            }

            public int? RecordVersion { get; set; }

            public int GetNewItemNo()
            {
                if (_purchaseItemList.Count == 0) return 1;
                return _purchaseItemList.Max(x => x.PurchaseItemNo) + 1;
            }

            public void AddItem(PurchaseItem purchaseItem)
            {
                if (_purchaseItemList.Any(x => x.PurchaseItemNo == purchaseItem.PurchaseItemNo))
                {
                    throw new ArgumentException("PurchaseItemNo is invalid.");
                }

                _purchaseItemList.Add(purchaseItem);
            }

            public void RemoveItem(PurchaseItem purchaseItem)
            {
                if (!_purchaseItemList.Any(x => x == purchaseItem))
                {
                    throw new ArgumentException("purchaseItem is invalid.");
                }

                _purchaseItemList.Remove(purchaseItem);
            }
        }

        public class PurchaseItem
        {
            protected PurchaseItem()
            {

            }

            public static PurchaseItem Create(int no)
            {
                return new PurchaseItem { PurchaseItemNo = no };
            }

            public int PurchaseItemNo { get; private set; }

            public Product Product { get; set; }

            public int Number { get; set; }

            public int? RecordVersion { get; set; }
        }

    }
}
