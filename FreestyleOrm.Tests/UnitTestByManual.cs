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
    public class UnitTestByManual
    {
        [TestInitialize]
        public void Init()
        {
            _databaseKind = TestInitializer.DatabaseKinds.SqlServer;
            _testInitializer = new TestInitializer(_databaseKind, CreatePurchaseOrderQuery, _purchaseOrderNo);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _testInitializer.Dispose();
        }

        protected TestInitializer.DatabaseKinds _databaseKind;
        protected int _purchaseOrderNo = 100;
        protected TestInitializer _testInitializer;

        protected virtual IQuery<PurchaseOrder> CreatePurchaseOrderQuery(IDbConnection connection, IDbTransaction transaction = null)
        {
            IQuery<PurchaseOrder> query = connection.Query<PurchaseOrder>(
                @"
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
                ")
                .Map(m =>
                {
                    m.To()
                        .UniqueKeys("PurchaseOrderId")
                        .FormatPropertyName(x => x)
                        .Refer(Refer.Write)      
                        .CreateEntity((row, rootEntity) => PurchaseOrder.Create(row.Get<int>("PurchaseOrderId")))                  
                        .SetEntity((row, rootEntity, entity) =>
                        {                            
                            entity.Title = row.Get<string>(nameof(entity.Title));
                            entity.RecordVersion = row.Get<int>(nameof(entity.RecordVersion));                            
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
                        .OptimisticLock("RecordVersion", x => x.RecordVersion + 1);

                    m.ToOne(x => x.Customer)
                        .UniqueKeys("CustomerId")
                        .IncludePrefix("Customer_");

                    m.ToMany(x => x.PurchaseItems)
                        .UniqueKeys("PurchaseOrderId, PurchaseItemNo")
                        .FormatPropertyName(x => x)
                        .IncludePrefix("PurchaseItems_")
                        .Refer(Refer.Write)
                        .CreateEntity((row, rootEntity) => PurchaseItem.Create(row.Get<int>("PurchaseItemNo")))                  
                        .SetEntity((row, rootEntity, entity) =>
                        {                            
                            entity.Number = row.Get<int>(nameof(entity.Number));
                            entity.RecordVersion = row.Get<int>($"PurchaseItems_{nameof(entity.RecordVersion)}");
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
                        .OptimisticLock("RecordVersion", x => x.RecordVersion + 1);

                    m.ToOne(x => x.PurchaseItems.First().Product)
                        .UniqueKeys("ProductId")
                        .IncludePrefix("Product_");
                })
                .Transaction(transaction);

            return query;
        }

        [TestMethod]
        public virtual void Test_Fetch()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();                

                var customers = connection.Query<Customer>("select * from Customer").Fetch().ToList();

                Assert.AreEqual(10, customers.Count);
                
                connection.Close();
            }
        }

        [TestMethod]
        public void Test_Edit()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();
                var transaction = connection.BeginTransaction();

                var query = connection
                    .Query<Customer>("select * from Customer where CustomerId = @CustomerId")                    
                    .Transaction(transaction);

                var customer = Customer.Create(11);
                customer.CustomerName = $"{nameof(customer.CustomerName)}_{11}";

                query.Insert(customer);

                query = query.Params(p => p["@CustomerId"] = customer.CustomerId);

                var insertedCustomer = query.Fetch().Single();

                insertedCustomer.CustomerName += "_update";

                query.Update(insertedCustomer);

                var updatedCustomer = query.Fetch().Single();

                Assert.AreEqual(insertedCustomer.CustomerName, updatedCustomer.CustomerName);

                query.Delete(updatedCustomer);

                updatedCustomer = query.Fetch().FirstOrDefault();

                Assert.AreEqual(null, updatedCustomer);

                transaction.Commit();
                connection.Close();
            }
        }

        [TestMethod]
        public void Test_FetchList()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var query = CreatePurchaseOrderQuery(connection);

                query
                    .Formats(f => f["where"] = string.Empty);

                var orders = query.Fetch().ToList();

                Assert.AreEqual(_purchaseOrderNo, orders.Count);

                connection.Close();
            }   
        }        

        [TestMethod]
        public void Test_Page()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var query = CreatePurchaseOrderQuery(connection);
                query
                    .Formats(f => f["where"] = string.Empty);

                int size = 5;
                int no = _purchaseOrderNo / size;

                var page = query.Page(no, size);

                Assert.AreEqual(no, page.PageNo);
                Assert.AreEqual(size, page.Lines.Count());
                Assert.AreEqual(size * no, page.Lines.Last().PurchaseOrderId);
                Assert.AreEqual(no, page.TotalLinesCount);

                connection.Close();
            }            
        }

        [TestMethod]
        public void Test_Skip()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();                

                var query = CreatePurchaseOrderQuery(connection);

                query
                    .Formats(f => f["where"] = string.Empty);

                var orders = query.Fetch();

                var ordersByTake = orders.Take(_purchaseOrderNo / 2);
                var orderListByTake = ordersByTake.ToList();

                var ordersBySkip = orders.Skip(_purchaseOrderNo / 2);
                var ordersBySkipList = ordersBySkip.ToList();

                var ordersList = query.Fetch().Skip(_purchaseOrderNo / 2).ToList();

                for (int i = 0; i < ordersList.Count; i++) AssertEqualPurchaseOrder(ordersList[i], ordersBySkipList[i], false);

                connection.Close();
            }            
        }

        [TestMethod]
        public void Test_Update()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();
                var transaction = connection.BeginTransaction();

                var query = CreatePurchaseOrderQuery(connection, transaction);

                query
                    .Formats(f => f["where"] = "where PurchaseOrder.PurchaseOrderId = @PurchaseOrderId")
                    .Params(p => p["@PurchaseOrderId"] = 1);

                var order = query.Fetch().Single();

                EditPurchaseOrder(order);

                query.Update(order);

                transaction.Commit();

                query.Transaction(null);

                var updatedOrder = query.Fetch().Single();

                AssertEqualPurchaseOrder(order, updatedOrder, true);
                
                connection.Close();
            }            
        }

        [TestMethod]
        public void Test_Delete()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();
                var transaction = connection.BeginTransaction();

                var query = CreatePurchaseOrderQuery(connection, transaction);

                query
                    .Formats(f => f["where"] = "where PurchaseOrder.PurchaseOrderId = @PurchaseOrderId")
                    .Params(p => p["@PurchaseOrderId"] = 1);

                var order = query.Fetch().Single();

                query.Delete(order);

                var count = query.Fetch().Count();

                Assert.AreEqual(0, count);

                transaction.Commit();
                connection.Close();
            }            
        }

        protected void EditPurchaseOrder(PurchaseOrder order)
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

        protected void AssertEqualPurchaseOrder(PurchaseOrder srcOrder, PurchaseOrder destOrder, bool updated)
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

                if (updated) Assert.AreEqual(srcItem.RecordVersion + 1, destItem.RecordVersion);
                else Assert.AreEqual(srcItem.RecordVersion, destItem.RecordVersion);
            }
        }
        
    }
}
