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
            using (_connection = CreateConnection())
            {
                _connection.Open();
                _transaction = _connection.BeginTransaction();

                var testInitializer = new TestInitializer(_connection, _transaction, CreatePurchaseOrderQuery());
                testInitializer.Init(_purchaseOrderNo);

                _transaction.Commit();
                _transaction = null;
                _connection.Close();
            }
        }

        protected IDbConnection _connection;
        protected IDbTransaction _transaction;
        protected int _purchaseOrderNo = 10;
        
        protected IDbConnection CreateConnection()
        {
            return new SqlConnection(@"Data Source=akawawin8\sqlserver2016;Initial Catalog=cplan_demo;Integrated Security=True");
        }

        protected virtual IQuery<PurchaseOrder> CreatePurchaseOrderQuery()
        {
            IQuery<PurchaseOrder> query = _connection.Query<PurchaseOrder>(
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
                        .OptimisticLock("RecordVersion", x => x.RecordVersion + 1);

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
                        .OptimisticLock("RecordVersion", x => x.RecordVersion + 1);

                    m.ToOne(x => x.PurchaseItems.First().Product)
                        .UniqueKeys("ProductId");
                })
                .Transaction(_transaction);

            return query;
        }

        [TestMethod]
        public virtual void Test_Fetch()
        {
            using (_connection = CreateConnection())
            {
                _connection.Open();                

                var customers = _connection.Query<Customer>("select * from Customer").Fetch().ToList();

                Assert.AreEqual(10, customers.Count);
                
                _connection.Close();
            }
        }

        [TestMethod]
        public void Test_Edit()
        {
            using (_connection = CreateConnection())
            {
                _connection.Open();
                _transaction = _connection.BeginTransaction();

                var query = _connection
                    .Query<Customer>("select * from Customer where CustomerId = @CustomerId")                    
                    .Transaction(_transaction);

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

                _transaction.Commit();
                _connection.Close();
            }
        }

        [TestMethod]
        public void Test_FetchList()
        {
            using (_connection = CreateConnection())
            {
                _connection.Open();

                var query = CreatePurchaseOrderQuery();

                query
                    .Formats(f => f["where"] = string.Empty);

                var orders = query.Fetch().ToList();

                Assert.AreEqual(_purchaseOrderNo, orders.Count);

                _connection.Close();
            }   
        }        

        [TestMethod]
        public void Test_Page()
        {
            using (_connection = CreateConnection())
            {
                _connection.Open();

                var query = CreatePurchaseOrderQuery();

                query
                    .Formats(f => f["where"] = string.Empty);

                int size = 5;
                int no = _purchaseOrderNo / size;

                var page = query.Page(no, size);

                Assert.AreEqual(size, page.Lines.Count());
                Assert.AreEqual(size * no, page.Lines.Last().PurchaseOrderId);

                _connection.Close();
            }            
        }

        [TestMethod]
        public void Test_Skip()
        {
            using (_connection = CreateConnection())
            {
                _connection.Open();

                var query = CreatePurchaseOrderQuery();

                query
                    .Formats(f => f["where"] = string.Empty);

                var orders = query.Fetch();

                var ordersByTake = orders.Take(_purchaseOrderNo / 2);
                var orderListByTake = ordersByTake.ToList();

                var ordersBySkip = orders.Skip(_purchaseOrderNo / 2);
                var ordersBySkipList = ordersBySkip.ToList();

                var ordersList = query.Fetch().Skip(_purchaseOrderNo / 2).ToList();

                for (int i = 0; i < ordersList.Count; i++) AssertEqualPurchaseOrder(ordersList[i], ordersBySkipList[i], false);

                _connection.Close();
            }            
        }

        [TestMethod]
        public void Test_Update()
        {
            using (_connection = CreateConnection())
            {
                _connection.Open();
                _transaction = _connection.BeginTransaction();

                var query = CreatePurchaseOrderQuery();

                query
                    .Formats(f => f["where"] = "where PurchaseOrder.PurchaseOrderId = @PurchaseOrderId")
                    .Params(p => p["@PurchaseOrderId"] = 1);

                var order = query.Fetch().Single();

                EditPurchaseOrder(order);

                query.Update(order);

                var updatedOrder = query.Fetch().Single();

                AssertEqualPurchaseOrder(order, updatedOrder, true);

                _transaction.Commit();
                _connection.Close();
            }            
        }

        [TestMethod]
        public void Test_Delete()
        {
            using (_connection = CreateConnection())
            {
                _connection.Open();
                _transaction = _connection.BeginTransaction();

                var query = CreatePurchaseOrderQuery();

                query
                    .Formats(f => f["where"] = "where PurchaseOrder.PurchaseOrderId = @PurchaseOrderId")
                    .Params(p => p["@PurchaseOrderId"] = 1);

                var order = query.Fetch().Single();

                query.Delete(order);

                var count = query.Fetch().Count();

                Assert.AreEqual(0, count);

                _transaction.Commit();
                _connection.Close();
            }            
        }

        protected void EditPurchaseOrder(PurchaseOrder order)
        {
            using (_connection = CreateConnection())
            {
                _connection.Open();

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

                _connection.Close();
            }            
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
