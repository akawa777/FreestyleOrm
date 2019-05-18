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
    public class UnitTestByManual : UnitTestBase
    {
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

                using (var transaction = connection.BeginTransaction())
                {
                    var query = connection
                        .Query<Customer>("select * from Customer where CustomerId = @CustomerId")
                        .Map(m => m.ToRoot().Writable())
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
                }
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

                int size = 5;
                int no = _purchaseOrderNo / size;

                var page = query.Page(no, size);

                Assert.AreEqual(no, page.PageNo);
                Assert.AreEqual(size, page.Items.Count());
                Assert.AreEqual(size * no, page.Items.Last().PurchaseOrderId);
                Assert.AreEqual(query.Fetch().Count(), page.Total);

                query = CreatePurchaseOrderQuery(connection);

                size = int.MaxValue;

                no = 1;

                page = query.Page(no, size);

                Assert.AreEqual(no, page.PageNo);

                query = CreatePurchaseOrderQuery(connection);

                size = 100000;

                no = 1;

                page = query.Page(no, size);

                Assert.AreEqual(no, page.PageNo);

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

                IQuery<PurchaseOrder> query;
                PurchaseOrder order = null;

                using (var transaction = connection.BeginTransaction())
                {
                    query = CreatePurchaseOrderQuery(connection, transaction, "where PurchaseOrder.PurchaseOrderId = @PurchaseOrderId");

                    query
                        .Params(p => p["@PurchaseOrderId"] = 1);

                    order = query.Fetch().Single();

                    EditPurchaseOrder(order);

                    query.Update(order);

                    transaction.Commit();
                }

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

                using (var transaction = connection.BeginTransaction())
                {
                    var query = CreatePurchaseOrderQuery(connection, transaction, "where PurchaseOrder.PurchaseOrderId = @PurchaseOrderId");

                    query
                        .Params(p => p["@PurchaseOrderId"] = 1);

                    var order = query.Fetch().Single();

                    query.Delete(order);

                    var count = query.Fetch().Count();

                    Assert.AreEqual(0, count);

                    transaction.Commit();
                }
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
