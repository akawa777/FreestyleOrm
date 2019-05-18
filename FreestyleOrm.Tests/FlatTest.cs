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
using System.Collections.Specialized;

namespace FreestyleOrm.Tests
{

    [TestClass]
    public class FlatTest : UnitTestBase
    {
        private string _purchaseOrdersSql = @"
            select
                PurchaseOrder.*,
                PurchaseItemNo,
                Number,
                PurchaseItem.RecordVersion PurchaseItems_RecordVersion
            from
                PurchaseOrder                        
            left join
                PurchaseItem
            on
                PurchaseOrder.PurchaseOrderId = PurchaseItem.PurchaseOrderId
            order by                    
                PurchaseOrder.PurchaseOrderId,
                PurchaseItem.PurchaseItemNo
        ";

        private string _customerSql = @"
            select
                *
            from
                Customer  
            where
                CustomerId = @customerId
        ";

        private string _purchaseOrderSql = @"
            select
                *
            from
                PurchaseOrder  
            where
                PurchaseOrderId = @purchaseOrderId
        ";

        [TestMethod]
        public void Fetch()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var query = connection.Query(_purchaseOrdersSql);

                var list = query.Fetch().ToList();

                var command = connection.CreateCommand();
                command.CommandText = _purchaseOrdersSql;

                using (var reader = command.ExecuteReader())
                {
                    int rowIndex = 0;
                    while (reader.Read())
                    {
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            var column = reader.GetName(i);
                            var value = reader.GetValue(i);

                            var item = list[rowIndex];

                            Assert.AreEqual(item[column], value);
                        }

                        rowIndex++;
                    }

                    Assert.AreEqual(list.Count, rowIndex);
                }

                connection.Close();
            }
        }

        [TestMethod]
        public void Save()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    var idCount = 10;

                    for (int i = 0; i < idCount; i++)
                    {
                        var query = connection
                            .Query(_customerSql)
                            .Map(m =>
                            {
                                m.ToRoot()
                                    .Writable()
                                    .Table("Customer");
                            })
                            .Params(p => p["@customerId"] = 0)
                            .Transaction(transaction);

                        var customer = new OrderedDictionary();

                        customer["CustomerId"] = 100 + i;
                        customer["CustomerName"] = $"user_{customer["CustomerId"]}";
                        customer["RecordVersion"] = 1;

                        query.Insert(customer);

                        Action validation = () =>
                        {
                            var storedCustomer = query
                                .Params(p => p["@customerId"] = customer["CustomerId"])
                                .Fetch()
                                .Single();

                            Assert.AreEqual(customer["CustomerId"].ToString(), storedCustomer["CustomerId"].ToString());
                            Assert.AreEqual(customer["CustomerName"], storedCustomer["CustomerName"]);
                            Assert.AreEqual(customer["RecordVersion"].ToString(), storedCustomer["RecordVersion"].ToString());
                        };

                        validation();

                        customer["CustomerName"] = $"user_{customer["CustomerId"]}_update";
                        customer["RecordVersion"] = 2;

                        query.Update(customer);

                        validation();

                        query.Delete(customer);
                        var count = query.Fetch().Count();

                        Assert.AreEqual(0, count);
                    }

                    transaction.Commit();
                }

                connection.Close();
            }
        }

        [TestMethod]
        public void AutoId()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    var query = connection
                            .Query(_purchaseOrderSql)
                            .Map(m =>
                            {
                                m.ToRoot()
                                    .Writable()
                                    .Table("PurchaseOrder")
                                    .AutoId();
                            })
                            .Params(p => p["@purchaseOrderId"] = 0)
                            .Transaction(transaction);

                    var purchaseOrder = new OrderedDictionary();

                    purchaseOrder["Title"] = $"new_purchase_order";
                    purchaseOrder["CustomerId"] = 1;
                    purchaseOrder["RecordVersion"] = 1;

                    query.Insert(purchaseOrder, out int purchaseOrderId);

                    transaction.Commit();

                    var storedCustomer = query
                            .Params(p => p["@purchaseOrderId"] = purchaseOrderId)
                            .Fetch()
                            .Single();

                    Assert.AreEqual(purchaseOrder["Title"], storedCustomer["Title"]);
                    Assert.AreEqual(purchaseOrder["CustomerId"].ToString(), storedCustomer["CustomerId"].ToString());
                    Assert.AreEqual(purchaseOrder["RecordVersion"].ToString(), storedCustomer["RecordVersion"].ToString());                    
                }

                connection.Close();
            }
        }

        [TestMethod]
        public void OptimisticLock()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();


                var query = connection
                        .Query(_purchaseOrderSql)
                        .Map(m =>
                        {
                            m.ToRoot()
                                .Writable()
                                .Table("PurchaseOrder")
                                .AutoId()
                                .OptimisticLock(o =>
                                {
                                    o.Columns("RecordVersion")
                                        .CurrentValues(x => new object[] { x["RecordVersion"].ToString() })
                                        .NewValues(x =>
                                        {
                                            if (x.Contains("RecordVersion"))
                                            {
                                                return new object[] { int.Parse(x["RecordVersion"].ToString()) + 1 };
                                            }
                                            else
                                            {
                                                return new object[] { 1 };
                                            }
                                        });
                                });
                        })
                        .Params(p => p["@purchaseOrderId"] = 0);

                var purchaseOrder = new OrderedDictionary();

                int purchaseOrderId = 0;
                purchaseOrder["Title"] = $"new_purchase_order";
                purchaseOrder["CustomerId"] = 1;                

                using (var transaction = connection.BeginTransaction())
                {
                    query.Transaction(transaction);                    

                    query.Insert(purchaseOrder, out purchaseOrderId);

                    transaction.Commit();                    
                }

                using (var transaction = connection.BeginTransaction())
                {
                    query
                        .Transaction(transaction)
                        .Params(p => p["@purchaseOrderId"] = purchaseOrderId);

                    purchaseOrder = query.Fetch().Single();
                    
                    purchaseOrder["Title"] = $"update_purchase_order";

                    query.Update(purchaseOrder);

                    var storedPurchaseOrder = query.Fetch().Single();

                    Assert.AreEqual(storedPurchaseOrder["Title"], purchaseOrder["Title"]);
                    Assert.AreEqual("2", storedPurchaseOrder["RecordVersion"].ToString());

                    transaction.Commit();
                }

                using (var transaction = connection.BeginTransaction())
                {
                    query
                        .Transaction(transaction)
                        .Params(p => p["@purchaseOrderId"] = purchaseOrderId);                    

                    purchaseOrder = query.Fetch().Single();

                    purchaseOrder["RecordVersion"] = -1;

                    DBConcurrencyException exception = null;

                    try
                    {
                        query.Update(purchaseOrder);
                    }
                    catch(DBConcurrencyException e)
                    {
                        exception = e;
                    }

                    Assert.AreEqual(true, exception != null);

                    transaction.Commit();
                }

                connection.Close();
            }
        }
    }
}
