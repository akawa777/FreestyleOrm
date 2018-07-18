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
    public class UnitTestByAuto : UnitTestByManual
    {
        protected override IQuery<PurchaseOrder> CreatePurchaseOrderQuery(IDbConnection connection, IDbTransaction  transaction = null, string where = "")
        {
            IQuery<PurchaseOrder> query = base.CreatePurchaseOrderQuery(connection, transaction, where);

            query.Map(m =>
            {
                m.ToRoot()
                    .UniqueKeys("PurchaseOrderId")    
                    .Writable()                
                    .SetRow((entity, root, row) =>
                    {
                        row.BindRow(entity);
                        row[nameof(entity.Customer.CustomerId)] = entity.Customer.CustomerId;
                    })
                    .AutoId()
                    .OptimisticLock(o => o.Columns("RecordVersion").CurrentValues(x => new object[] { x.RecordVersion }).NewValues(x => new object[] { x.RecordVersion + 1 }));

                m.ToOne(x => x.Customer)
                    .UniqueKeys("CustomerId");                    

                m.ToMany(x => x.PurchaseItems)
                    .UniqueKeys("PurchaseOrderId, PurchaseItemNo")                    
                    .Writable()
                    .SetRow((entity, root, row) =>
                    {
                        row.BindRow(entity);
                        row[nameof(root.PurchaseOrderId)] = root.PurchaseOrderId;
                        row[nameof(entity.Product.ProductId)] = entity.Product.ProductId;
                    })
                    .RelationId("PurchaseOrderId", x => x)
                    .OptimisticLock(o => o.Columns("RecordVersion").CurrentValues(x => new object[] { x.RecordVersion }).NewValues(x => new object[] { x.RecordVersion + 1 }));

                m.ToOne(x => x.PurchaseItems.First().Product)
                    .UniqueKeys("ProductId");
            })
            .Transaction(transaction);

            return query;
        }

        [TestMethod]
        public override void Test_Fetch()
        {
            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                var query = base.CreatePurchaseOrderQuery(connection, null, "where PurchaseOrder.PurchaseOrderId not in (@PurchaseOrderIds)");

                query                    
                    .Params(p => p["@PurchaseOrderIds"] = new object[] { _purchaseOrderNo + 1, _purchaseOrderNo + 2 });

                var ordersByManual = query.Fetch().ToList();

                query = this.CreatePurchaseOrderQuery(connection);

                string tempTable = _databaseKind == TestInitializer.DatabaseKinds.Sqlite ? "OrderIds" : "#OrderIds";

                query = base.CreatePurchaseOrderQuery(connection, null, $"where PurchaseOrder.PurchaseOrderId not in (select OrderId from {tempTable})");

                query                    
                    .TempTables(t =>
                    {
                        t.Table($"{tempTable}", "OrderId int, KeyWord nvarchar(100)")
                            .Indexes("OrderId", "OrderId, KeyWord")
                            .Values(
                                new { OrderId = _purchaseOrderNo + 1, KeyWord = "xxx" },
                                new { OrderId = _purchaseOrderNo + 2, KeyWord = "yyy" }
                            );
                    });

                var ordersByAuto = query.Fetch().ToList();

                Assert.AreEqual(_purchaseOrderNo, ordersByManual.Count);
                for (int i = 0; i < ordersByManual.Count; i++) AssertEqualPurchaseOrder(ordersByManual[i], ordersByAuto[i], false);

                connection.Close();
            }
        }
    }
}
