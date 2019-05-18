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
    public class UnitTestBase
    {
        [TestInitialize]
        public void Init()
        {
            _databaseKind = TestInitializer.DatabaseKinds.SqlServer;
            _testInitializer = new TestInitializer(_databaseKind, _purchaseOrderNo);
            InsertPuchaseOrders();
        }

        public void InsertPuchaseOrders()
        {
            var orders = CreatePurchaseOrders(_purchaseOrderNo, 10, 10, 10);

            using (var connection = _testInitializer.CreateConnection())
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    var query = CreatePurchaseOrderQuery(connection, transaction, "where PurchaseOrder.PurchaseOrderId = @PurchaseOrderId");

                    query
                        .Params(p => p["@PurchaseOrderId"] = 0);

                    foreach (var order in orders)
                    {
                        query.Insert(order);
                    }

                    transaction.Commit();
                }

                connection.Close();
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

        [TestCleanup]
        public void Cleanup()
        {
            _testInitializer.Dispose();
        }

        protected TestInitializer.DatabaseKinds _databaseKind;
        protected int _purchaseOrderNo = 10;
        protected TestInitializer _testInitializer;

        protected virtual IQuery<PurchaseOrder> CreatePurchaseOrderQuery(IDbConnection connection, IDbTransaction transaction = null, string where = "")
        {
            IQuery<PurchaseOrder> query = connection.Query<PurchaseOrder>(
                $@"
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
                    {where}
                    order by
                        PurchaseOrder.PurchaseOrderId,
                        PurchaseItem.PurchaseItemNo
                ")
                .Map(m =>
                {
                    m.ToRoot()
                        .UniqueKeys("PurchaseOrderId")
                        .FormatPropertyName(x => x)
                        .Writable()
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
                        .AutoId()
                        .OptimisticLock(o => o.Columns("RecordVersion").CurrentValues(x => new object[] { x.RecordVersion }).NewValues(x => new object[] { x.RecordVersion + 1 }));

                    m.ToOne(x => x.Customer)
                        .UniqueKeys("CustomerId")
                        .IncludePrefix("Customer_");

                    m.ToMany(x => x.PurchaseItems)
                        .UniqueKeys("PurchaseOrderId, PurchaseItemNo")
                        .FormatPropertyName(x => x)
                        .IncludePrefix("PurchaseItems_")
                        .Writable()
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
                        .OptimisticLock(o => o.Columns("RecordVersion").CurrentValues(x => new object[] { x.RecordVersion }).NewValues(x => new object[] { x.RecordVersion + 1 }));

                    m.ToOne(x => x.PurchaseItems.First().Product)
                        .UniqueKeys("PurchaseOrderId, PurchaseItemNo, ProductId")
                        .IncludePrefix("Product_");
                })
                .Transaction(transaction);

            return query;
        }
    }
}
