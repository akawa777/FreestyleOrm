using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace FreestyleOrm.Tests
{
    public class TestInitializer
    {
        public TestInitializer(IDbConnection connection, IDbTransaction transaction, IQuery<PurchaseOrder> purchaseOrderQuery)
        {
            _connection = connection;
            _transaction = transaction;
            _purchaseOrderQuery = purchaseOrderQuery;
        }

        private IDbConnection _connection;
        private IDbTransaction _transaction;
        private IQuery<PurchaseOrder> _purchaseOrderQuery;

        public void Init(int purchaseOrderNo)
        {
            CreateTable();
            InsertCustomers(CreateCustomers(10));
            InsertProducts(CreateProducts(10));
            InsertPurchaseOrders(CreatePurchaseOrders(purchaseOrderNo, 10, 10, 10));
        }

        public void CreateTable()
        {
            var command = _connection.CreateCommand();
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
            var query = _purchaseOrderQuery.Formats(f => f["where"] = string.Empty);

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
    }
}
