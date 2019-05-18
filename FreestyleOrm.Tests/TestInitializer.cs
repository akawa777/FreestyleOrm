using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Data.Sqlite;

namespace FreestyleOrm.Tests
{
    public class TestInitializer: IDisposable
    {
        public TestInitializer(DatabaseKinds databaseKind, int purchaseOrderNo)
        {
            _databaseKind = databaseKind;            

            using (_connection = CreateConnection())
            {            
                _connection.Open();
                _transaction = _connection.BeginTransaction();                
                
                Init(purchaseOrderNo);

                _transaction.Commit();
                _connection.Close();
            }            
        }

        private IDbConnection _connection;
        private IDbTransaction _transaction;
        private IQuery<PurchaseOrder> _purchaseOrderQuery;

        public enum DatabaseKinds
        {
            SqlServer,
            Sqlite
        }

        private DatabaseKinds _databaseKind;

        private class SqliteDatabaseContext : DbContext
        {
            protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            {
                if (System.IO.File.Exists(_databaseFile))
                {
                    System.IO.File.Delete(_databaseFile);
                }

                optionsBuilder.UseSqlite($"Filename={_databaseFile}");
            }

            private string _databaseFile = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "database.sqlite");

            public string GetConnectionString()
            {
                SqliteConnectionStringBuilder builder = new SqliteConnectionStringBuilder
                {
                    DataSource = _databaseFile,
                    Mode = SqliteOpenMode.ReadWrite
                };

                return builder.ConnectionString;
            }
        }

        public void Dispose()
        {
            if (_sqliteDatabaseContext != null) _sqliteDatabaseContext.Dispose();
        }

        private SqliteDatabaseContext _sqliteDatabaseContext;

        public IDbConnection CreateConnection()
        {
            if (_databaseKind == DatabaseKinds.SqlServer)
            {
                //return new SqlConnection(@"Data Source=akirapcwin7\sqlexpress;Initial Catalog=test;Integrated Security=True");
                //return new SqlConnection(@"Data Source=akawawin8\sqlserver2016;Initial Catalog=cplan_demo;Integrated Security=True");
                return new SqlConnection(@"Data Source=workbench\sql2017;Initial Catalog=cplan_1000U;Integrated Security=True");
            }
            else
            {
                IDbConnection connection;

                if (_sqliteDatabaseContext == null)
                {
                    _sqliteDatabaseContext = new SqliteDatabaseContext();

                    try
                    {
                        _sqliteDatabaseContext.Database.EnsureCreated();
                    }
                    catch
                    {

                    }
                }

                connection = new SqliteConnection(_sqliteDatabaseContext.GetConnectionString());

                return connection;
            }            
        }

        private void Init(int purchaseOrderNo)
        {
            CreateTable();
            InsertCustomers(CreateCustomers(10));
            InsertProducts(CreateProducts(10));            
        }

        private void CreateTable()
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
                drop table D_TEST_TABLE;                
                drop table Node;
                drop table Root;
                drop table Many;
                drop table One;
                drop table Many2;
                drop table Many2One;                
                drop table Attr;                
                drop table InternalTable;
            ";
        }

        private string GetCreateTableSql()
        {
            string identity = _databaseKind == DatabaseKinds.Sqlite ? "integer" : "int identity";

            return $@"
				CREATE TABLE Customer(
	                CustomerId int NOT NULL,
	                CustomerName nvarchar(100) NULL,
	                RecordVersion int NULL,
                PRIMARY KEY
                (
	                CustomerId
                ));

                CREATE TABLE Product(
	                ProductId int NOT NULL,
	                ProductName nvarchar(100) NULL,
	                RecordVersion int NULL,
                PRIMARY KEY
                (
	                ProductId
                ));

                CREATE TABLE PurchaseOrder(
	                PurchaseOrderId {identity} NOT NULL,
	                Title nvarchar(100) NULL,
	                CustomerId int NULL,
	                RecordVersion int NULL,
                PRIMARY KEY
                (
	                PurchaseOrderId
                ));

                CREATE TABLE PurchaseItem(
	                PurchaseOrderId int NOT NULL,
	                PurchaseItemNo int NOT NULL,
	                ProductId int NULL,
	                Number int NULL,
	                RecordVersion int NULL,
                PRIMARY KEY
                (
	                PurchaseOrderId,
	                PurchaseItemNo
                ));

                CREATE TABLE D_TEST_TABLE(
                    ID int primary key,
	                COL_ONE nvarchar(100),
                    COL_TWO_ONE nvarchar(100)
                );

                create table Node (
	                Id int primary key,	
	                Name nvarchar(100),
	                ParentId int
                );

                insert into Node values(1000, 'node_1000', null);
                    insert into Node values(1100, 'node_1100', 1000);
                        insert into Node values(1110, 'node_1110', 1100);
                    insert into Node values(1200, 'node_1200', 1000);                        
                insert into Node values(2000, 'node_2000', null);
                    insert into Node values(2100, 'node_2100', 2000);  

                create table Root (
	                RootId int primary key,
                    Text nvarchar(100),
                    LastUpdate nvarchar(100)
                );

                create table Many(
                    RootId int,
                    ManyId int,
                    Text nvarchar(100),
                    AttrFlg int,
                    LastUpdate nvarchar(100),
                    primary key (
                        RootId,
                        ManyId
                    )
                );

                create table One (
	                RootId int primary key,
                    Text nvarchar(100),
                    LastUpdate nvarchar(100)
                );

                create table Many2(
                    RootId int,
                    Many2Id int,
                    Text nvarchar(100),
                    AttrFlg int,                    
                    LastUpdate nvarchar(100),
                    primary key (
                        RootId,
                        Many2Id
                    )
                );

                create table Many2One (
                    RootId int,
	                Many2Id int,                     
                    Text nvarchar(100),
                    LastUpdate nvarchar(100),
                    primary key (
                        RootId,
                        Many2Id
                    )
                );

                create table Attr (
                    AttrId int,
	                AttrFlg int,                                         
                    LastUpdate nvarchar(100),
                    primary key (
                        AttrId
                    )
                );

                insert into Attr values(1, 1, null);
                insert into Attr values(2, 1, null);
                insert into Attr values(3, 2, null);
                insert into Attr values(4, 2, null);
               
                create table InternalTable (
                    Id int primary key,
	                Text nvarchar(100)
                );
                    
            ";
        }

        private void InsertCustomers(IEnumerable<Customer> models)
        {
            var query = _connection
                .Query<Customer>("select * from Customer")
                .Map(m =>
                {
                    m.ToRoot()
                        .UniqueKeys("CustomerId")
                        .Writable()
                        .OptimisticLock(o => o.Columns("RecordVersion").CurrentValues(x => new object[] { x.RecordVersion }).NewValues(x => new object[] { x.RecordVersion + 1 }));
                })
                .Transaction(_transaction);

            foreach (var model in models) query.Insert(model);
        }

        private void InsertProducts(IEnumerable<Product> models)
        {
            var query = _connection
                .Query<Product>("select * from Product")
                .Map(m =>
                {
                    m.ToRoot()
                        .UniqueKeys("ProductId")
                        .Writable()
                        .OptimisticLock(o => o.Columns("RecordVersion").CurrentValues(x => new object[] { x.RecordVersion }).NewValues(x => new object[] { x.RecordVersion + 1 }));
                })
                .Transaction(_transaction);

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
    }
}
