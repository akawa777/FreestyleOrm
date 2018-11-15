using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace FreestyleOrm.Tests
{
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

        public int RecordVersion { get; set; }
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

        public int RecordVersion { get; set; }
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
        private List<PurchaseItem> _purchaseItemList2 = new List<PurchaseItem>();

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

        public int RecordVersion { get; set; }

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

        public int RecordVersion { get; set; }
    }
}
