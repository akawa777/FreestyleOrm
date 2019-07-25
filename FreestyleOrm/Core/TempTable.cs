using System;
using System.Collections.Generic;
using System.Text;

namespace FreestyleOrm.Core
{
    internal class TempTableSet : ITempTableSet
    {
        public List<TempTable> TempTableList { get; } = new List<TempTable>();

        public ITempTable Table(string table, string columns)
        {
            if (string.IsNullOrEmpty(table)) throw new ArgumentException($"{table} is null or empty");
            if (string.IsNullOrEmpty(columns)) throw new ArgumentException($"{columns} is null or empty");

            TempTable tempTable = new TempTable(table, columns);
            TempTableList.Add(tempTable);

            return tempTable;
        }
    }

    internal class TempTable : ITempTable
    {
        public TempTable(string table, string columns)
        {
            Table = table;
            Columns = columns;
        }

        public string Table { get; private set; }
        public string Columns { get; private set; }
        public List<string> IndexList { get; private set; } = new List<string>();
        public IEnumerable<object> ValueList { get; private set; }
        public ITempTable Indexes(params string[] indexes)
        {
            if (indexes == null) throw new ArgumentException($"{indexes} is null");

            IndexList.AddRange(indexes);
            return this;
        }
        public ITempTable Values(params object[] values)
        {
            if (values == null) throw new ArgumentException($"{values} is null");

            ValueList = values;
            return this;
        }

        public ITempTable Values(params IDictionary<string, object>[] values)
        {
            if (values == null) throw new ArgumentException($"{values} is null");

            ValueList = values;
            return this;
        }
    }
}
