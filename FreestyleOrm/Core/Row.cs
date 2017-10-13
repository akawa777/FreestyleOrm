using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Collections.Specialized;

namespace FreestyleOrm.Core
{
    internal class Row : IRow
    {
        public static Row CreateReadRow(IDataRecord dataRecord, MapOptions mapOptions)
        {
            return new Row(dataRecord, mapOptions, false, new string[0], null);
        }

        public static Row CreateWriteRow(IDataRecord dataRecord, MapOptions mapOptions, string[] primaryKeys, object entity)
        {
            return new Row(dataRecord, mapOptions, true, primaryKeys, entity);
        }        

        protected Row(IDataRecord dataRecord, MapOptions mapOptions, bool noSetValue, string[] primaryKeys, object entity)
        {
            SetMapOptions(mapOptions);            

            for (int i = 0; i < dataRecord.FieldCount; i++)
            {
                string name = dataRecord.GetName(i);                
                _columns.Add(name);

                if (noSetValue) continue;

                _valueMap[name] = dataRecord[i];
            }

            PrimaryKeys = primaryKeys;
            _entity = entity;
        }
        
        private Dictionary<string, object> _valueMap = new Dictionary<string, object>();
        private List<string> _columns = new List<string>();
        private MapOptions _mapOptions;
        private object _entity;

        public bool StartWithPrefix(string column) => string.IsNullOrEmpty(_mapOptions.IncludePrefix) ? false : column.StartsWith(_mapOptions.IncludePrefix);

        public bool IsRowVersionColumn(string column)
        {
            if (string.IsNullOrEmpty(RowVersionColumn)) return false;
            return RowVersionColumn == column;
        }

        public bool IsPrimaryKey(string column)
        {
            return PrimaryKeys.Any(x => x == column);
        }

        private string GetRealName(string column)
        {
            if (StartWithPrefix(column)) return column.Substring(_mapOptions.IncludePrefix.Length, column.Length - _mapOptions.IncludePrefix.Length);
            else return column;
        }

        public void SetMapOptions(MapOptions mapOptions)
        {
            _mapOptions = mapOptions;
        }

        public string ExpressionPath => _mapOptions.ExpressionPath;                

        public object this[string column] 
        {
            get { return _valueMap[column]; }
            set { _valueMap[column] = value; }
        }

        public string IncludePrefix => _mapOptions.IncludePrefix;

        public Func<string, string> FormatPropertyName => _mapOptions.FormatPropertyName;

        public void BindEntity(object entity)
        {
            _mapOptions.BindEntity(this, entity);            
        }

        public void BindRow(object entity)
        {
            _mapOptions.BindRow(entity, this);
        }

        public IEnumerable<string> Columns => _columns;        

        public IEnumerable<string> UniqueKeys
        {
            get
            {
                if (!string.IsNullOrEmpty(_mapOptions.UniqueKeys))
                {
                    foreach (var column in _mapOptions.UniqueKeys.Split(',').Select(x => x.Trim()))
                    {
                        yield return column;
                    }
                }
                else
                {
                    Enumerable.Empty<string>();
                }
            }
        }

        public IEnumerable<string> PrimaryKeys { get; set; }
        
        public string GetColumnWithoutPrefix(string column)
        {
            if (!StartWithPrefix(column))
            {
                return column;
            }

            return column.Substring(_mapOptions.IncludePrefix.Length, column.Length - _mapOptions.IncludePrefix.Length);
        }

        public bool AutoId => _mapOptions.AutoId;        

        public bool CanCreate(IRow prevRow, IEnumerable<string> prevUniqueKeys)
        {
            if (UniqueKeys.Count() == 0) return true;

            foreach (var column in prevUniqueKeys.Concat(UniqueKeys))
            {
                if (this[column] == DBNull.Value) return false;
            }

            if (prevRow == null) return true;
            if (UniqueKeys.Count() == 0) return true;

            foreach (var column in prevUniqueKeys.Concat(UniqueKeys))
            {
                if (prevRow[column] == DBNull.Value) continue;
                if (this[column].ToString() != prevRow[column].ToString()) return true;
            }

            return false;
        }
        
        public string Id
        {
            get
            {
                List<string> primaryValues = new List<string>();

                foreach (var primaryKey in PrimaryKeys)
                {
                    primaryValues.Add($"{primaryKey}: {this[primaryKey].ToString()}");
                }

                return string.Join(",", primaryValues);
            }
        }

        public string Table => _mapOptions.Table;

        public string RelationIdColumn => _mapOptions.RelationIdColumn;

        public string RelationEntityPath => _mapOptions.RelationEntityPath;

        public string RowVersionColumn => _mapOptions.RowVersionColumn;        

        public object NewRowVersion => _mapOptions.NewRowVersion(_entity);

        public bool IsRootRow => string.IsNullOrEmpty(_mapOptions.ExpressionPath);
    }
}