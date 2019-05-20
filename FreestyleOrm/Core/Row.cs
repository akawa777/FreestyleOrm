using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Collections.Specialized;

namespace FreestyleOrm.Core
{
    internal class Row : IRow
    {
        public static Row CreateReadRow(IDataRecord dataRecord, MapRuleBasic mapRuleBasic)
        {
            return new Row(dataRecord, mapRuleBasic, false, new string[0], null);
        }

        public static Row CreateWriteRow(IDataRecord dataRecord, MapRuleBasic mapRuleBasic, string[] primaryKeys, object entity)
        {
            return new Row(dataRecord, mapRuleBasic, true, primaryKeys, entity);
        }

        protected Row(IDataRecord dataRecord, MapRuleBasic mapRuleBasic, bool noSetValue, string[] primaryKeys, object entity)
        {
            SetMapRuleBasic(mapRuleBasic);            

            for (int i = 0; i < dataRecord.FieldCount; i++)
            {
                string name = dataRecord.GetName(i);                
                _columns.Add(name);

                if (noSetValue) _valueMap[name] = null;
                else _valueMap[name] = dataRecord[i];
            }

            PrimaryKeys = primaryKeys;
            Entity = entity;
        }
        
        private Dictionary<string, object> _valueMap = new Dictionary<string, object>();
        private List<string> _columns = new List<string>();
        private MapRuleBasic _mapRuleBasic;
        public object Entity { get; set; }

        public bool StartWithPrefix(string column) => string.IsNullOrEmpty(_mapRuleBasic.IncludePrefix) ? false : column.StartsWith(_mapRuleBasic.IncludePrefix);

        public bool IsConcurrencyColumn(string column, out int index)
        {
            index = -1;

            string[] columns = OptimisticLock.GetColumns();

            if (columns.Length == 0) return false;

            for (int i = 0; i < columns.Length; i++)
            {
                string name = columns[i];

                if (name == column)
                {
                    index = i;
                    return true;
                }
            }

            return false;
        }

        public bool IsPrimaryKey(string column)
        {
            return PrimaryKeys.Any(x => x == column);
        }

        private string GetRealName(string column)
        {
            if (StartWithPrefix(column)) return column.Substring(_mapRuleBasic.IncludePrefix.Length, column.Length - _mapRuleBasic.IncludePrefix.Length);
            else return column;
        }

        public void SetMapRuleBasic(MapRuleBasic mapRuleBasic)
        {
            _mapRuleBasic = mapRuleBasic;
        }

        public string ExpressionPath => _mapRuleBasic.ExpressionPath;                

        public object this[string column] 
        {
            get 
            { 
                if (_valueMap.TryGetValue(column, out object value))
                {
                    return _valueMap[column]; 
                }
                else
                {
                    throw new ArgumentException($"{column} column is not exist.");
                }
            }
            set { _valueMap[column] = value; }
        }

        public string IncludePrefix => _mapRuleBasic.IncludePrefix;

        public Func<string, string> FormatPropertyName => _mapRuleBasic.FormatPropertyName;

        public void BindEntity(object entity)
        {
            _mapRuleBasic.BindEntity(this, entity);            
        }

        public void BindRow(object entity)
        {
            _mapRuleBasic.BindRow(entity, this);
        }

        public IEnumerable<string> Columns => _columns;        

        public IEnumerable<string> UniqueKeys
        {
            get
            {
                if (!string.IsNullOrEmpty(_mapRuleBasic.UniqueKeys))
                {
                    foreach (var column in _mapRuleBasic.UniqueKeys.Split(',').Select(x => x.Trim()))
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

            return column.Substring(_mapRuleBasic.IncludePrefix.Length, column.Length - _mapRuleBasic.IncludePrefix.Length);
        }

        public bool AutoId => _mapRuleBasic.AutoId;        

        public bool CanCreate(IRow prevRow, IEnumerable<string> prevUniqueKeys)
        {            
            if (UniqueKeys.Count() == 0 && IsRootRow)
            {
                return true;                
            }

            foreach (var column in UniqueKeys)
            {
                if (this[column] == DBNull.Value) return false;
            }

            if (prevRow == null) return true;
            if (UniqueKeys.Count() == 0) return true;

            foreach (var column in prevUniqueKeys.Merge(UniqueKeys))
            {
                if (this[column] == DBNull.Value && prevRow[column] == DBNull.Value) continue;
                if (this[column] == DBNull.Value && prevRow[column] != DBNull.Value) return true;
                if (this[column] != DBNull.Value && prevRow[column] == DBNull.Value) return true;
                if (this[column].ToString() != prevRow[column].ToString()) return true;
            }

            return false;
        }
        
        public string Id
        {
            get
            {
                List<string> primaryValues = new List<string>();

                primaryValues.Add($"ExpressionPath: {ExpressionPath}");

                foreach (var primaryKey in PrimaryKeys)
                {
                    primaryValues.Add($"{primaryKey}: {this[primaryKey].ToString()}");
                }

                return string.Join(",", primaryValues);
            }
        }

        public string Table => _mapRuleBasic.Table;

        public RelationId RelationId => _mapRuleBasic.RelationId;

        public  OptimisticLock OptimisticLock => _mapRuleBasic.OptimisticLock;

        public bool IsRootRow => _mapRuleBasic.IsRootOptions;

        public TValue Get<TValue>(string column)
        {
            // return (TValue)Convert.ChangeType(this[column], typeof(TValue));
	        return (TValue)Convert.ChangeType(this[column], Type.GetType(typeof(TValue).FullName));
        }
    }
}