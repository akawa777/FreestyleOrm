using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Data;
using System.Reflection;
using System.Collections;
using System.Collections.Specialized;

namespace FreestyleOrm.Core
{
    internal interface IDatabaseAccessor
    {
        void BeginSave();        
        int Insert(Row row, QueryOptions queryOptions, out object lastId);
        int Update(Row row, QueryOptions queryOptions);
        int Delete(Row row, QueryOptions queryOptions);        
        IDataReader CreateTableReader(QueryOptions queryOptions, MapRuleBasic mapRuleBasic, out string[] primaryKeys);
        IDataReader CreateFetchReader(QueryOptions queryOptions, out Action dispose);
        string[] GetPrimaryKeys(QueryOptions queryOptions, MapRuleBasic mapRuleBasic);
        void CreateTempTable(QueryOptions queryOptions, List<string> outDorpTempTableSqls);
        void EndModifingRecord(Row afterRow, string commandName, OrderedDictionary beforeRecord, OrderedDictionary afterRecord, QueryOptions queryOptions);
    }

    internal enum ParameterFilter
    {
        All,
        PrimaryKeys,
        WithoutPrimaryKeys,
        RowVersion
    }

    internal class SqlServerDatabaseAccessor : IDatabaseAccessor
    {
        private Dictionary<string, object> _lastIdMap = new Dictionary<string, object>();

        public void BeginSave()
        {
            _lastIdMap.Clear();
        }

        public IDataReader CreateTableReader(QueryOptions queryOptions, MapRuleBasic mapRuleBasic, out string[] primaryKeys)
        {
            primaryKeys = GetPrimaryKeys(queryOptions, mapRuleBasic);

            IDbCommand command = queryOptions.Connection.CreateCommand();
            command.Transaction = queryOptions.Transaction;

            string sql = $"select * from {mapRuleBasic.Table} where 1 = 0";

            command.CommandText = sql;

            return command.ExecuteReader();
        }

        public virtual string[] GetPrimaryKeys(QueryOptions queryOptions, MapRuleBasic mapRuleBasic)
        {
            List<string> columns = new List<string>();

            if (string.IsNullOrEmpty(mapRuleBasic.PrimaryKeys))
            {
                string schema = mapRuleBasic.Table.Split('.').Length == 1 ? string.Empty : mapRuleBasic.Table.Split('.')[0];
                string table = mapRuleBasic.Table.Split('.').Length == 1 ? mapRuleBasic.Table.Split('.')[0] : mapRuleBasic.Table.Split('.')[1];


                string[] primaryKeys = GetPrimaryKeys(queryOptions, schema, table);

                foreach (var primaryKey in primaryKeys)
                {
                    columns.Add(primaryKey);
                }
            }
            else
            {
                columns = mapRuleBasic.PrimaryKeys.Split(',').Select(x => x.Trim()).ToList();
            }

            if (columns.Count == 0) throw new InvalidOperationException($"[{mapRuleBasic.Table}] primary keys not setted.");

            return columns.ToArray();
        }

        private string[] GetPrimaryKeys(QueryOptions queryOptions, string schema, string table)
        {
            List<string> columns = new List<string>();

            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                var tableType = string.Empty;

                command.CommandText = $"select TABLE_TYPE from INFORMATION_SCHEMA.TABLES where {(string.IsNullOrEmpty(schema) ? string.Empty : $"TABLE_SCHEMA = '{schema}' and ")}TABLE_NAME = '{table}'";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tableType = reader["TABLE_TYPE"].ToString();
                    }

                    reader.Close();
                }

                if (string.IsNullOrEmpty(tableType))
                {
                    throw new InvalidOperationException($"[{schema}.{table}] is not table or view.");
                }

                if (tableType.ToUpper() == "VIEW")
                {
                    command.CommandText = $"select top 1 TABLE_SCHEMA, TABLE_NAME from INFORMATION_SCHEMA.VIEW_COLUMN_USAGE where {(string.IsNullOrEmpty(schema) ? string.Empty : $"VIEW_SCHEMA = '{schema}' and ")}VIEW_NAME = '{table}'";

                    var entitySchema = string.Empty;
                    var entityTable = string.Empty;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            entitySchema = reader["TABLE_SCHEMA"].ToString();
                            entityTable = reader["TABLE_NAME"].ToString();
                        }

                        reader.Close();
                    }

                    return GetPrimaryKeys(queryOptions, entitySchema, entityTable);
                }

                command.CommandText = $"select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1 and {(string.IsNullOrEmpty(schema) ? string.Empty : $"TABLE_SCHEMA = '{schema}' and ")}TABLE_NAME = '{table}'";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns.Add(reader["COLUMN_NAME"].ToString());
                    }

                    reader.Close();
                }

                if (columns.Count == 0)
                {

                }
            }

            return columns.ToArray();
        }

        public int Insert(Row row, QueryOptions queryOptions, out object lastId)
        {
            lastId = null;

            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                Dictionary<string, IDbDataParameter> parameters = GetParameters(row, command, row.AutoId ? ParameterFilter.WithoutPrimaryKeys : ParameterFilter.All);

                if (!string.IsNullOrEmpty(row.RelationId.RelationIdColumn)
                    && (
                        row[row.RelationId.RelationIdColumn] == DBNull.Value
                        || row[row.RelationId.RelationIdColumn] == null
                        || row[row.RelationId.RelationIdColumn].ToString() == string.Empty
                        || (decimal.TryParse(row[row.RelationId.RelationIdColumn].ToString(), out decimal result) && result == 0)
                    ))
                {
                    if (_lastIdMap.TryGetValue(row.RelationId.RelationEntityPath, out object id))
                    {
                        parameters[row.RelationId.RelationIdColumn].Value = id;
                        row[row.RelationId.RelationIdColumn] = id;
                    }
                    else
                    {
                        throw new InvalidOperationException($"[{row.ExpressionPath}] RelationEntityPath is invalid.");
                    }
                }

                string columnNames = string.Join(", ", parameters.Keys);
                string paramNames = string.Join(", ", parameters.Values.Select(x => x.ParameterName));

                string sql = $"insert into {row.Table} ({columnNames}) values({paramNames})";

                command.CommandText = sql;
                foreach (var param in parameters.Values) command.Parameters.Add(param);

                OrderedDictionary beforeRecord = GetCurrentRecord(row, queryOptions);

                int rtn = command.ExecuteNonQuery();

                if (row.AutoId)
                {
                    lastId = GetLastId(row, queryOptions);
                    _lastIdMap[row.ExpressionPath] = lastId;
                    row[row.PrimaryKeys.First()] = lastId;
                }                

                OrderedDictionary afterRecord = GetCurrentRecord(row, queryOptions);

                EndModifingRecord(row, "insert", beforeRecord, afterRecord, queryOptions);

                return rtn;
            }
        }

        public int Update(Row row, QueryOptions queryOptions)
        {
            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                Dictionary<string, IDbDataParameter> rowVersionParameters = GetParameters(row, command, ParameterFilter.RowVersion);
                Dictionary<string, IDbDataParameter> parameters = GetParameters(row, command, ParameterFilter.WithoutPrimaryKeys);
                Dictionary<string, IDbDataParameter> primaryKeyParameters = GetParameters(row, command, ParameterFilter.PrimaryKeys);                

                IEnumerable<KeyValuePair<string, IDbDataParameter>> whereParameters = primaryKeyParameters.Concat(rowVersionParameters);

                string set = string.Join(", ", parameters.Select(x => $"{x.Key} = {x.Value.ParameterName}"));
                string where = string.Join(" and ", whereParameters.Select(x => $"{x.Key} = {x.Value.ParameterName}"));

                string sql = $"update {row.Table} set {set} where {where}";

                command.CommandText = sql;
                foreach (var param in parameters.Values) command.Parameters.Add(param);
                foreach (var param in whereParameters.Select(x => x.Value)) command.Parameters.Add(param);

                OrderedDictionary beforeRecord = GetCurrentRecord(row, queryOptions);

                int rtn = command.ExecuteNonQuery();

                _lastIdMap.Remove(row.ExpressionPath);

                if (rtn == 0) throw new DBConcurrencyException($"DBConcurrencyException at {row.Table} table.");

                OrderedDictionary afterRecord = GetCurrentRecord(row, queryOptions);

                EndModifingRecord(row, "update", beforeRecord, afterRecord, queryOptions);

                return rtn;
            }
        }

        public int Delete(Row row, QueryOptions queryOptions)
        {
            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                Dictionary<string, IDbDataParameter> rowVersionParameters = GetParameters(row, command, ParameterFilter.RowVersion);
                Dictionary<string, IDbDataParameter> primaryKeyParameters = GetParameters(row, command, ParameterFilter.PrimaryKeys);                

                IEnumerable<KeyValuePair<string, IDbDataParameter>> whereParameters = primaryKeyParameters.Concat(rowVersionParameters);

                string where = string.Join(" and ", whereParameters.Select(x => $"{x.Key} = {x.Value.ParameterName}"));

                string sql = $"delete from {row.Table} where {where}";

                command.CommandText = sql;
                foreach (var param in whereParameters.Select(x => x.Value)) command.Parameters.Add(param);

                OrderedDictionary beforeRecord = GetCurrentRecord(row, queryOptions);

                int rtn = command.ExecuteNonQuery();

                _lastIdMap.Remove(row.ExpressionPath);

                if (rtn == 0) throw new DBConcurrencyException($"DBConcurrencyException at {row.Table} table.");

                OrderedDictionary afterRecord = GetCurrentRecord(row, queryOptions);

                EndModifingRecord(row, "delete", beforeRecord, afterRecord, queryOptions);

                return rtn;
            }
        }

        private Dictionary<string, IDbDataParameter> GetParameters(Row row, IDbCommand command, ParameterFilter parameterFilter)
        {
            Dictionary<string, IDbDataParameter> parameters = new Dictionary<string, IDbDataParameter>();            

            foreach (var column in row.Columns)
            {
                bool isTargetColumn = false;

                if (parameterFilter == ParameterFilter.All)
                {
                    isTargetColumn = true;
                }
                else if (parameterFilter == ParameterFilter.PrimaryKeys)
                {
                    if (row.IsPrimaryKey(column)) isTargetColumn = true;
                }
                else if (parameterFilter == ParameterFilter.WithoutPrimaryKeys)
                {
                    if (!row.IsPrimaryKey(column)) isTargetColumn = true;
                }
                else if (parameterFilter == ParameterFilter.RowVersion)
                {
                    if (row.IsConcurrencyColumn(column, out int index)) isTargetColumn = true;
                }

                if (!isTargetColumn) continue;

                IDbDataParameter parameter = null;

                int rowVersionColumn = -1;
                if (parameterFilter == ParameterFilter.RowVersion)
                {                       
                    row.IsConcurrencyColumn(column, out rowVersionColumn);
                    object[] values = row.OptimisticLock.GetCurrentValues(row.Entity);

                    if (values == null || values.Length == 0)
                    {
                        throw new InvalidOperationException($"concurrency column value is invalid.");
                    }

                    if (rowVersionColumn < values.Length)
                    {
                        object value = values[rowVersionColumn];

                        if (value != null)
                        {
                            parameter = CreateParameter(command, $"row_version_{column}", value, false);
                        }
                    }
                }
                else if ((parameterFilter == ParameterFilter.All || parameterFilter == ParameterFilter.WithoutPrimaryKeys) && row.IsConcurrencyColumn(column, out int index))
                {
                    row.IsConcurrencyColumn(column, out rowVersionColumn);
                    object[] values = row.OptimisticLock.GetNewValues(row.Entity);

                    if (rowVersionColumn < values.Length)
                    {
                        object value = values[rowVersionColumn];

                        if (value != null)
                        {
                            parameter = CreateParameter(command, column, value, false);
                        }
                    }
                }
                else if (row.Columns.Contains(column))
                {                    
                    parameter = CreateParameter(command, column, row[column], false);
                }

                if (parameter != null) parameters[column] = parameter;
            }

            return parameters;
        }

        public IDbDataParameter CreateParameter(IDbCommand command, string name, object value, bool includeParamPrefix)
        {
            IDbDataParameter parameter = command.CreateParameter();
            parameter.ParameterName = includeParamPrefix ? name : $"@{name}";
            parameter.Value = value ?? DBNull.Value;

            return parameter;
        }

        public virtual object GetLastId(Row row, QueryOptions queryOptions)
        {
            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                string sql = $"select @@IDENTITY";

                command.CommandText = sql;

                return command.ExecuteScalar();
            }
        }        

        public string FormatSql(QueryOptions queryOptions, Dictionary<string, List<IDbDataParameter>> complexParameters)
        {
            string formatedSql = queryOptions.Sql;
            
            foreach (var param in complexParameters) formatedSql = formatedSql.Replace(param.Key, string.Join(", ", param.Value.Select(x => x.ParameterName)));

            return formatedSql;
        }

        public virtual void CreateTempTable(QueryOptions queryOptions, List<string> outDorpTempTableSqls)
        {
            outDorpTempTableSqls.Clear();

            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                TempTableSet tempTableSet = new TempTableSet();
                queryOptions.SetTempTables(tempTableSet);

                foreach (var tempTable in tempTableSet.TempTableList)
                {
                    string sql = $"if object_id(N'tempdb..{tempTable.Table}', N'U') IS NOT NULL begin drop table {tempTable.Table} end";

                    command.CommandText = sql;
                    command.ExecuteNonQuery();

                    outDorpTempTableSqls.Add(sql);

                    sql = $"create table {tempTable.Table} ({tempTable.Columns})";

                    command.CommandText = sql;
                    command.ExecuteNonQuery();

                    int indexNo = 0;
                    foreach (var index in tempTable.IndexList)
                    {
                        indexNo++;

                        sql = $@"create index idx_{indexNo} on {tempTable.Table} ({index})";

                        command.CommandText = sql;
                        command.ExecuteNonQuery();
                    }

                    foreach (var value in tempTable.ValueList)
                    {
                        command.Parameters.Clear();
                        
                        List<string> parameterNames = new List<string>();
                        List<IDbDataParameter> parameters = new List<IDbDataParameter>();


                        if (value is IDictionary<string, object> entries)
                        {
                            foreach (var entry in entries)
                            {
                                IDbDataParameter parameter = CreateParameter(command, entry.Key, entry.Value, false);
                                parameterNames.Add(entry.Key);
                                parameters.Add(parameter);
                            }
                        }
                        else
                        {
                            foreach (var property in value.GetType().GetPropertyMap(BindingFlags.GetProperty, PropertyTypeFilters.IgonreClass))
                            {
                                IDbDataParameter parameter = CreateParameter(command, property.Key, property.Value.Get(value), false);
                                parameterNames.Add(property.Key);
                                parameters.Add(parameter);
                            }
                        }

                        if (parameterNames.Count == 0) continue;

                        string columnNames = string.Join(", ", parameterNames.Select(x => x));
                        string paramNames = string.Join(", ", parameters.Select(x => x.ParameterName));

                        sql = $"insert into {tempTable.Table} ({columnNames}) values({paramNames})";

                        command.CommandText = sql;
                        foreach (var param in parameters) command.Parameters.Add(param);

                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        private List<IDbDataParameter> GetParameters(IDbCommand command, QueryOptions queryOptions, Dictionary<string, List<IDbDataParameter>> outComplexParameters)
        {
            outComplexParameters.Clear();
            List<IDbDataParameter> dbParameters = new List<IDbDataParameter>();
            Dictionary<string, object> parameters = new Dictionary<string, object>();            
            queryOptions.SetParams(parameters);

            foreach (var param in parameters)
            {                
                if (param.Value != null && param.Value.GetType() != typeof(string) && param.Value is IEnumerable)
                {
                    int suffix = 0;
                    List<IDbDataParameter> dbPrametersForComplex = new List<IDbDataParameter>();

                    foreach (var value in param.Value as IEnumerable)
                    {
                        suffix++;

                        IDbDataParameter dbParameter = CreateParameter(command, $"{param.Key}_{suffix}", value, true);
                        dbPrametersForComplex.Add(dbParameter);
                    }

                    outComplexParameters[param.Key] = dbPrametersForComplex;

                    dbParameters.AddRange(dbPrametersForComplex);
                }
                else
                {
                    IDbDataParameter dbParameter = CreateParameter(command, param.Key, param.Value, true);
                    dbParameters.Add(dbParameter);
                }
            }

            return dbParameters;
        }

        public IDataReader CreateFetchReader(QueryOptions queryOptions, out Action dispose)
        {
            List<string> dropTempTableSqls = new List<string>();
            CreateTempTable(queryOptions, dropTempTableSqls);            

            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                dispose = () =>
                {
                    foreach (var dropTempTableSql in dropTempTableSqls)
                    {
                        command.CommandText = dropTempTableSql;
                        command.ExecuteNonQuery();
                    }
                };

                Dictionary<string, List<IDbDataParameter>> complexParameters = new Dictionary<string, List<IDbDataParameter>>();
                IEnumerable<IDbDataParameter> parameters = GetParameters(command, queryOptions, complexParameters);
                string sql = FormatSql(queryOptions, complexParameters);

                command.CommandText = sql;
                foreach (var param in parameters) command.Parameters.Add(param);                

                return command.ExecuteReader();                
            }
        }

        private OrderedDictionary GetCurrentRecord(Row row, QueryOptions queryOptions)
        {
            using (IDataReader dataReader = SelectCurrentRecord(row, queryOptions))
            {
                OrderedDictionary currentRecord = new OrderedDictionary();

                while (dataReader.Read())
                {
                    for (var i = 0; i < dataReader.FieldCount; i++)
                    {
                        string name = dataReader.GetName(i);
                        object value = dataReader[i];
                        currentRecord[name] = value;
                    }
                }

                return currentRecord;
            }
        }

        protected virtual IDataReader SelectCurrentRecord(Row row, QueryOptions queryOptions)
        {
            IDbCommand command = queryOptions.Connection.CreateCommand();
            command.Transaction = queryOptions.Transaction;

            Dictionary<string, IDbDataParameter> primaryKeyParameters = GetParameters(row, command, ParameterFilter.PrimaryKeys);

            string where = string.Join(" and ", primaryKeyParameters.Select(x => $"{x.Key} = {x.Value.ParameterName}"));
            
            foreach (var param in primaryKeyParameters.Select(x => x.Value)) command.Parameters.Add(param);            

            string sql = $"select * from {row.Table} where {where} ";

            command.CommandText = sql;

            return command.ExecuteReader();
        }

        public void EndModifingRecord(Row afterRow, string commandName, OrderedDictionary beforeRecord, OrderedDictionary afterRecord, QueryOptions queryOptions)
        {
            OrderedDictionary primaryValues = new OrderedDictionary();

            foreach (var key in afterRow.PrimaryKeys)
            {
                OrderedDictionary record = commandName == "insert" ? afterRecord : beforeRecord;
                primaryValues[key] = record[key];
            }

            ModifingEntry modifingEntry = new ModifingEntry(queryOptions.Connection, queryOptions.Transaction, afterRow.Table, primaryValues, commandName, beforeRecord, afterRecord);

            queryOptions.QueryDefine.EndModifingRecord(modifingEntry);
        }
    }
}
