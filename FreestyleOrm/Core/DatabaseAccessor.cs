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
        IDataReader CreateTableReader(QueryOptions queryOptions, MapOptions mapOptions, out string[] primaryKeys);
        IDataReader CreateFetchReader(QueryOptions queryOptions, out Action dispose);
    }

    internal enum ParameterFilter
    {
        All,
        PrimaryKeys,
        WithoutPrimaryKeys,
        RowVersion
    }

    internal class DatabaseAccessor : IDatabaseAccessor
    {
        private Dictionary<string, object> _lastIdMap = new Dictionary<string, object>();

        public void BeginSave()
        {
            _lastIdMap.Clear();
        }

        public IDataReader CreateTableReader(QueryOptions queryOptions, MapOptions mapOptions, out string[] primaryKeys)
        {
            primaryKeys = GetPrimaryKeys(queryOptions, mapOptions);

            IDbCommand command = queryOptions.Connection.CreateCommand();
            command.Transaction = queryOptions.Transaction;

            string sql = $"select * from {mapOptions.Table} where 1 = 0";

            command.CommandText = sql;

            return command.ExecuteReader();
        }

        private string[] GetPrimaryKeys(QueryOptions queryOptions, MapOptions mapOptions)
        {
            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                string table = mapOptions.Table.Split('.').Length == 1 ? mapOptions.Table.Split('.')[0] : mapOptions.Table.Split('.')[1];
                string schema = mapOptions.Table.Split('.').Length == 1 ? string.Empty : $"AND TABLE_SCHEMA = '{mapOptions.Table.Split('.')[0]}'";

                string sql = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1 AND TABLE_NAME = '{table}' {schema}";

                command.CommandText = sql;

                List<string> columns = new List<string>();

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns.Add(reader["COLUMN_NAME"].ToString());
                    }

                    reader.Close();
                }

                return columns.ToArray();
            }
        }

        public int Insert(Row row, QueryOptions queryOptions, out object lastId)
        {
            lastId = null;

            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                Dictionary<string, IDbDataParameter> parameters = GetParameters(row, command, row.AutoId ? ParameterFilter.WithoutPrimaryKeys : ParameterFilter.All);

                if (!string.IsNullOrEmpty(row.RelationIdColumn)
                    && (
                        row[row.RelationIdColumn] == DBNull.Value
                        || row[row.RelationIdColumn] == null
                        || row[row.RelationIdColumn].ToString() == string.Empty
                        || (decimal.TryParse(row[row.RelationIdColumn].ToString(), out decimal result) && result == 0)
                    )
                    && _lastIdMap.TryGetValue(row.RelationEntityPath, out object id))
                {
                    parameters[row.RelationIdColumn].Value = id;
                }

                string columnNames = string.Join(", ", parameters.Keys);
                string paramNames = string.Join(", ", parameters.Values.Select(x => x.ParameterName));

                string sql = $"insert into {row.Table} ({columnNames}) values({paramNames})";

                command.CommandText = sql;
                foreach (var param in parameters.Values) command.Parameters.Add(param);

                int rtn = command.ExecuteNonQuery();

                if (row.AutoId)
                {
                    lastId = GetLastId(row, queryOptions);
                    _lastIdMap[row.ExpressionPath] = lastId;
                }

                return rtn;
            }
        }

        public int Update(Row row, QueryOptions queryOptions)
        {
            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                Dictionary<string, IDbDataParameter> parameters = GetParameters(row, command, ParameterFilter.WithoutPrimaryKeys);
                Dictionary<string, IDbDataParameter> primaryKeyParameters = GetParameters(row, command, ParameterFilter.PrimaryKeys);
                Dictionary<string, IDbDataParameter> rowVersionParameters = GetParameters(row, command, ParameterFilter.RowVersion);

                IEnumerable<KeyValuePair<string, IDbDataParameter>> whereParameters = primaryKeyParameters.Concat(rowVersionParameters);

                string set = string.Join(", ", parameters.Select(x => $"{x.Key} = {x.Value.ParameterName}"));
                string where = string.Join(" and ", whereParameters.Select(x => $"{x.Key} = {x.Value.ParameterName}"));

                string sql = $"update {row.Table} set {set} where {where}";

                command.CommandText = sql;
                foreach (var param in parameters.Values) command.Parameters.Add(param);
                foreach (var param in whereParameters.Select(x => x.Value)) command.Parameters.Add(param);

                int rtn = command.ExecuteNonQuery();

                _lastIdMap.Remove(row.ExpressionPath);

                if (!string.IsNullOrEmpty(row.RowVersionColumn) && row.IsRootRow && rtn == 0) throw new InvalidOperationException("Concurrency is invalid.");

                return rtn;
            }
        }

        public int Delete(Row row, QueryOptions queryOptions)
        {
            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                Dictionary<string, IDbDataParameter> primaryKeyParameters = GetParameters(row, command, ParameterFilter.PrimaryKeys);
                Dictionary<string, IDbDataParameter> rowVersionParameters = GetParameters(row, command, ParameterFilter.RowVersion);

                IEnumerable<KeyValuePair<string, IDbDataParameter>> whereParameters = primaryKeyParameters.Concat(rowVersionParameters);

                string where = string.Join(" and ", whereParameters.Select(x => $"{x.Key} = {x.Value.ParameterName}"));

                string sql = $"delete from {row.Table} where {where}";

                command.CommandText = sql;
                foreach (var param in whereParameters.Select(x => x.Value)) command.Parameters.Add(param);

                int rtn = command.ExecuteNonQuery();

                _lastIdMap.Remove(row.ExpressionPath);

                if (!string.IsNullOrEmpty(row.RowVersionColumn) && row.IsRootRow && rtn == 0) throw new InvalidOperationException("Concurrency is invalid.");

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
                    if (row.IsPrimaryKey(column) || row.IsRowVersionColumn(column)) isTargetColumn = true;
                }
                else if (parameterFilter == ParameterFilter.WithoutPrimaryKeys)
                {
                    if (!row.IsPrimaryKey(column)) isTargetColumn = true;
                }

                if (!isTargetColumn) continue;

                IDbDataParameter parameter;

                if (parameterFilter != ParameterFilter.PrimaryKeys
                    && !string.IsNullOrEmpty(row.RowVersionColumn)
                    && row.NewRowVersion != null
                    && column == row.RowVersionColumn)
                {
                    parameter = CreateParameter(command, $"new_{column}", row.NewRowVersion, false);
                }
                else
                {
                    parameter = CreateParameter(command, column, row[column], false);
                }

                parameters[column] = parameter;
            }

            return parameters;
        }

        public IDbDataParameter CreateParameter(IDbCommand command, string name, object value, bool includeParamPrefix)
        {
            IDbDataParameter parameter = command.CreateParameter();
            parameter.ParameterName = includeParamPrefix ? name : $"@{name}";
            parameter.Value = value == null ? DBNull.Value : value;

            return parameter;
        }

        public object GetLastId(Row row, QueryOptions queryOptions)
        {
            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                string sql = $"select @@IDENTITY Id";

                command.CommandText = sql;

                return command.ExecuteScalar();
            }
        }        

        public string FormatSql(QueryOptions queryOptions, Dictionary<string, List<IDbDataParameter>> complexParameters)
        {
            string formatedSql = queryOptions.Sql;

            Dictionary<string, object> formats = new Dictionary<string, object>();
            queryOptions.SetFormats(formats);

            foreach (var format in formats) formatedSql = formatedSql.Replace("{{" + format.Key + "}}", format.Value.ToString());
            foreach (var param in complexParameters) formatedSql = formatedSql.Replace(param.Key, string.Join(", ", param.Value.Select(x => x.ParameterName)));

            return formatedSql;
        }

        public void CreateTempTable(QueryOptions queryOptions, List<string> outDorpTempTableSqls)
        {
            outDorpTempTableSqls.Clear();

            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                Dictionary<string, TempTable> tempTables = new Dictionary<string, TempTable>();
                queryOptions.SetTempTables(tempTables);

                foreach (var entry in tempTables)
                {
                    string sql = $"if object_id(N'tempdb..{entry.Key}', N'U') IS NOT NULL begin drop table {entry.Key} end";

                    command.CommandText = sql;
                    command.ExecuteNonQuery();

                    outDorpTempTableSqls.Add(sql);

                    sql = $"create table {entry.Key} ({entry.Value.Columns})";

                    command.CommandText = sql;
                    command.ExecuteNonQuery();

                    int indexNo = 0;
                    foreach (var index in entry.Value.IndexSet)
                    {
                        indexNo++;

                        sql = $@"create index idx_{indexNo} on {entry.Key} ({index})";

                        command.CommandText = sql;
                        command.ExecuteNonQuery();
                    }

                    foreach (var value in entry.Value.Values)
                    {
                        command.Parameters.Clear();
                        
                        List<string> parameterNames = new List<string>();
                        List<IDbDataParameter> parameters = new List<IDbDataParameter>();

                        foreach (var property in value.GetType().GetPropertyMap(BindingFlags.GetProperty, PropertyTypeFilters.IgonreClass))
                        {
                            IDbDataParameter parameter = CreateParameter(command, property.Key, property.Value.Get(value), false);
                            parameterNames.Add(property.Key);
                            parameters.Add(parameter);
                        }

                        string columnNames = string.Join(", ", parameterNames.Select(x => x));
                        string paramNames = string.Join(", ", parameters.Select(x => x.ParameterName));

                        sql = $"insert into {entry.Key} ({columnNames}) values({paramNames})";

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
            queryOptions.SetParameters(parameters);

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
    }
}
