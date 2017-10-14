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
    internal class SqliteDatabaseAccessor : SqlServerDatabaseAccessor
    {
        public override string[] GetPrimaryKeys(QueryOptions queryOptions, MapOptions mapOptions)
        {
            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;            

                string sql = $"pragma table_info({mapOptions.Table})";

                command.CommandText = sql;

                List<string> columns = new List<string>();

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        if (reader["pk"].ToString() == "0") continue;
                        columns.Add(reader["name"].ToString());
                    }

                    reader.Close();
                }

                return columns.ToArray();
            }
        }

        public override object GetLastId(Row row, QueryOptions queryOptions)
        {
            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                string sql = $"select last_insert_rowid()";

                command.CommandText = sql;

                return command.ExecuteScalar();
            }
        }

        public override void CreateTempTable(QueryOptions queryOptions, List<string> outDorpTempTableSqls)
        {
            outDorpTempTableSqls.Clear();

            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                Dictionary<string, TempTable> tempTables = new Dictionary<string, TempTable>();
                queryOptions.SetTempTables(tempTables);

                foreach (var entry in tempTables)
                {
                    if (entry.Value == null) throw new InvalidOperationException("TempTable is null.");
                    if (entry.Value.Columns == null) throw new InvalidOperationException("TempTable.Columns is null.");
                    if (entry.Value.Values == null) throw new InvalidOperationException("TempTable.Values is null.");

                    string sql = $"drop table if exists {entry.Key}";

                    command.CommandText = sql;
                    command.ExecuteNonQuery();

                    outDorpTempTableSqls.Add(sql);

                    sql = $"create temporary table {entry.Key} ({entry.Value.Columns})";

                    command.CommandText = sql;
                    command.ExecuteNonQuery();

                    int indexNo = 0;
                    if (entry.Value.IndexSet != null)
                    {
                        foreach (var index in entry.Value.IndexSet)
                        {
                            indexNo++;

                            sql = $@"create index idx_{indexNo} on {entry.Key} ({index})";

                            command.CommandText = sql;
                            command.ExecuteNonQuery();
                        }
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
    }
}
