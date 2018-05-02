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
    internal class MySqlDatabaseAccessor : SqliteDatabaseAccessor
    {
        public override string[] GetPrimaryKeys(QueryOptions queryOptions, MapRule mapRule)
        {
            List<string> columns = new List<string>();            

            if (string.IsNullOrEmpty(mapRule.PrimaryKeys))
            {
                using (var command = queryOptions.Connection.CreateCommand())
                {
                    command.Transaction = queryOptions.Transaction;            

                    string sql = $"show keys from {mapRule.Table} where Key_name = 'PRIMARY'";

                    command.CommandText = sql;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {                        
                            columns.Add(reader["Column_name"].ToString());
                        }

                        reader.Close();
                    }
                }
            }
            else
            {
                columns = mapRule.PrimaryKeys.Split(',').Select(x => x.Trim()).ToList();
            }

            if (columns.Count == 0) throw new InvalidOperationException($"[{mapRule.Table}] primary keys not setted.");       
            
            return columns.ToArray();
        }

        public override object GetLastId(Row row, QueryOptions queryOptions)
        {
            using (var command = queryOptions.Connection.CreateCommand())
            {
                command.Transaction = queryOptions.Transaction;

                string sql = $"select last_insert_id()";

                command.CommandText = sql;

                return command.ExecuteScalar();
            }
        }
    }
}
