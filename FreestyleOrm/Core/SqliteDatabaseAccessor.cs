﻿using System;
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
        public override string[] GetPrimaryKeys(QueryOptions queryOptions, MapRuleBasic mapRuleBasic)
        {
            List<string> columns = new List<string>();            

            if (string.IsNullOrEmpty(mapRuleBasic.PrimaryKeys))
            {
                using (var command = queryOptions.Connection.CreateCommand())
                {
                    command.Transaction = queryOptions.Transaction;            

                    string sql = $"pragma table_info({mapRuleBasic.Table})";

                    command.CommandText = sql;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (reader["pk"].ToString() == "0") continue;
                            columns.Add(reader["name"].ToString());
                        }

                        reader.Close();
                    }
                }
            }
            else
            {
                columns = mapRuleBasic.PrimaryKeys.Split(',').Select(x => x.Trim()).ToList();
            }

            if (columns.Count == 0) throw new InvalidOperationException($"[{mapRuleBasic.Table}] primary keys not setted.");       
            
            return columns.ToArray();
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

                TempTableSet tempTableSet = new TempTableSet();
                queryOptions.SetTempTables(tempTableSet);

                foreach (var tempTable in tempTableSet.TempTableList)
                {
                    string sql = $"drop table if exists {tempTable.Table}";

                    command.CommandText = sql;
                    command.ExecuteNonQuery();

                    outDorpTempTableSqls.Add(sql);

                    sql = $"create temporary table {tempTable.Table} ({tempTable.Columns})";

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
    }
}
