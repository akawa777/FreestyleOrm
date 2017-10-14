using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using FreestyleOrm.Core;

namespace FreestyleOrm
{
    public static class Extensions
    {
        public static IQuery<TRootEntity> Query<TRootEntity>(this IDbConnection connection, string sql, IQueryDefine queryDefine = null) where TRootEntity : class
        {
            if (connection == null) throw new AggregateException("connection is null.");
            if (string.IsNullOrEmpty(sql)) throw new AggregateException("sql is null or empty.");

            IDatabaseAccessor databaseAccessor;

            if (connection.GetType().Name.ToLower().IndexOf("sqlite") != -1)
            {
                databaseAccessor = new SqliteDatabaseAccessor();
            }
            else if (connection.GetType().Name.ToLower().IndexOf("mysql") != -1)
            {
                databaseAccessor = new MySqlDatabaseAccessor();
            }
            else
            {
                databaseAccessor = new SqlServerDatabaseAccessor();
            }

            if (queryDefine == null) queryDefine = new QueryDefine();

            QueryOptions queryOptions = new QueryOptions(queryDefine, typeof(TRootEntity));
            queryOptions.Connection = connection;
            queryOptions.Sql = sql;

            return new Query<TRootEntity>(databaseAccessor, queryOptions, queryDefine);
        }
    }
}
