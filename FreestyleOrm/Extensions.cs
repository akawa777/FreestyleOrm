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
            IDatabaseAccessor databaseAccessor = new DatabaseAccessor();

            if (queryDefine == null) queryDefine = new QueryDefine();

            QueryOptions queryOptions = new QueryOptions(queryDefine, typeof(TRootEntity));
            queryOptions.Connection = connection;
            queryOptions.Sql = sql;

            return new Query<TRootEntity>(databaseAccessor, queryOptions, queryDefine);
        }
    }
}
