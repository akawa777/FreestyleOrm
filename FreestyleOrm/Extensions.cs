using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
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

        public static IQueryBase<IRowBase> Query(this IDbConnection connection, string sql, IQueryDefine queryDefine = null) 
        {
            return Query<IRowBase>(connection, sql, queryDefine);
        }

        public static void AddMap<TKey, TValue>(this IDictionary<string, object> self, IDictionary<TKey, TValue> map)
        {
            foreach (var entry in map)
            {
                self[entry.Key.ToString()] = entry.Value;
            }
        }

        public static void AddMap<TKey, TValue>(this IDictionary<string, object> self, IParamMapGetter<TKey, TValue> paramMapGetter)
        {
            self.AddMap(paramMapGetter.ParamMap);
        }

        public static IEnumerable<string> Merge(this IEnumerable<string> self, IEnumerable<string> second)
        {
            Dictionary<string, string> exists = new Dictionary<string, string>();

            foreach (var item in self.Concat(second))
            {
                if (!exists.ContainsKey(item))
                {
                    exists[item] = item;
                    yield return item;
                }
            }
        }

        public static void Merge(this List<string> self, IEnumerable<string> second)
        {
            Dictionary<string, string> exists = new Dictionary<string, string>();

            foreach (var item in second)
            {
                if (!self.Any(x => x == item))
                {   
                    self.Add(item);
                }
            }
        }
    }
}
