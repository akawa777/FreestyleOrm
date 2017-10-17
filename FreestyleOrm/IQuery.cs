using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Data;
using System.Reflection;
using FreestyleOrm.Core;

namespace FreestyleOrm
{
    public interface IQuery<TRootEntity> where TRootEntity : class
    {
        IQuery<TRootEntity> Map(Action<IMap<TRootEntity>> setMap);
        IQuery<TRootEntity> Params(Action<Dictionary<string, object>> setParams);
        IQuery<TRootEntity> Formats(Action<Dictionary<string, object>> setFormats);
        IQuery<TRootEntity> TempTables(Action<ITempTableSet> setTempTables);
        IQuery<TRootEntity> Connection(IDbConnection connection);
        IQuery<TRootEntity> Transaction(IDbTransaction transaction);
        IEnumerable<TRootEntity> Fetch();
        Page<TRootEntity> Page(int no, int size);
        void Insert<TId>(TRootEntity rootEntity, out TId lastId);
        void Insert(TRootEntity rootEntity);
        void Update(TRootEntity rootEntity);
        void Delete(TRootEntity rootEntity);
    }

    public interface IMap<TRootEntity> where TRootEntity : class
    {
        IMapRule<TRootEntity, TRootEntity> To();
        IMapRule<TRootEntity, TEntity> ToOne<TEntity>(Expression<Func<TRootEntity, TEntity>> target) where TEntity : class;
        IMapRule<TRootEntity, TEntity> ToMany<TEntity>(Expression<Func<TRootEntity, IEnumerable<TEntity>>> target) where TEntity : class;
    }

    public interface IMapRule<TRootEntity, TEntity> where TEntity :class where TRootEntity : class
    {
        IMapRule<TRootEntity, TEntity> UniqueKeys(string columns);
        IMapRule<TRootEntity, TEntity> IncludePrefix(string prefix);
        IMapRule<TRootEntity, TEntity> Refer(Refer refer);
        IMapRule<TRootEntity, TEntity> GetEntity(Func<IRow, TRootEntity, TEntity> getEntity);
        IMapRule<TRootEntity, TEntity> SetRow(Action<TEntity, TRootEntity, IRow> setRow);
        IMapRule<TRootEntity, TEntity> Table(string table);
        IMapRule<TRootEntity, TEntity> RelationId<TRelationEntity>(string relationIdColumn, Expression<Func<TRootEntity, TRelationEntity>> relationEntity) where TRelationEntity : class;
        IMapRule<TRootEntity, TEntity> FormatPropertyName(Func<string, string> formatPropertyName);
        IMapRule<TRootEntity, TEntity> AutoId(bool autoId);                
        IMapRule<TRootEntity, TEntity> OptimisticLock<TRowVersion>(string rowVersionColumn, Func<TEntity, TRowVersion> newRowVersion = null);
    }

    public class Page<TRootEntity>
    {
        internal Page(int no, IEnumerable<TRootEntity> list, int total)
        {
            No = no;
            Lines = list;
            Total = total;            
        }

        public int No { get; }
        public IEnumerable<TRootEntity> Lines { get; }        
        public int Total { get; }        
    }

    public enum Refer
    {
        Read,
        Write
    }

    public interface IRow
    {        
        object this[string column] { get; set; }
        void BindEntity(object entity);
        void BindRow(object entity);
        IEnumerable<string> Columns { get; }
        TValue Get<TValue>(string column);
    }

    public interface ITempTableSet
    {
        ITempTable Table(string table, string columns);
    }

    public interface ITempTable
    {
        ITempTable Indexes(params string[] indexes);
        ITempTable Values(params object[] values);
    }


    public interface IMapRule
    {
        string ExpressionPath { get; }
        Type EntityType { get; }
        PropertyInfo Property { get; }
        string Table { get; }
    }
}