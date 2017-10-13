using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Data;
using FreestyleOrm.Core;

namespace FreestyleOrm
{
    public interface IQuery<TRootEntity> where TRootEntity : class
    {
        IQuery<TRootEntity> Map(Action<IMap<TRootEntity>> setMap);
        IQuery<TRootEntity> Parametes(Action<Dictionary<string, object>> setParameters);
        IQuery<TRootEntity> Formats(Action<Dictionary<string, object>> setFormats);
        IQuery<TRootEntity> TempTables(Action<Dictionary<string, TempTable>> setTempTables);
        IQuery<TRootEntity> Connection(IDbConnection connection);
        IQuery<TRootEntity> Transaction(IDbTransaction transaction);
        IEnumerable<TRootEntity> Fetch();
        IEnumerable<TRootEntity> Fetch(int page, int size);
        void Insert<TId>(TRootEntity rootEntity, out TId lastId);
        void Insert(TRootEntity rootEntity);
        void Update(TRootEntity rootEntity);
        void Delete(TRootEntity rootEntity);

    }

    public interface IMap<TRootEntity> where TRootEntity : class
    {
        IMapOptions<TRootEntity, TRootEntity> To();
        IMapOptions<TRootEntity, TEntity> ToOne<TEntity>(Expression<Func<TRootEntity, TEntity>> target) where TEntity : class;
        IMapOptions<TRootEntity, TEntity> ToMany<TEntity>(Expression<Func<TRootEntity, IEnumerable<TEntity>>> target) where TEntity : class;
    }

    public interface IMapOptions<TRootEntity, TEntity> where TEntity :class where TRootEntity : class
    {
        IMapOptions<TRootEntity, TEntity> UniqueKeys(string columns);
        IMapOptions<TRootEntity, TEntity> IncludePrefix(string prefix);
        IMapOptions<TRootEntity, TEntity> Refer(Refer refer);
        IMapOptions<TRootEntity, TEntity> GetEntity(Func<IRow, TRootEntity, TEntity> getEntity);
        IMapOptions<TRootEntity, TEntity> SetRow(Action<TEntity, TRootEntity, IRow> setRow);
        IMapOptions<TRootEntity, TEntity> Table(string table);
        IMapOptions<TRootEntity, TEntity> RelationId<TRelationEntity>(string relationIdColumn, Expression<Func<TRootEntity, TRelationEntity>> relationEntity) where TRelationEntity : class;
        IMapOptions<TRootEntity, TEntity> FormatPropertyName(Func<string, string> formatPropertyName);
        IMapOptions<TRootEntity, TEntity> AutoId(bool autoId);                
        IMapOptions<TRootEntity, TEntity> OptimisticLock<TRowVersion>(string rowVersionColumn, Func<TEntity, TRowVersion> newRowVersion = null);
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
    }
}