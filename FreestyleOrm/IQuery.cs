﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Data;
using System.Reflection;
using FreestyleOrm.Core;
using System.Collections;
using System.Collections.Specialized;

namespace FreestyleOrm
{
    public interface IAction
    {
        IEnumerable<OrderedDictionary> Fetch();
        Page<OrderedDictionary> Page(int no, int size);
        void Insert<TId>(OrderedDictionary rootEntity, out TId lastId);
        void Insert(OrderedDictionary rootEntity);
        void Update(OrderedDictionary rootEntity);
        void Delete(OrderedDictionary rootEntity);
    }

    public interface IAction<TRootEntity> where TRootEntity : class
    {
        IEnumerable<TRootEntity> Fetch();
        Page<TRootEntity> Page(int no, int size);
        void Insert<TId>(TRootEntity rootEntity, out TId lastId);
        void Insert(TRootEntity rootEntity);
        void Update(TRootEntity rootEntity);
        void Delete(TRootEntity rootEntity);
    }

    public interface IQuery : IAction
    {
        IQuery Params(Action<Dictionary<string, object>> setParams);
        IQuery TempTables(Action<ITempTableSet> setTempTables);
        IQuery Connection(IDbConnection connection);
        IQuery Transaction(IDbTransaction transaction);
        IQuery Map(Action<IMap> setMap);
    }
        
    public interface IQuery<TRootEntity> : IAction<TRootEntity> where TRootEntity : class
    {
        IQuery<TRootEntity> Params(Action<Dictionary<string, object>> setParams);
        IQuery<TRootEntity> TempTables(Action<ITempTableSet> setTempTables);
        IQuery<TRootEntity> Connection(IDbConnection connection);
        IQuery<TRootEntity> Transaction(IDbTransaction transaction);
        IQuery<TRootEntity> Map(Action<IMap<TRootEntity>> setMap);
    }

    public interface IMap
    {
        IMapRule ToRoot();
    }

    public interface IMap<TRootEntity> where TRootEntity : class
    {
        IMapRule<TRootEntity, TRootEntity> ToRoot();
        IMapRule<TRootEntity, TEntity> ToOne<TEntity>(Expression<Func<TRootEntity, TEntity>> target) where TEntity : class;
        IMapRule<TRootEntity, TEntity> ToMany<TEntity>(Expression<Func<TRootEntity, IEnumerable<TEntity>>> target) where TEntity : class;
    }

    public interface IMapRule
    {
        IMapRule Writable();
        IMapRule SetEntity(Action<IRow, OrderedDictionary> setEntity);
        IMapRule SetRow(Action<OrderedDictionary,IRow> setRow);
        IMapRule Table(string table, string primaryKeys = null);
        IMapRule AutoId();
        IMapRule OptimisticLock(Action<IOptimisticLock<OrderedDictionary>> setOptimisticLock);
        IMapRule ClearRule(Func<IMapRule<OrderedDictionary, OrderedDictionary>, string> methodName);
    }

    public interface IMapRule<TRootEntity, TEntity> where TEntity : class where TRootEntity : class
    {
        IMapRule<TRootEntity, TEntity> UniqueKeys(string columns);
        IMapRule<TRootEntity, TEntity> IncludePrefix(string prefix);
        IMapRule<TRootEntity, TEntity> Writable();
        IMapRule<TRootEntity, TEntity> CreateEntity(Func<IRow, TRootEntity, TEntity> createEntity);
        IMapRule<TRootEntity, TEntity> SetEntity(Action<IRow, TRootEntity, TEntity> setEntity);
        IMapRule<TRootEntity, TEntity> SetRow(Action<TEntity, TRootEntity, IRow> setRow);
        IMapRule<TRootEntity, TEntity> Table(string table, string primaryKeys = null);
        IMapRule<TRootEntity, TEntity> RelationId<TRelationEntity>(string relationIdColumn, Expression<Func<TRootEntity, TRelationEntity>> relationEntity) where TRelationEntity : class;
        IMapRule<TRootEntity, TEntity> FormatPropertyName(Func<string, string> formatPropertyName);
        IMapRule<TRootEntity, TEntity> AutoId();                
        IMapRule<TRootEntity, TEntity> OptimisticLock(Action<IOptimisticLock<TEntity>> setOptimisticLock);      
        [Obsolete("currently not used.")]
        IMapRule<TRootEntity, TEntity> ReNest<TProperty, TId, TParentId>(Expression<Func<TEntity, IEnumerable<TProperty>>> nestEntity, Expression<Func<TEntity, TId>> idPropertiy, Expression<Func<TEntity, TParentId>> parentProperty) where  TProperty : TEntity;
        IMapRule<TRootEntity, TEntity> ClearRule(Func<IMapRule<TRootEntity, TEntity>, string> methodName);
    }

    public interface IFormat
    {
        IFormat Set(string name, Func<string> getFormat, Func<bool> validation = null);
    }

    public class Page<TRootEntity>
    {
        internal Page(int no, int size, int total, IEnumerable<TRootEntity> items)
        {
            PageNo = no;
            PageSize = size;
            Total = total;
            MaxPageNo = (total + size - 1) / size;

            if (MaxPageNo < 1 && items.Count() > 0) MaxPageNo = 1;

            Items = items;
        }

        public int PageNo { get; } 
        public int PageSize { get; }
        public int Total { get; }  
        public int MaxPageNo { get; }
        public IEnumerable<TRootEntity> Items { get; }
    }

    public interface IRow
    {
        object this[string column] { get; set; }
        IEnumerable<string> Columns { get; }
        TValue Get<TValue>(string column);
        void BindEntity(object entity);
        void BindRow(object entity);
    }

    public interface ITempTableSet
    {
        ITempTable Table(string table, string columns);
    }

    public interface ITempTable
    {
        ITempTable Indexes(params string[] indexes);
        ITempTable Values(params object[] values);
        ITempTable Values(IDictionary<string, object>[] values);
    }


    public interface IMapRuleBasic
    {
        Type RootEntityType { get; }
        string ExpressionPath { get; }
        Type EntityType { get; }
        PropertyInfo Property { get; }
        string Table { get; }
        string PrimaryKeys { get; }
        bool IsFlatFormat { get; }
    }

    public enum SaveMode
    {
        Insert,
        Update,
        Delete
    }

    public interface IOptimisticLock
    {
        IOptimisticLock Columns(string columns);
        IOptimisticLock CurrentValues(Func<object, object[]> values);
        IOptimisticLock NewValues(Func<object, object[]> values);
    }

    public interface IOptimisticLock<TEntity> where TEntity : class
    {
        IOptimisticLock<TEntity> Columns(string columns);
        IOptimisticLock<TEntity> CurrentValues(Func<TEntity, object[]> values);
        IOptimisticLock<TEntity> NewValues(Func<TEntity, object[]> values);
    }

    public interface IParamMapGetter<TKey, TValue>
    {
        Dictionary<TKey, TValue> ParamMap { get; }
    }

    public interface ISqlSpec : IParamMapGetter<string, object>
    {
        
    }
}