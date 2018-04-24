using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Linq;

namespace FreestyleOrm
{
    public interface IQueryDefine
    {        
        string GetTable(IMapRule mapRule);
        void SetRelationId(IMapRule mapRule, RelationId relationId);
        bool GetAutoId(IMapRule mapRule);
        string GetFormatPropertyName(IMapRule mapRule, string column);
        object CreateEntity(IMapRule mapRule, object rootEntity);
        void SetEntity(IMapRule mapRule, IRow row, object rootentiy, object entity);
        void SetRow(IMapRule mapRule, object entity, object rootentiy, IRow row);
        void SetOptimisticLock(IMapRule mapRule, OptimisticLock optimisticLock);
        string GetIncludePrefix(IMapRule mapRule);        
        string GetUniqueKeys(IMapRule mapRule);
    }

    public interface IOptimisticLock
    {
        string[] GetColumns();
        object[] GetCurrentValues(object entity);
        object[] GetNewValues(object entity);
    }

    public class OptimisticLock : IOptimisticLock
    {
        public string[] GetColumns() => string.IsNullOrEmpty(_columns) ? new string[0] : _columns.Split(',').Select(x => x.Trim()).ToArray();
        public object[] GetCurrentValues(object entity) => _currentValues(entity);
        public object[] GetNewValues(object entity) => _newValues(entity);

        protected string _columns;
        protected Func<object, object[]> _currentValues;
        protected Func<object, object[]> _newValues;

        public IOptimisticLock Columns(string columns)
        {
            _columns = columns;

            return this;
        }

        public IOptimisticLock CurrentValues(Func<object, object[]> values)
        {
            _currentValues = values;

            return this;
        }

        public IOptimisticLock NewValues(Func<object, object[]> values)
        {
            _newValues = values;

            return this;
        }
    }

    public interface IOptimisticLock<TEntity> where TEntity : class
    {
        IOptimisticLock<TEntity> Columns(string columns);
        IOptimisticLock<TEntity> CurrentValues(Func<TEntity, object[]> values);
        IOptimisticLock<TEntity> NewValues(Func<TEntity, object[]> values);
    }

    internal class OptimisticLock<TEntity> : OptimisticLock, IOptimisticLock, IOptimisticLock<TEntity> where TEntity : class
    {
        IOptimisticLock<TEntity> IOptimisticLock<TEntity>.Columns(string columns)
        {
            _columns = columns;

            return this;
        }

        IOptimisticLock<TEntity> IOptimisticLock<TEntity>.CurrentValues(Func<TEntity, object[]> values)
        {
            _currentValues = entity => values(entity as TEntity);

            return this;
        }

        IOptimisticLock<TEntity> IOptimisticLock<TEntity>.NewValues(Func<TEntity, object[]> values)
        {
            _newValues = entity => values(entity as TEntity);

            return this;
        }
    }

    public class RelationId
    {        
        public string RelationIdColumn { get; set; }
        public string RelationEntityPath { get; set; }
    }

    internal class ReNest
    {   
        public string NestEntityPath { get; set; }
        public string[] IdProperties { get; set; }
        public string[] ParentProperties { get; set; }
        public bool Should() => !string.IsNullOrEmpty(NestEntityPath);
    }

    public class QueryDefine : IQueryDefine
    {

        public virtual void SetFormats(Type rootEntityType, Dictionary<string, object> formats)
        {

        }

        public virtual object CreateEntity(IMapRule mapRule, object rootEntity)
        {
            return Activator.CreateInstance(mapRule.EntityType, true);
        }

        public virtual void SetEntity(IMapRule mapRule, IRow row, object rootEntity, object entity)
        {
            row.BindEntity(entity);
        }

        public virtual void SetRow(IMapRule mapRule, object entity, object rootEntity, IRow row)
        {
            row.BindRow(entity);
        }

        public virtual bool GetAutoId(IMapRule mapRule)
        {
            return false;
        }

        public virtual string GetFormatPropertyName(IMapRule mapRule, string column)
        {
            return column;
        }

        public virtual string GetTable(IMapRule mapRule)
        {
            return mapRule.EntityType.Name;
        }

        public virtual void SetRelationId(IMapRule mapRule, RelationId relationId)
        {

        }

        public virtual void SetOptimisticLock(IMapRule mapRule, OptimisticLock optimisticLock)
        {
            
        }

        public virtual string GetIncludePrefix(IMapRule mapRule)
        {
            if (mapRule.Property == null) return null;
            return mapRule.Property.Name + "_";
        }

        public virtual string GetUniqueKeys(IMapRule mapRule)
        {            
            return string.Empty;
        }
    }
}
