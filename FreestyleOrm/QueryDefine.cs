using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace FreestyleOrm
{
    public interface IQueryDefine
    {
        void SetFormats(Type rootEntityType, Dictionary<string, object> formats);
        string GetTable(Type rootEntityType, Type entityType);
        void SetRelationId(Type rootEntityType, Type entityType, RelationId relationId);
        bool GetAutoId(Type rootEntityType, Type entityType);
        string GetFormatPropertyName(Type rootEntityType, Type entityTypee, string column);
        object CreateEntity(Type rootEntityType, Type entityType);
        void SetOptimisticLock(Type rootEntityType, Type entityType, OptimisticLock optimisticLock);
        string GetIncludePrefix(Type rootEntityType, Type entityType, PropertyInfo property);
    }

    public class OptimisticLock
    {
        public string RowVersionColumn { get; set; }
        public Func<object, object> NewRowVersion { get; set; }
    }

    public class RelationId
    {        
        public string RelationIdColumn { get; set; }
        public string RelationEntityPath { get; set; }
    }

    public class QueryDefine : IQueryDefine
    {

        public virtual void SetFormats(Type rootEntityType, Dictionary<string, object> formats)
        {

        }

        public virtual object CreateEntity(Type rootEntityType, Type entityType)
        {
            return Activator.CreateInstance(entityType, true);
        }

        public virtual bool GetAutoId(Type rootEntityType, Type entityType)
        {
            return false;
        }

        public virtual string GetFormatPropertyName(Type rootEntityType, Type entityType, string column)
        {
            return column;
        }

        public virtual string GetTable(Type rootEntityType, Type entityType)
        {
            return entityType.Name;
        }

        public virtual void SetRelationId(Type rootEntityType, Type entityType, RelationId relationId)
        {

        }

        public void SetOptimisticLock(Type rootEntityType, Type entityType, OptimisticLock optimisticLock)
        {
            
        }

        public string GetIncludePrefix(Type rootEntityType, Type entityType, PropertyInfo property)
        {
            if (property == null) return null;
            return property.Name + "_";
        }
    }
}
