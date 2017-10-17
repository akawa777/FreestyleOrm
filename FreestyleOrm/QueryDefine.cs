using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace FreestyleOrm
{
    public interface IQueryDefine
    {
        void SetFormats(Type rootEntityType, Dictionary<string, object> formats);
        string GetTable(Type rootEntityType, IMapRule mapRule);
        void SetRelationId(Type rootEntityType, IMapRule mapRule, RelationId relationId);
        bool GetAutoId(Type rootEntityType, IMapRule mapRule);
        string GetFormatPropertyName(Type rootEntityType, IMapRule mapRule, string column);
        object CreateEntity(Type rootEntityType, IMapRule mapRule);
        void SetOptimisticLock(Type rootEntityType, IMapRule mapRule, OptimisticLock optimisticLock);
        string GetIncludePrefix(Type rootEntityType, IMapRule mapRule);
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

        public virtual object CreateEntity(Type rootEntityType, IMapRule mapRule)
        {
            return Activator.CreateInstance(mapRule.EntityType, true);
        }

        public virtual bool GetAutoId(Type rootEntityType, IMapRule mapRule)
        {
            return false;
        }

        public virtual string GetFormatPropertyName(Type rootEntityType, IMapRule mapRule, string column)
        {
            return column;
        }

        public virtual string GetTable(Type rootEntityType, IMapRule mapRule)
        {
            return mapRule.EntityType.Name;
        }

        public virtual void SetRelationId(Type rootEntityType, IMapRule mapRule, RelationId relationId)
        {

        }

        public virtual void SetOptimisticLock(Type rootEntityType, IMapRule mapRule, OptimisticLock optimisticLock)
        {
            
        }

        public virtual string GetIncludePrefix(Type rootEntityType, IMapRule mapRule)
        {
            if (mapRule.Property == null) return null;
            return mapRule.Property.Name + "_";
        }
    }
}
