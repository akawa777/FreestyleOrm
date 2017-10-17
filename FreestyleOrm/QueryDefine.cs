using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace FreestyleOrm
{
    public interface IQueryDefine
    {
        void SetFormats(Type rootEntityType, Dictionary<string, object> formats);
        string GetTable(Type rootEntityType, IMapOptions mapOptions);
        void SetRelationId(Type rootEntityType, IMapOptions mapOptions, RelationId relationId);
        bool GetAutoId(Type rootEntityType, IMapOptions mapOptions);
        string GetFormatPropertyName(Type rootEntityType, IMapOptions mapOptions, string column);
        object CreateEntity(Type rootEntityType, IMapOptions mapOptionse);
        void SetOptimisticLock(Type rootEntityType, IMapOptions mapOptions, OptimisticLock optimisticLock);
        string GetIncludePrefix(Type rootEntityType, IMapOptions mapOptions);
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

        public virtual object CreateEntity(Type rootEntityType, IMapOptions mapOptions)
        {
            return Activator.CreateInstance(mapOptions.EntityType, true);
        }

        public virtual bool GetAutoId(Type rootEntityType, IMapOptions mapOptions)
        {
            return false;
        }

        public virtual string GetFormatPropertyName(Type rootEntityType, IMapOptions mapOptions, string column)
        {
            return column;
        }

        public virtual string GetTable(Type rootEntityType, IMapOptions mapOptions)
        {
            return mapOptions.EntityType.Name;
        }

        public virtual void SetRelationId(Type rootEntityType, IMapOptions mapOptions, RelationId relationId)
        {

        }

        public virtual void SetOptimisticLock(Type rootEntityType, IMapOptions mapOptions, OptimisticLock optimisticLock)
        {
            
        }

        public virtual string GetIncludePrefix(Type rootEntityType, IMapOptions mapOptions)
        {
            if (mapOptions.Property == null) return null;
            return mapOptions.Property.Name + "_";
        }
    }
}
