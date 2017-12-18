using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Linq;

namespace FreestyleOrm
{
    public interface IQueryDefine
    {
        void SetFormats(Type rootEntityType, Dictionary<string, object> formats);
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

    public class OptimisticLock
    {
        public string Columns { get; set; }
        public string[] GetColumns() => Columns.Split(',').Select(x => x.Trim()).ToArray();
        public Func<object, object[]> GetNewToken{ get; set; }
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
