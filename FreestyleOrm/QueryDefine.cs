using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Linq;
using System.Collections.Specialized;
using System.Data;

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
        void SetOptimisticLock(IMapRule mapRule, IOptimisticLock optimisticLock);
        string GetIncludePrefix(IMapRule mapRule);        
        string GetUniqueKeys(IMapRule mapRule);        
        void EndModifingRecord(ModifingEntry modifingEntry);
    }    

    public class ModifingEntry
    {
        internal ModifingEntry(IDbConnection connection, IDbTransaction transaction, string tableName, OrderedDictionary primaryValues, string commandName, OrderedDictionary beforeRecords, OrderedDictionary afterRecords)
        {
            Connection = connection;
            Transaction = transaction;            
            TableName = tableName;
            PrimaryValues = primaryValues;
            CommandName = commandName;
            BeforeRecoreds = beforeRecords;
            AfterRecoreds = afterRecords;
        }

        public IDbConnection Connection { get; }
        public IDbTransaction Transaction { get; }        
        public string TableName { get; }
        public OrderedDictionary PrimaryValues { get; }
        public string CommandName { get; }
        public OrderedDictionary BeforeRecoreds { get; }
        public OrderedDictionary AfterRecoreds { get; }
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

        public virtual void SetOptimisticLock(IMapRule mapRule, IOptimisticLock optimisticLock)
        {
            
        }

        public virtual string GetIncludePrefix(IMapRule mapRule)
        {
            return null;            
        }

        public virtual string GetUniqueKeys(IMapRule mapRule)
        {            
            return string.Empty;
        }

        public virtual void EndModifingRecord(ModifingEntry modifingEntry)
        {
            
        }
    }
}
