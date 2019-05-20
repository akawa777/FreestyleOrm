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
        string GetTable(IMapRuleBasic mapRuleBasic);
        void SetRelationId(IMapRuleBasic mapRuleBasic, RelationId relationId);
        bool GetAutoId(IMapRuleBasic mapRuleBasic);
        string GetFormatPropertyName(IMapRuleBasic mapRuleBasic, string column);
        object CreateEntity(IMapRuleBasic mapRuleBasic, object rootEntity);
        void SetEntity(IMapRuleBasic mapRuleBasic, IRow row, object rootentiy, object entity);
        void SetRow(IMapRuleBasic mapRuleBasic, object entity, object rootentiy, IRow row);
        void SetOptimisticLock(IMapRuleBasic mapRuleBasic, IOptimisticLock optimisticLock);
        string GetIncludePrefix(IMapRuleBasic mapRuleBasic);        
        string GetUniqueKeys(IMapRuleBasic mapRuleBasic);        
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

        public virtual object CreateEntity(IMapRuleBasic mapRuleBasic, object rootEntity)
        {
            return Activator.CreateInstance(mapRuleBasic.EntityType, true);
        }

        public virtual void SetEntity(IMapRuleBasic mapRuleBasic, IRow row, object rootEntity, object entity)
        {
            row.BindEntity(entity);
        }

        public virtual void SetRow(IMapRuleBasic mapRuleBasic, object entity, object rootEntity, IRow row)
        {
            row.BindRow(entity);
        }

        public virtual bool GetAutoId(IMapRuleBasic mapRuleBasic)
        {
            return false;
        }

        public virtual string GetFormatPropertyName(IMapRuleBasic mapRuleBasic, string column)
        {
            return column;
        }

        public virtual string GetTable(IMapRuleBasic mapRuleBasic)
        {
            return mapRuleBasic.EntityType.Name;
        }

        public virtual void SetRelationId(IMapRuleBasic mapRuleBasic, RelationId relationId)
        {

        }

        public virtual void SetOptimisticLock(IMapRuleBasic mapRuleBasic, IOptimisticLock optimisticLock)
        {
            
        }

        public virtual string GetIncludePrefix(IMapRuleBasic mapRuleBasic)
        {
            return null;            
        }

        public virtual string GetUniqueKeys(IMapRuleBasic mapRuleBasic)
        {            
            return string.Empty;
        }

        public virtual void EndModifingRecord(ModifingEntry modifingEntry)
        {
            
        }
    }
}
