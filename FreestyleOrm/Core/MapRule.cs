using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

namespace FreestyleOrm.Core
{
    internal class MapRule : IMapRule
    {
        public MapRule(QueryOptions queryOptions, Type rootEntityType): this(queryOptions, rootEntityType, rootEntityType, null, null, false)
        {
            
        }

        public MapRule(QueryOptions queryOptions, Type rootEntityType, Type entityType, string expressionPath, PropertyInfo property, bool isToMany)
        {
            _queryOptions = queryOptions;

            RootEntityType = rootEntityType;
            ExpressionPath = expressionPath ?? string.Empty;
            EntityType = property == null ? rootEntityType : entityType;
            Property = property;
            IsToMany = isToMany;
            Binder binder = new Binder(queryOptions.IsFlatFormat);
            BindEntity = binder.Bind;
            BindRow = binder.Bind;
            Refer = Refer.Read;

            InvokeInitMethods();

            GetEntity = (row, rootEntity) =>
            {
                object entity = CreateEntity(row, rootEntity);
                if (rootEntity == null) SetEntity(row, entity, entity);
                else SetEntity(row, rootEntity, entity);

                return entity;
            };
        }

        public void InvokeInitMethods()
        {
            InitUniqueKeys();
            InitIncludePrefix();
            InitCreateEntity();
            InitSetEntity();
            InitSetRow();
            InitFormatPropertyName();
            InitAutoId();
            InitTable();
            InitRelationId();
            InitOptimisticLock();
        }

        private QueryOptions _queryOptions;
        public bool IsRootOptions => string.IsNullOrEmpty(ExpressionPath);
        public Type RootEntityType { get; set; }        
        public string ExpressionPath { get; set; }
        public Type EntityType { get; set; }
        public PropertyInfo Property { get; set; }
        public string[] ExpressionSections => ExpressionPath.Split('.');
        public bool IsToMany { get; set; }
        public string UniqueKeys { get; set; }
        public void InitUniqueKeys() => UniqueKeys = _queryOptions.QueryDefine.GetUniqueKeys(this);
        public string IncludePrefix { get; set; }
        public void InitIncludePrefix() => IncludePrefix = _queryOptions.QueryDefine.GetIncludePrefix(this);
        public Refer Refer { get; set; }
        public Func<Row, object, object> GetEntity { get; set; }
        public Func<Row, object, object> CreateEntity { get; set; }
        public void InitCreateEntity() => CreateEntity = (row, rootEntity) => _queryOptions.QueryDefine.CreateEntity(this, rootEntity);
        public Action<Row, object, object> SetEntity { get; set; }        
        public void InitSetEntity() => SetEntity = (row, rootEntity, entity) => _queryOptions.QueryDefine.SetEntity(this, row, rootEntity, entity);
        public Action<object, object, Row> SetRow { get; set; }
        public void InitSetRow() => SetRow = (entity, rootEntity, row) => _queryOptions.QueryDefine.SetRow(this, entity, rootEntity, row);             
        public Action<Row, object> BindEntity { get; set; }
        public Action<object, Row> BindRow { get; set; }
        public Func<string, string> FormatPropertyName { get; set; }
        public void InitFormatPropertyName() => FormatPropertyName = column => _queryOptions.QueryDefine.GetFormatPropertyName(this, column);
        public bool AutoId { get; set; }        
        public void InitAutoId() => AutoId = _queryOptions.QueryDefine.GetAutoId(this); 
        public string Table { get; set; }
        public string PrimaryKeys { get; set; }
        public void InitTable() => Table = _queryOptions.QueryDefine.GetTable(this); 
        public RelationId RelationId { get; set; } = new RelationId();
        public void InitRelationId() => _queryOptions.QueryDefine.SetRelationId(this, RelationId);
        public OptimisticLock OptimisticLock { get; set; } = new OptimisticLock();        
        public void InitOptimisticLock() => _queryOptions.QueryDefine.SetOptimisticLock(this, OptimisticLock);
        public void InitRule(string methodName)
        {
            if (string.IsNullOrEmpty(methodName)) throw new ArgumentException($"{methodName} is required.");

            MethodInfo method = typeof(MapRule).GetMethod($"Init{methodName}", Type.EmptyTypes);

            if (method == null) throw new ArgumentException($"{methodName} is not exists in MapRules.");

            method.Invoke(this, new object[0]);
        }
        public ReNest ReNest {  get; set; } = new ReNest();
        public bool IsFlatFormat => _queryOptions.IsFlatFormat;
    }
}
