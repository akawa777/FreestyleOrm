using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

namespace FreestyleOrm.Core
{
    internal class MapRule : IMapRule
    {
        public MapRule(IQueryDefine queryDefine, Type rootEntityType): this(queryDefine, rootEntityType, rootEntityType, null, null, false)
        {
            
        }

        public MapRule(IQueryDefine queryDefine, Type rootEntityType, Type entityType, string expressionPath, PropertyInfo property, bool isToMany)
        {
            _queryDefine = queryDefine;

            RootEntityType = rootEntityType;
            ExpressionPath = expressionPath ?? string.Empty;
            EntityType = property == null ? rootEntityType : entityType;
            Property = property;
            IsToMany = isToMany;            
            Binder binder = new Binder();
            BindEntity = binder.Bind;
            BindRow = binder.Bind;
            Refer = Refer.Read;

            IEnumerable<MethodInfo> initMethods = typeof(MapRule).GetMethods().Where(x => x.Name.StartsWith("Init"));
            foreach (var methodInfo in initMethods) if (methodInfo.Name != nameof(this.InitRule)) methodInfo.Invoke(this, new object[0]);

            GetEntity = (row, rootEntity) =>
            {
                object entity = CreateEntity(row, rootEntity);
                SetEntity(row, rootEntity, entity);
                return entity;
            };
        }

        private IQueryDefine _queryDefine;
        public bool IsRootOptions => string.IsNullOrEmpty(ExpressionPath);
        public Type RootEntityType { get; set; }        
        public string ExpressionPath { get; set; }
        public Type EntityType { get; set; }
        public PropertyInfo Property { get; set; }
        public string[] ExpressionSections => ExpressionPath.Split('.');
        public bool IsToMany { get; set; }
        public string UniqueKeys { get; set; }
        public void InitUniqueKeys() => UniqueKeys = _queryDefine.GetUniqueKeys(this);
        public string IncludePrefix { get; set; }
        public void InitIncludePrefix() => IncludePrefix = _queryDefine.GetIncludePrefix(this);
        public Refer Refer { get; set; }
        public Func<Row, object, object> GetEntity { get; set; }
        public Func<Row, object, object> CreateEntity { get; set; }
        public void InitCreateEntity() => CreateEntity = (row, rootEntity) => _queryDefine.CreateEntity(this, rootEntity);
        public Action<Row, object, object> SetEntity { get; set; }        
        public void InitSetEntity() => SetEntity = (row, rootEntity, entity) => _queryDefine.SetEntity(this, row, rootEntity, entity);
        public Action<object, object, Row> SetRow { get; set; }
        public void InitSetRow() => SetRow = (entity, rootEntity, row) => _queryDefine.SetRow(this, entity, rootEntity, row);             
        public Action<Row, object> BindEntity { get; set; }
        public Action<object, Row> BindRow { get; set; }
        public Func<string, string> FormatPropertyName { get; set; }
        public void InitFormatPropertyName() => FormatPropertyName = column => _queryDefine.GetFormatPropertyName(this, column);
        public bool AutoId { get; set; }        
        public void InitAutoId() => AutoId = _queryDefine.GetAutoId(this); 
        public string Table { get; set; }
        public string PrimaryKeys { get; set; }
        public void InitTable() => Table = _queryDefine.GetTable(this); 
        public RelationId RelationId { get; set; } = new RelationId();
        public void InitRelationId() => _queryDefine.SetRelationId(this, RelationId);
        public OptimisticLock OptimisticLock { get; set; } = new OptimisticLock();        
        public void InitOptimisticLock() =>  _queryDefine.SetOptimisticLock(this, OptimisticLock);
        public void InitRule(string methodName)
        {
            if (string.IsNullOrEmpty(methodName)) throw new ArgumentException($"{methodName} is required.");

            MethodInfo method = typeof(MapRule).GetMethod($"Init{methodName}", Type.EmptyTypes);

            if (method == null) throw new ArgumentException($"{methodName} is not exists in MapRules.");

            method.Invoke(this, new object[0]);
        }
        public ReNest ReNest {  get; set; } = new ReNest();
    }
}
