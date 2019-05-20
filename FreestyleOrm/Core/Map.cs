using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Linq;
using System.Collections.Specialized;

namespace FreestyleOrm.Core
{

    internal class Map<TRootEntity> : IMap<TRootEntity>, IMap where TRootEntity : class
    {
        public Map(QueryOptions queryOptions)
        {
            _queryOptions = queryOptions;
        }

        private QueryOptions _queryOptions;
        private List<MapRuleBasic> _mapRuleBasicList = new List<MapRuleBasic>();     

        public MapRuleBasic RootMapRuleBasic
        {
            get
            {
                MapRuleBasic mapRuleBasic = _mapRuleBasicList.FirstOrDefault(x => string.IsNullOrEmpty(x.ExpressionPath));

                if ((mapRuleBasic == null) && _mapRuleBasicList.Count() > 0)
                {
                    throw new InvalidOperationException("root map is not setted.");
                }

                if (mapRuleBasic != null)
                {
                    if (_mapRuleBasicList.Count() > 1 && string.IsNullOrEmpty(mapRuleBasic.UniqueKeys)) throw new InvalidOperationException("root map UniqueKeys is not setted.");
                    return mapRuleBasic;
                }

                mapRuleBasic = new MapRuleBasic(_queryOptions, typeof(TRootEntity));

                _mapRuleBasicList.Insert(0, mapRuleBasic);

                return RootMapRuleBasic;
            }
        }

        public IEnumerable<MapRuleBasic> MapRuleBasicListWithoutRoot
        {
            get
            {
                int prevLevel = 1;
                string prevPrefixPath = string.Empty;
                List<string> paths = new List<string>();

                foreach (var mapRuleBasic in _mapRuleBasicList.Where(x => x != RootMapRuleBasic))
                {
                    bool valid = false;

                    int level = mapRuleBasic.ExpressionSections.Length;

                    string prefixPath;
                    if (level == 1)
                    {
                        prefixPath = string.Join(".", mapRuleBasic.ExpressionSections);
                    }
                    else
                    {
                        prefixPath = string.Join(".", mapRuleBasic.ExpressionSections.Take(level - 1));
                    }

                    if (level > prevLevel)
                    {
                        if (prevPrefixPath == prefixPath)
                        {
                            prevLevel = level;
                            valid = true;
                        }
                    }
                    else
                    {
                        valid = true;
                    }

                    paths.Add(mapRuleBasic.ExpressionPath);

                    if (!valid)
                    {
                        throw new InvalidOperationException($"map order is invalid. [{string.Join(", ", paths)}]");
                    }

                    prevLevel = level;
                    prevPrefixPath = prefixPath;

                    if (string.IsNullOrEmpty(mapRuleBasic.UniqueKeys)) throw new InvalidOperationException($"[{mapRuleBasic.ExpressionPath}] map UniqueKeys is not setted.");                    

                    yield return mapRuleBasic;
                }
            }
        }

        private MapRule<TRootEntity, TRootEntity> CreateMapRule()
        {
            MapRule<TRootEntity, TRootEntity> mapRule = new MapRule<TRootEntity, TRootEntity>(_queryOptions, x => x);

            _mapRuleBasicList.Add(mapRule.GetMapRuleBasic());

            return mapRule;
        }
        

        public IMapRule<TRootEntity, TRootEntity> ToRoot()
        {
            return CreateMapRule();
        }

        public IMapRule<TRootEntity, TEntity> ToMany<TEntity>(Expression<Func<TRootEntity, IEnumerable<TEntity>>> target) where TEntity : class
        {
            if (target == null) throw new ArgumentException($"[Expression<Func<{typeof(TRootEntity).Name}, IEnumerable<{typeof(TRootEntity).Name}>>>] {nameof(target)} is null.");

            MapRule<TRootEntity, TEntity> mapRule = new MapRule<TRootEntity, TEntity>(_queryOptions, target);

            _mapRuleBasicList.Add(mapRule.GetMapRuleBasic());

            return mapRule;
        }

        public IMapRule<TRootEntity, TEntity> ToOne<TEntity>(Expression<Func<TRootEntity, TEntity>> target) where TEntity : class
        {
            if (target == null) throw new ArgumentException($"[Expression<Func<{typeof(TRootEntity).Name}, {typeof(TRootEntity).Name}>>>] {nameof(target)} is null.");

            MapRule<TRootEntity, TEntity> mapRule = new MapRule<TRootEntity, TEntity>(_queryOptions, target);

            _mapRuleBasicList.Add(mapRule.GetMapRuleBasic());

            return mapRule;
        }

        IMapRule IMap.ToRoot()
        {
            return CreateMapRule();
        }
    }

    internal class MapRule<TRootEntity, TEntity> : IMapRule<TRootEntity, TEntity>, IMapRule where TRootEntity : class where TEntity : class
    {
        public MapRule(QueryOptions queryOptions, Expression<Func<TRootEntity, TEntity>> target)
        {
            _mapRuleBasic = new MapRuleBasic(queryOptions, typeof(TRootEntity), typeof(TEntity), target.GetExpressionPath(out PropertyInfo property).First(), property, false);
        }

        public MapRule(QueryOptions queryOptions, Expression<Func<TRootEntity, IEnumerable<TEntity>>> target)
        {
            _mapRuleBasic = new MapRuleBasic(queryOptions, typeof(TRootEntity), typeof(TEntity), target.GetExpressionPath(out PropertyInfo property).First(), property, true);
        }

        private MapRuleBasic _mapRuleBasic;

        public MapRuleBasic GetMapRuleBasic() => _mapRuleBasic;

        public IMapRule<TRootEntity, TEntity> AutoId()
        {
            _mapRuleBasic.AutoId = true;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> CreateEntity(Func<IRow, TRootEntity, TEntity> createEntity)
        {
            if (createEntity == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(createEntity)} is null.");

            _mapRuleBasic.CreateEntity = (row, rootEntity) => createEntity(row, rootEntity as TRootEntity);

            return this;
        }

        public IMapRule<TRootEntity, TEntity> SetEntity(Action<IRow, TRootEntity, TEntity> setEntity)
        {
            if (setEntity == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(setEntity)} is null.");

            _mapRuleBasic.SetEntity = (row, rootEntity, entity) => setEntity(row, rootEntity as TRootEntity, entity as TEntity);

            return this;
        }

        public IMapRule<TRootEntity, TEntity> SetRow(Action<TEntity, TRootEntity, IRow> setRow)
        {
            if (setRow == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(setRow)} is null.");

            _mapRuleBasic.SetRow = (entity, rootEntity, row) => setRow(entity as TEntity, rootEntity as TRootEntity, row);

            return this;
        }

        public IMapRule<TRootEntity, TEntity> FormatPropertyName(Func<string, string> formatPropertyName)
        {
            if (formatPropertyName == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(formatPropertyName)} is null.");

            _mapRuleBasic.FormatPropertyName = formatPropertyName;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> IncludePrefix(string prefix)
        {
            _mapRuleBasic.IncludePrefix = prefix;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> Writable()
        {
            _mapRuleBasic.Refer = Refer.Write;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> Table(string table, string primaryKeys = null)
        {
            if (string.IsNullOrEmpty(table)) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(table)} is null or empty.");

            _mapRuleBasic.Table = table;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> RelationId<TRelationEntity>(string relationIdColumn, Expression<Func<TRootEntity, TRelationEntity>> relationEntity) where TRelationEntity : class
        {
            if (_mapRuleBasic.IsRootOptions) throw new InvalidOperationException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] root entity can not set {nameof(relationIdColumn)}.");
            if (string.IsNullOrEmpty(relationIdColumn)) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(relationIdColumn)} is null or empty.");
            if (relationEntity == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(relationEntity)} is null.");

            _mapRuleBasic.RelationId.RelationIdColumn = relationIdColumn;
            _mapRuleBasic.RelationId.RelationEntityPath = relationEntity.GetExpressionPath().First();

            return this;
        }

        public IMapRule<TRootEntity, TEntity> UniqueKeys(string columns)
        {
            if (string.IsNullOrEmpty(columns)) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(columns)} is null or empty.");

            _mapRuleBasic.UniqueKeys = columns;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> OptimisticLock(Action<IOptimisticLock<TEntity>> setOptimisticLock)        
        {
            if (setOptimisticLock == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(setOptimisticLock)} is null.");

            OptimisticLock<TEntity> optimisticLock = new OptimisticLock<TEntity>();
            setOptimisticLock(optimisticLock);

            _mapRuleBasic.OptimisticLock = optimisticLock;

            return this;
        }  

        public IMapRule<TRootEntity, TEntity> ReNest<TProperty, TId, TParentId>(Expression<Func<TEntity, IEnumerable<TProperty>>> nestEntity, Expression<Func<TEntity, TId>> idPropertiy, Expression<Func<TEntity, TParentId>> parentProperty) where  TProperty : TEntity
        {
            if (!_mapRuleBasic.IsRootOptions && !_mapRuleBasic.IsToMany) throw new InvalidOperationException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(ReNest)} is valid only for ToMany.");    
            if (nestEntity == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(nestEntity)} is null.");    
            if (idPropertiy == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(idPropertiy)} is null.");        
            if (parentProperty == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(parentProperty)} is null.");

            _mapRuleBasic.ReNest.NestEntityPath = nestEntity.GetExpressionPath().First();
            _mapRuleBasic.ReNest.IdProperties = idPropertiy.GetExpressionPath();
            _mapRuleBasic.ReNest.ParentProperties = parentProperty.GetExpressionPath();

            return this;
        }    

        public IMapRule<TRootEntity, TEntity> ClearRule(Func<IMapRule<TRootEntity, TEntity>, string> methodName)
        {
            string name = methodName(this);
            _mapRuleBasic.InitRule(name);

            return this;
        }

        IMapRule IMapRule.Writable()
        {
            Writable();

            return this;
        }


        IMapRule IMapRule.SetEntity(Action<IRow, OrderedDictionary> setEntity)
        {
            Action<IRow, TRootEntity, TEntity> setEntityAction = (row, root, entity) =>
            {
                setEntity(row, entity as OrderedDictionary);
            };

            SetEntity(setEntityAction);

            return this;
        }

        IMapRule IMapRule.SetRow(Action<OrderedDictionary, IRow> setRow)
        {
            throw new NotImplementedException();
        }

        IMapRule IMapRule.Table(string table, string primaryKeys)
        {
            Table(table, primaryKeys);

            return this;
        }

        IMapRule IMapRule.AutoId()
        {
            AutoId();

            return this;
        }

        IMapRule IMapRule.OptimisticLock(Action<IOptimisticLock<OrderedDictionary>> setOptimisticLock)
        {
            OptimisticLock(setOptimisticLock as Action<IOptimisticLock<TEntity>>);

            return this;
        }

        IMapRule IMapRule.ClearRule(Func<IMapRule<OrderedDictionary, OrderedDictionary>, string> methodName)
        {
            ClearRule(methodName as Func<IMapRule<TRootEntity, TEntity>, string>);

            return this;
        }
    }
}
