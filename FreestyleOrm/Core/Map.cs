using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Linq;

namespace FreestyleOrm.Core
{

    internal class Map<TRootEntity> : IMap<TRootEntity> where TRootEntity : class
    {
        public Map(IQueryDefine queryDefine)
        {
            _queryDefine = queryDefine;
        }

        private IQueryDefine _queryDefine;
        private List<MapRule> _mapRuleList = new List<MapRule>();     

        public MapRule RootMapRule
        {
            get
            {
                MapRule mapRule = _mapRuleList.FirstOrDefault(x => string.IsNullOrEmpty(x.ExpressionPath));

                if ((mapRule == null) && _mapRuleList.Count() > 0)
                {
                    throw new InvalidOperationException("root map is not setted.");
                }

                if (mapRule != null)
                {
                    if (_mapRuleList.Count() > 1 && string.IsNullOrEmpty(mapRule.UniqueKeys)) throw new InvalidOperationException("root map UniqueKeys is not setted.");
                    return mapRule;
                }

                mapRule = new MapRule(_queryDefine, typeof(TRootEntity));
                mapRule.Refer = Refer.Write;

                _mapRuleList.Insert(0, mapRule);

                return _mapRuleList.FirstOrDefault(x => string.IsNullOrEmpty(x.ExpressionPath));
            }
        }

        public IEnumerable<MapRule> MapRuleListWithoutRoot
        {
            get
            {
                int prevLevel = 1;
                string prevPrefixPath = string.Empty;
                List<string> paths = new List<string>();

                foreach (var mapRule in _mapRuleList.Where(x => x != RootMapRule))
                {
                    bool valid = false;

                    int level = mapRule.ExpressionSections.Length;

                    string prefixPath;
                    if (level == 1)
                    {
                        prefixPath = string.Join(".", mapRule.ExpressionSections);
                    }
                    else
                    {
                        prefixPath = string.Join(".", mapRule.ExpressionSections.Take(level - 1));
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

                    paths.Add(mapRule.ExpressionPath);

                    if (!valid)
                    {
                        throw new InvalidOperationException($"map order is invalid. [{string.Join(", ", paths)}]");
                    }                    

                    prevLevel = level;
                    prevPrefixPath = prefixPath;

                    if (string.IsNullOrEmpty(mapRule.UniqueKeys)) throw new InvalidOperationException($"[{mapRule.ExpressionPath}] map UniqueKeys is not setted.");                    

                    yield return mapRule;
                }
            }
        }
        

        public IMapRule<TRootEntity, TRootEntity> To()
        {
            MapRule<TRootEntity, TRootEntity> mapRule = new MapRule<TRootEntity, TRootEntity>(_queryDefine, x => x);

            _mapRuleList.Add(mapRule.GetMapRule());

            return mapRule;
        }

        public IMapRule<TRootEntity, TEntity> ToMany<TEntity>(Expression<Func<TRootEntity, IEnumerable<TEntity>>> target) where TEntity : class
        {
            if (target == null) throw new ArgumentException($"[Expression<Func<{typeof(TRootEntity).Name}, IEnumerable<{typeof(TRootEntity).Name}>>>] {nameof(target)} is null.");

            MapRule<TRootEntity, TEntity> mapRule = new MapRule<TRootEntity, TEntity>(_queryDefine, target);

            _mapRuleList.Add(mapRule.GetMapRule());

            return mapRule;
        }

        public IMapRule<TRootEntity, TEntity> ToOne<TEntity>(Expression<Func<TRootEntity, TEntity>> target) where TEntity : class
        {
            if (target == null) throw new ArgumentException($"[Expression<Func<{typeof(TRootEntity).Name}, {typeof(TRootEntity).Name}>>>] {nameof(target)} is null.");

            MapRule<TRootEntity, TEntity> mapRule = new MapRule<TRootEntity, TEntity>(_queryDefine, target);

            _mapRuleList.Add(mapRule.GetMapRule());

            return mapRule;
        }
    }

    internal class MapRule<TRootEntity, TEntity> : IMapRule<TRootEntity, TEntity> where TRootEntity : class where TEntity : class
    {
        public MapRule(IQueryDefine queryDefine, Expression<Func<TRootEntity, TEntity>> target)
        {   
            _mapRule = new MapRule(queryDefine, typeof(TRootEntity), typeof(TEntity), target.GetExpressionPath(out PropertyInfo property), property, false);
        }

        public MapRule(IQueryDefine queryDefine, Expression<Func<TRootEntity, IEnumerable<TEntity>>> target)
        {
            _mapRule = new MapRule(queryDefine, typeof(TRootEntity), typeof(TEntity), target.GetExpressionPath(out PropertyInfo property), property, true);
        }

        private MapRule _mapRule;

        public MapRule GetMapRule() => _mapRule;

        public IMapRule<TRootEntity, TEntity> AutoId(bool autoId)
        {
            _mapRule.AutoId = autoId;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> GetEntity(Func<IRow, TRootEntity, TEntity> getEntity)
        {
            if (getEntity == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(getEntity)} is null.");

            _mapRule.GetEntity = (row, rootEntity) => getEntity(row, rootEntity as TRootEntity);

            return this;
        }

        public IMapRule<TRootEntity, TEntity> FormatPropertyName(Func<string, string> formatPropertyName)
        {
            if (formatPropertyName == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(formatPropertyName)} is null.");

            _mapRule.FormatPropertyName = formatPropertyName;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> IncludePrefix(string prefix)
        {
            _mapRule.IncludePrefix = prefix;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> Refer(Refer refer)
        {
            _mapRule.Refer = refer;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> SetRow(Action<TEntity, TRootEntity, IRow> setRow)
        {
            if (setRow == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(setRow)} is null.");

            _mapRule.SetRow = (entity, rootEntity, row) => setRow(entity as TEntity, rootEntity as TRootEntity, row);

            return this;
        }

        public IMapRule<TRootEntity, TEntity> Table(string table)
        {
            if (string.IsNullOrEmpty(table)) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(table)} is null or empty.");

            _mapRule.Table = table;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> RelationId<TRelationEntity>(string relationIdColumn, Expression<Func<TRootEntity, TRelationEntity>> relationEntity) where TRelationEntity : class
        {
            if (_mapRule.IsRootOptions) throw new InvalidOperationException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] root entity can not set {nameof(relationIdColumn)}.");
            if (string.IsNullOrEmpty(relationIdColumn)) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(relationIdColumn)} is null or empty.");
            if (relationEntity == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(relationEntity)} is null.");            

            _mapRule.RelationIdColumn = relationIdColumn;
            _mapRule.RelationEntityPath = relationEntity.GetExpressionPath();

            return this;
        }

        public IMapRule<TRootEntity, TEntity> UniqueKeys(string columns)
        {
            if (string.IsNullOrEmpty(columns)) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(columns)} is null or empty.");

            _mapRule.UniqueKeys = columns;

            return this;
        }

        public IMapRule<TRootEntity, TEntity> OptimisticLock<TRowVersion>(string rowVersionColumn, Func<TEntity, TRowVersion> newRowVersion = null)
        {
            if (string.IsNullOrEmpty(rowVersionColumn)) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(rowVersionColumn)} is null or empty.");
            if (newRowVersion == null) throw new ArgumentException($"[IMapRule<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(newRowVersion)} is null.");

            _mapRule.RowVersionColumn = rowVersionColumn;
            if (newRowVersion != null) _mapRule.NewRowVersion = entity => newRowVersion(entity as TEntity);

            return this;
        }

    }
}
