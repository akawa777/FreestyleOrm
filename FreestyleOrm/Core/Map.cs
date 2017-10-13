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
        private List<MapOptions> _mapOptionsList = new List<MapOptions>();     

        public MapOptions RootMapOptions
        {
            get
            {
                MapOptions mapOptions = _mapOptionsList.FirstOrDefault(x => string.IsNullOrEmpty(x.ExpressionPath));

                if ((mapOptions == null) && _mapOptionsList.Count() > 0)
                {
                    throw new InvalidOperationException("root map is not setted.");
                }

                if (mapOptions != null)
                {
                    if (_mapOptionsList.Count() > 1 && string.IsNullOrEmpty(mapOptions.UniqueKeys)) throw new InvalidOperationException("root map UniqueKeys is not setted.");
                    return mapOptions;
                }

                mapOptions = new MapOptions(_queryDefine, typeof(TRootEntity));
                mapOptions.Refer = Refer.Write;

                _mapOptionsList.Insert(0, mapOptions);

                return _mapOptionsList.FirstOrDefault(x => string.IsNullOrEmpty(x.ExpressionPath));
            }
        }

        public IEnumerable<MapOptions> MapOptionsListWithoutRoot
        {
            get
            {
                int prevLevel = 1;
                string prevPrefixPath = string.Empty;
                List<string> paths = new List<string>();

                foreach (var mapOptions in _mapOptionsList.Where(x => x != RootMapOptions))
                {
                    bool valid = false;

                    int level = mapOptions.ExpressionSections.Length;

                    string prefixPath;
                    if (level == 1)
                    {
                        prefixPath = string.Join(".", mapOptions.ExpressionSections);
                    }
                    else
                    {
                        prefixPath = string.Join(".", mapOptions.ExpressionSections.Take(level - 1));
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

                    paths.Add(mapOptions.ExpressionPath);

                    if (!valid)
                    {
                        throw new InvalidOperationException($"map order is invalid. [{string.Join(", ", paths)}]");
                    }                    

                    prevLevel = level;
                    prevPrefixPath = prefixPath;

                    if (string.IsNullOrEmpty(mapOptions.UniqueKeys)) throw new InvalidOperationException($"[{mapOptions.ExpressionPath}] map UniqueKeys is not setted.");                    

                    yield return mapOptions;
                }
            }
        }
        

        public IMapOptions<TRootEntity, TRootEntity> To()
        {
            MapOptions<TRootEntity, TRootEntity> mapOptions = new MapOptions<TRootEntity, TRootEntity>(_queryDefine, x => x);

            _mapOptionsList.Add(mapOptions.GetMapOptions());

            return mapOptions;
        }

        public IMapOptions<TRootEntity, TEntity> ToMany<TEntity>(Expression<Func<TRootEntity, IEnumerable<TEntity>>> target) where TEntity : class
        {
            if (target == null) throw new ArgumentException($"[Expression<Func<{typeof(TRootEntity).Name}, IEnumerable<{typeof(TRootEntity).Name}>>>] {nameof(target)} is null.");

            MapOptions<TRootEntity, TEntity> mapOptions = new MapOptions<TRootEntity, TEntity>(_queryDefine, target);

            _mapOptionsList.Add(mapOptions.GetMapOptions());

            return mapOptions;
        }

        public IMapOptions<TRootEntity, TEntity> ToOne<TEntity>(Expression<Func<TRootEntity, TEntity>> target) where TEntity : class
        {
            if (target == null) throw new ArgumentException($"[Expression<Func<{typeof(TRootEntity).Name}, {typeof(TRootEntity).Name}>>>] {nameof(target)} is null.");

            MapOptions<TRootEntity, TEntity> mapOptions = new MapOptions<TRootEntity, TEntity>(_queryDefine, target);

            _mapOptionsList.Add(mapOptions.GetMapOptions());

            return mapOptions;
        }
    }

    internal class MapOptions<TRootEntity, TEntity> : IMapOptions<TRootEntity, TEntity> where TRootEntity : class where TEntity : class
    {
        public MapOptions(IQueryDefine queryDefine, Expression<Func<TRootEntity, TEntity>> target)
        {   
            _mapOptions = new MapOptions(queryDefine, typeof(TRootEntity), typeof(TEntity), target.GetExpressionPath(out PropertyInfo property), property, false);
        }

        public MapOptions(IQueryDefine queryDefine, Expression<Func<TRootEntity, IEnumerable<TEntity>>> target)
        {
            _mapOptions = new MapOptions(queryDefine, typeof(TRootEntity), typeof(TEntity), target.GetExpressionPath(out PropertyInfo property), property, true);
        }

        private MapOptions _mapOptions;

        public MapOptions GetMapOptions() => _mapOptions;

        public IMapOptions<TRootEntity, TEntity> AutoId(bool autoId)
        {
            _mapOptions.AutoId = autoId;

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> GetEntity(Func<IRow, TRootEntity, TEntity> getEntity)
        {
            if (getEntity == null) throw new ArgumentException($"[IMapOptions<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(getEntity)} is null.");

            _mapOptions.GetEntity = (row, rootEntity) => getEntity(row, rootEntity as TRootEntity);

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> FormatPropertyName(Func<string, string> formatPropertyName)
        {
            if (formatPropertyName == null) throw new ArgumentException($"[IMapOptions<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(formatPropertyName)} is null.");

            _mapOptions.FormatPropertyName = formatPropertyName;

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> IncludePrefix(string prefix)
        {
            _mapOptions.IncludePrefix = prefix;

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> Refer(Refer refer)
        {
            _mapOptions.Refer = refer;

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> SetRow(Action<TEntity, TRootEntity, IRow> setRow)
        {
            if (setRow == null) throw new ArgumentException($"[IMapOptions<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(setRow)} is null.");

            _mapOptions.SetRow = (entity, rootEntity, row) => setRow(entity as TEntity, rootEntity as TRootEntity, row);

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> Table(string table)
        {
            if (string.IsNullOrEmpty(table)) throw new ArgumentException($"[IMapOptions<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(table)} is null or empty.");

            _mapOptions.Table = table;

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> RelationId<TRelationEntity>(string relationIdColumn, Expression<Func<TRootEntity, TRelationEntity>> relationEntity) where TRelationEntity : class
        {
            if (_mapOptions.IsRootOptions) throw new InvalidOperationException($"[IMapOptions<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] root entity can not set {nameof(relationIdColumn)}.");
            if (string.IsNullOrEmpty(relationIdColumn)) throw new ArgumentException($"[IMapOptions<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(relationIdColumn)} is null or empty.");
            if (relationEntity == null) throw new ArgumentException($"[IMapOptions<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(relationEntity)} is null.");            

            _mapOptions.RelationIdColumn = relationIdColumn;
            _mapOptions.RelationEntityPath = relationEntity.GetExpressionPath();

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> UniqueKeys(string columns)
        {
            if (string.IsNullOrEmpty(columns)) throw new ArgumentException($"[IMapOptions<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(columns)} is null or empty.");

            _mapOptions.UniqueKeys = columns;

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> OptimisticLock<TRowVersion>(string rowVersionColumn, Func<TEntity, TRowVersion> newRowVersion = null)
        {
            if (string.IsNullOrEmpty(rowVersionColumn)) throw new ArgumentException($"[IMapOptions<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(rowVersionColumn)} is null or empty.");
            if (newRowVersion == null) throw new ArgumentException($"[IMapOptions<{typeof(TRootEntity).Name}, {typeof(TEntity).Name}>] {nameof(newRowVersion)} is null.");

            _mapOptions.RowVersionColumn = rowVersionColumn;
            if (newRowVersion != null) _mapOptions.NewRowVersion = entity => newRowVersion(entity as TEntity);

            return this;
        }

    }
}
