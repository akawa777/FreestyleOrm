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

                if (mapOptions != null) return mapOptions;

                mapOptions = new MapOptions(_queryDefine, typeof(TRootEntity));

                _mapOptionsList.Insert(0, mapOptions);

                return _mapOptionsList.FirstOrDefault(x => string.IsNullOrEmpty(x.ExpressionPath));
            }
        }

        public IEnumerable<MapOptions> MapOptionsListWithoutRoot => _mapOptionsList.Where(x => x != RootMapOptions).OrderBy(x => x.ExpressionSections.Length);

        public IMapOptions<TRootEntity, TRootEntity> To()
        {
            MapOptions<TRootEntity, TRootEntity> mapOptions = new MapOptions<TRootEntity, TRootEntity>(_queryDefine, x => x);

            _mapOptionsList.Add(mapOptions.GetMapOptions());

            return mapOptions;
        }

        public IMapOptions<TRootEntity, TEntity> ToMany<TEntity>(Expression<Func<TRootEntity, IEnumerable<TEntity>>> target) where TEntity : class
        {
            MapOptions<TRootEntity, TEntity> mapOptions = new MapOptions<TRootEntity, TEntity>(_queryDefine, target);

            _mapOptionsList.Add(mapOptions.GetMapOptions());

            return mapOptions;
        }

        public IMapOptions<TRootEntity, TEntity> ToOne<TEntity>(Expression<Func<TRootEntity, TEntity>> target) where TEntity : class
        {
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
            _mapOptions.GetEntity = (row, rootEntity) => getEntity(row, rootEntity as TRootEntity);

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> FormatPropertyName(Func<string, string> formatPropertyName)
        {
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
            _mapOptions.SetRow = (entity, rootEntity, row) => setRow(entity as TEntity, rootEntity as TRootEntity, row);

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> Table(string table)
        {
            _mapOptions.Table = table;

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> RelationId<TRelationEntity>(string relationIdColumn, Expression<Func<TRootEntity, TRelationEntity>> relationEntity) where TRelationEntity : class
        {
            _mapOptions.RelationIdColumn = relationIdColumn;
            _mapOptions.RelationEntityPath = relationEntity.GetExpressionPath();

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> UniqueKeys(string columns)
        {
            _mapOptions.UniqueKeys = columns;

            return this;
        }

        public IMapOptions<TRootEntity, TEntity> OptimisticLock<TRowVersion>(string rowVersionColumn, Func<TEntity, TRowVersion> newRowVersion = null)
        {
            _mapOptions.RowVersionColumn = rowVersionColumn;
            if (newRowVersion != null) _mapOptions.NewRowVersion = entity => newRowVersion(entity as TEntity);

            return this;
        }

    }
}
