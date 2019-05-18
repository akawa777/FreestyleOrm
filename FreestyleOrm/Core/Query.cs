using System;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;
using System.Data;
using System.Collections;
using System.Collections.Specialized;
using System.Text;

namespace FreestyleOrm.Core
{
    internal class Query<TRootEntity> : IQueryFlat<TRootEntity>, IQuery<TRootEntity> where TRootEntity : class
    {
        public Query(IDatabaseAccessor databaseAccessor, QueryOptions queryOptions)
        {
            _databaseAccessor = databaseAccessor;
            _queryOptions = queryOptions;
            _binder = new Binder(queryOptions.IsFlatFormat);
        }

        private IDatabaseAccessor _databaseAccessor;
        private QueryOptions _queryOptions;        
        private Action<Map<TRootEntity>> _setMap = map => { };
        private Binder _binder;

        private class TotalCount
        {
            public int Value { get; set; }
        }

        private IEnumerable<object> CreateNodes(IEnumerable<object> entites, ReNest reNest)
        {
            if (entites == null) Enumerable.Empty<object>();

            Dictionary<string, object> nodeMap = new Dictionary<string, object>();
            Dictionary<string, List<string>> parentNodeMap = new Dictionary<string, List<string>>();

            Dictionary<string, PropertyInfo> propertyMap = null;

            foreach (var entity in entites)
            {
                if (propertyMap == null) propertyMap = entity.GetType().GetPropertyMap(BindingFlags.Public, PropertyTypeFilters.All);

                List<string> idValus = new List<string>();

                foreach (var propertyName in reNest.IdProperties)
                {
                    var propertyInfo = propertyMap[propertyName];
                    var value = propertyInfo.Get(entity);
                    idValus.Add(value.ToString());
                }

                var id = string.Join(",", idValus);
                nodeMap[id] = entity;

                List<string> parentValus = new List<string>();
                foreach (var propertyName in reNest.ParentProperties)
                {
                    var propertyInfo = propertyMap[propertyName];
                    var value = propertyInfo.Get(entity);
                    if (value == null) break;
                    parentValus.Add(value.ToString());
                }

                if (parentValus.Count == 0) continue;

                var parentId = string.Join(",", parentValus);

                List<string> propertyIdList;
                if (!parentNodeMap.TryGetValue(id, out propertyIdList))
                {
                    propertyIdList = new List<string>();
                    parentNodeMap[id] = propertyIdList;
                }

                propertyIdList.Add(parentId);
            }

            if (nodeMap.Count == 0) return Enumerable.Empty<object>();

            List<object> nodes = new List<object>();

            foreach (var id in nodeMap.Keys)
            {
                object node = nodeMap[id];

                if (!parentNodeMap.ContainsKey(id))
                {
                    nodes.Add(node);
                    continue;
                }

                foreach (var parentId in parentNodeMap[id])
                {
                    if (!nodeMap.ContainsKey(parentId))
                    {
                        nodes.Add(node);
                        continue;
                    }

                    var property = propertyMap[reNest.NestEntityPath];

                    object parentNode = nodeMap[parentId];

                    object list = property.Get(parentNode);

                    if (list == null)
                    {
                        if (property.PropertyType.IsArray)
                        {
                            list = Array.CreateInstance(node.GetType(), 0);
                        }
                        else if (property.PropertyType == typeof(IEnumerable<>).MakeGenericType(node.GetType()))
                        {
                            list = typeof(List<>).MakeGenericType(node.GetType()).Create();
                        }
                        else
                        {
                            list = property.PropertyType.Create();
                        }

                        property.Set(parentNode, list);
                    }

                    if (property.PropertyType.IsArray)
                    {
                        Array array = (Array)list;
                        Array newArray = Array.CreateInstance(node.GetType(), array.Length + 1);

                        array.CopyTo(newArray, 0);
                        newArray.SetValue(node, array.Length);

                        property.Set(parentNode, newArray);
                    }
                    else if (property.PropertyType == typeof(IEnumerable<>).MakeGenericType(node.GetType()))
                    {
                        dynamic dynamicList = typeof(List<>).MakeGenericType(node.GetType()).Create();
                        dynamicList.AddRange(list as dynamic);
                        dynamicList.Add(node as dynamic);

                        property.Set(parentNode, dynamicList as object);
                    }
                    else
                    {
                        dynamic dynamicList = list;
                        dynamic dynamicEntity = node;
                        dynamicList.Add(dynamicEntity);
                    }
                }
            }

            return nodes;
        }

        private void SetReNestNodes(Map<TRootEntity> map, object rootEntity)
        {
            foreach (var mapRule in map.MapRuleListWithoutRoot)
            {
                if (!mapRule.ReNest.Should()) continue;

                object parentEntity = rootEntity;
                PropertyInfo property = null;

                foreach (var section in mapRule.ExpressionSections)
                {
                    if (property != null && !property.PropertyType.IsList())
                    {
                        parentEntity = property.Get(parentEntity);
                    }
                    if (property != null && property.PropertyType.IsList())
                    {
                        var list = property.Get(parentEntity) as IEnumerable;
                        foreach (var item in list) parentEntity = item;
                    }

                    Dictionary<string, PropertyInfo> propertyMap = parentEntity.GetType().GetPropertyMap(BindingFlags.GetProperty | BindingFlags.SetProperty, PropertyTypeFilters.OnlyClass);

                    property = propertyMap[section];
                }

                IEnumerable<object> entities;

                if (mapRule.IsToMany)
                {
                    entities = property.Get(parentEntity) as IEnumerable<object>;
                }
                else
                {
                    var entity = property.Get(parentEntity);
                    entities = new object[] { entity };
                }

                IEnumerable<object> nodes = CreateNodes(entities, mapRule.ReNest);
                property.Set(parentEntity, null);

                if (nodes.Count() > 0 && mapRule.IsToMany)
                {
                    object newList;

                    if (property.PropertyType.IsArray)
                    {
                        newList = Array.CreateInstance(mapRule.EntityType, 0);
                    }
                    else if (property.PropertyType == typeof(IEnumerable<>).MakeGenericType(mapRule.EntityType))
                    {
                        newList = typeof(List<>).MakeGenericType(mapRule.EntityType).Create();
                    }
                    else
                    {
                        newList = property.PropertyType.Create();
                    }

                    property.Set(parentEntity, newList);

                    foreach (var node in nodes)
                    {
                        if (property.PropertyType.IsArray)
                        {
                            Array array = (Array)newList;
                            Array newArray = Array.CreateInstance(mapRule.EntityType, array.Length + 1);

                            array.CopyTo(newArray, 0);
                            newArray.SetValue(node, array.Length);

                            property.Set(parentEntity, newArray);
                        }
                        else if (property.PropertyType == typeof(IEnumerable<>).MakeGenericType(mapRule.EntityType))
                        {
                            dynamic dynamicList = typeof(List<>).MakeGenericType(mapRule.EntityType).Create();
                            dynamicList.AddRange(newList as dynamic);
                            dynamicList.Add(node as dynamic);

                            property.Set(parentEntity, dynamicList as object);
                        }
                        else
                        {
                            dynamic dynamicList = newList;
                            dynamic dynamicEntity = node;
                            dynamicList.Add(dynamicEntity);
                        }
                    }
                }
                else if (nodes.Count() > 0)
                {
                    property.Set(parentEntity, nodes.FirstOrDefault());
                }
            }
        }

        public IEnumerable<TRootEntity> Fetch()
        {
            Map<TRootEntity> map = CreateMap();

            if (map.RootMapRule.ReNest.Should())
            {
                var list = Fetch(1, int.MaxValue, new TotalCount(), map);
                var nodes = CreateNodes(list, map.RootMapRule.ReNest);

                return nodes.Select(x => x as TRootEntity);
            }
            else
            {
                return Fetch(1, int.MaxValue, new TotalCount(), map);
            }
        }

        public Page<TRootEntity> Page(int no, int size)
        {
            if (no < 1) throw new AggregateException($"{no} is less than 1.");
            if (size < 1) throw new AggregateException($"{size} is less than 1.");

            Map<TRootEntity> map = CreateMap();

            TotalCount totalCount = new TotalCount();
            IEnumerable<TRootEntity> list;

            if (map.RootMapRule.ReNest.Should())
            {
                var nodes = Fetch();
                totalCount.Value = nodes.Count();
                list = Fetch().Skip((no - 1) * size).Take(size);
            }
            else
            {
                list = Fetch(no, size, totalCount, map).ToList();
            }

            return new Page<TRootEntity>(no, size, totalCount.Value, list);
        }

        private Map<TRootEntity> CreateMap()
        {
            Map<TRootEntity> map = new Map<TRootEntity>(_queryOptions);

            _setMap(map);

            return map;
        }

        private IEnumerable<TRootEntity> Fetch(int page, int size, TotalCount totalCount, Map<TRootEntity> map)
        {
            totalCount.Value = 0;

            using (var reader = _databaseAccessor.CreateFetchReader(_queryOptions, out Action dispose))
            {
                TRootEntity rootEntity = null;
                Row prevRow = null;
                int currentPage = 0;
                int currentSize = 0;

                Dictionary<MapRule, Dictionary<string, bool>> mapUniqueKeyValueCache = new Dictionary<MapRule, Dictionary<string, bool>>();

                while (reader.Read())
                {
                    MapRule rootMapRule = map.RootMapRule;

                    Row currentRow = Row.CreateReadRow(reader, rootMapRule);

                    List<string> uniqueKeys = new List<string>();

                    if (currentRow.CanCreate(prevRow, uniqueKeys))
                    {
                        totalCount.Value++;

                        if (rootEntity != null)
                        {
                            SetReNestNodes(map, rootEntity);
                            yield return rootEntity;
                        }

                        rootEntity = null;

                        if (currentPage == 0 || currentSize % size == 0)
                        {
                            currentPage++;
                            currentSize = 0;
                        }

                        currentSize++;

                        if (currentPage == page)
                        {
                            rootEntity = rootMapRule.GetEntity(currentRow, rootEntity) as TRootEntity;
                        }
                    }

                    if (rootEntity == null)
                    {
                        prevRow = currentRow;
                        continue;
                    }

                    if (_queryOptions.IsFlatFormat)
                    {
                        continue;
                    }

                    uniqueKeys.AddRange(currentRow.UniqueKeys);                    

                    foreach (var mapRule in map.MapRuleListWithoutRoot)
                    {                        
                        currentRow.SetMapRule(mapRule);

                        // ユニークキーの値がNULL、または、依然と重複している場合は、処理をしない >>

                        StringBuilder uniqueKeyValuesBuilder = new StringBuilder();

                        bool canContinue = true;

                        foreach (var key in currentRow.UniqueKeys)
                        {
                            if (currentRow[key] == DBNull.Value) canContinue = false;

                            uniqueKeyValuesBuilder.Append(currentRow[key].ToString() + ":");
                        }

                        string uniqueKeyValues = uniqueKeyValuesBuilder.ToString();

                        if (mapUniqueKeyValueCache.TryGetValue(mapRule, out Dictionary<string, bool> values))
                        {
                            if (values.ContainsKey(uniqueKeyValues)) canContinue = false; ;
                        }
                        else
                        {
                            mapUniqueKeyValueCache[mapRule] = new Dictionary<string, bool>();
                        }

                        mapUniqueKeyValueCache[mapRule][uniqueKeyValues] = true;

                        if (!canContinue) continue;

                        // ユニークキーが重複している場合は、処理をしない <<

                        object parentEntity = rootEntity;
                        PropertyInfo property = null;

                        foreach (var section in mapRule.ExpressionSections)
                        {
                            if (property != null && !property.PropertyType.IsList())
                            {
                                parentEntity = property.Get(parentEntity);
                            }
                            if (property != null && property.PropertyType.IsList())
                            {
                                var list = property.Get(parentEntity) as IEnumerable;
                                foreach (var item in list) parentEntity = item;
                            }

                            Dictionary<string, PropertyInfo> propertyMap = parentEntity.GetType().GetPropertyMap(BindingFlags.GetProperty | BindingFlags.SetProperty, PropertyTypeFilters.OnlyClass);

                            property = propertyMap[section];
                        }

                        if (mapRule.IsToMany)
                        {
                            object list = property.Get(parentEntity);

                            if (list == null)
                            {
                                if (property.PropertyType.IsArray)
                                {
                                    list = Array.CreateInstance(mapRule.EntityType, 0);
                                }
                                else if (property.PropertyType == typeof(IEnumerable<>).MakeGenericType(mapRule.EntityType))
                                {
                                    list = typeof(List<>).MakeGenericType(mapRule.EntityType).Create();
                                }
                                else
                                {
                                    list = property.PropertyType.Create();
                                }

                                property.Set(parentEntity, list);
                            }

                            object entity = mapRule.GetEntity(currentRow, rootEntity);

                            if (property.PropertyType.IsArray)
                            {
                                Array array = (Array)list;
                                Array newArray = Array.CreateInstance(mapRule.EntityType, array.Length + 1);

                                array.CopyTo(newArray, 0);
                                newArray.SetValue(entity, array.Length);

                                property.Set(parentEntity, newArray);
                            }
                            else if (property.PropertyType == typeof(IEnumerable<>).MakeGenericType(mapRule.EntityType))
                            {
                                dynamic dynamicList = typeof(List<>).MakeGenericType(mapRule.EntityType).Create();
                                dynamicList.AddRange(list as dynamic);
                                dynamicList.Add(entity as dynamic);

                                property.Set(parentEntity, dynamicList as object);
                            }
                            else
                            {
                                MethodInfo method = list.GetType().GetMethod("Add", new Type[] { mapRule.EntityType });
                                method.Invoke(list, new object[] { entity });
                            }
                        }
                        else
                        {
                            object entity = mapRule.GetEntity(currentRow, rootEntity);
                            property.Set(parentEntity, entity);
                        }
                    }

                    prevRow = currentRow;
                }

                if (rootEntity != null)
                {
                    SetReNestNodes(map, rootEntity);
                    yield return rootEntity;
                }

                reader.Close();
                dispose();
            }
        }

        public IQuery<TRootEntity> Map(Action<IMap<TRootEntity>> setMap)
        {
            _setMap = setMap ?? throw new AggregateException($"{setMap} is null.");

            return this;
        }

        public IQuery<TRootEntity> Params(Action<Dictionary<string, object>> setParams)
        {
            _queryOptions.SetParams = setParams ?? throw new AggregateException($"{setParams} is null.");

            return this;
        }

        public void Insert(TRootEntity rootEntity)
        {
            Save(rootEntity, out object lastId, SaveMode.Insert);
        }

        public void Insert<TId>(TRootEntity rootEntity, out TId lastId)
        {
            Save(rootEntity, out lastId, SaveMode.Insert);
        }

        public void Update(TRootEntity rootEntity)
        {
            Save(rootEntity, out object lastId, SaveMode.Update);
        }

        public void Delete(TRootEntity rootEntity)
        {
            Save(rootEntity, out object lastId, SaveMode.Delete);
        }

        private void Save<TId>(TRootEntity rootEntity, out TId lastId, SaveMode saveMode)
        {
            if (rootEntity == null) throw new ArgumentException("rootEntity is null.");
            if (_queryOptions.Transaction == null) throw new InvalidOperationException("Transaction is null.");            

            Map<TRootEntity> map = new Map<TRootEntity>(_queryOptions);
            _setMap(map);

            lastId = default(TId);
            _databaseAccessor.BeginSave();

            List<Row> updateRows = GetRows(rootEntity, map);

            if (saveMode == SaveMode.Insert)
            {
                _databaseAccessor.Insert(updateRows.First(), _queryOptions, out object lastIdObj);

                if (lastIdObj != null) lastId = (TId)Convert.ChangeType(lastIdObj, typeof(TId));

                foreach (var updateRow in updateRows.Skip(1))
                {
                    _databaseAccessor.Insert(updateRow, _queryOptions, out lastIdObj);
                }

                return;
            }

            List<TRootEntity> currentEntityList = Fetch().ToList();
            if (currentEntityList.Count == 0) throw new InvalidOperationException("not exists current entity.");
            if (currentEntityList.Count != 1) throw new InvalidOperationException("1 or more current entities.");

            List<Row> currentRows = GetRows(currentEntityList.First(), map);
            if (currentRows.First().Id != updateRows.First().Id) throw new InvalidOperationException("not match primary values of current entity.");

            Dictionary<string, Row> currentRowMap = currentRows.ToDictionary(x => x.Id);
            Dictionary<string, Row> updateRowMap = updateRows.ToDictionary(x => x.Id);

            if (saveMode == SaveMode.Update)
            {
                _databaseAccessor.Update(updateRows.First(), _queryOptions);

                foreach (var updateRow in updateRows.Skip(1))
                {
                    string id = updateRow.Id;

                    if (currentRowMap.ContainsKey(updateRow.Id))
                    {
                        _databaseAccessor.Update(updateRow, _queryOptions);
                    }
                    else
                    {
                        _databaseAccessor.Insert(updateRow, _queryOptions, out object lastIdObj);
                    }
                }

                foreach (var currentRow in currentRows.Skip(1))
                {
                    if (updateRowMap.ContainsKey(currentRow.Id)) continue;

                    _databaseAccessor.Delete(currentRow, _queryOptions);
                }
            }
            else
            {
                foreach (var updateRow in updateRows)
                {
                    _databaseAccessor.Delete(updateRow, _queryOptions);
                }
            }
        }

        public IQuery<TRootEntity> TempTables(Action<ITempTableSet> setTempTables)
        {
            _queryOptions.SetTempTables = setTempTables ?? throw new AggregateException($"{setTempTables} is null.");

            return this;
        }

        private List<Row> GetRows(TRootEntity rootEntity, Map<TRootEntity> map)
        {
            List<Row> rows = new List<Row>();

            if (rootEntity == null) return rows;

            if (map.RootMapRule.Refer == Refer.Read) throw new InvalidOperationException($"{typeof(TRootEntity).Name} is not set Writable.");

            using (var reader = _databaseAccessor.CreateTableReader(_queryOptions, map.RootMapRule, out string[] primaryKeys))
            {
                Row row = Row.CreateWriteRow(reader, map.RootMapRule, primaryKeys, rootEntity);
                map.RootMapRule.SetRow(rootEntity, rootEntity, row);

                rows.Add(row);

                reader.Close();
            }

            foreach (var mapRule in map.MapRuleListWithoutRoot)
            {
                if (mapRule.Refer == Refer.Read) continue;

                List<object> entities = GetEntities(rootEntity, mapRule);

                using (var reader = _databaseAccessor.CreateTableReader(_queryOptions, mapRule, out string[] primaryKeys))
                {
                    foreach (object entity in entities)
                    {
                        Row row = Row.CreateWriteRow(reader, mapRule, primaryKeys, entity);
                        mapRule.SetRow(entity, rootEntity, row);
                        rows.Add(row);
                    }

                    reader.Close();
                }
            }

            return rows;
        }

        private List<object> GetEntities(TRootEntity rootEntity, MapRule mapRule)
        {
            List<object> entities = new List<object>();

            List<object> parentEntities = new List<object>();
            parentEntities.Add(rootEntity);

            PropertyInfo property = null;

            foreach (var section in mapRule.ExpressionSections)
            {
                entities = new List<object>();

                foreach (var parentEntity in parentEntities)
                {
                    Dictionary<string, PropertyInfo> propertyMap = parentEntity.GetType().GetPropertyMap(BindingFlags.GetProperty | BindingFlags.SetProperty, PropertyTypeFilters.OnlyClass);

                    property = propertyMap[section];

                    if (property.PropertyType.IsList())
                    {
                        dynamic list = property.Get(parentEntity);

                        if (list != null) entities.AddRange(list);
                    }
                    else
                    {
                        object entity = property.Get(parentEntity);

                        if (entity != null) entities.Add(entity);
                    }
                }

                parentEntities = entities;                
            }

            return entities;
        }
    

        public IQuery<TRootEntity> Connection(IDbConnection connection)
        {
            _queryOptions.Connection = connection ?? throw new AggregateException("connection is null.");

            return this;
        }

        public IQuery<TRootEntity> Transaction(IDbTransaction transaction)
        {
            _queryOptions.Transaction = transaction;

            return this;
        }

        IQueryFlat<TRootEntity> IQueryFlat<TRootEntity>.Params(Action<Dictionary<string, object>> setParams)
        {
            this.Params(setParams);

            return this;
        }

        IQueryFlat<TRootEntity> IQueryFlat<TRootEntity>.TempTables(Action<ITempTableSet> setTempTables)
        {
            this.TempTables(setTempTables);

            return this;
        }

        IQueryFlat<TRootEntity> IQueryFlat<TRootEntity>.Connection(IDbConnection connection)
        {
            this.Connection(connection);

            return this;
        }

        IQueryFlat<TRootEntity> IQueryFlat<TRootEntity>.Transaction(IDbTransaction transaction)
        {
            this.Transaction(transaction);

            return this;
        }

        IQueryFlat<TRootEntity> IQueryFlat<TRootEntity>.Map(Action<IMapFlat<TRootEntity>> setMap)
        {
            _setMap = setMap ?? throw new AggregateException($"{setMap} is null.");

            return this;
        }        
    }
}
