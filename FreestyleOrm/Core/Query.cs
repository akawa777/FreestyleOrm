using System;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;
using System.Data;
using System.Collections;

namespace FreestyleOrm.Core
{

    internal class Query<TRootEntity> : IQuery<TRootEntity> where TRootEntity : class
    {
        public Query(IDatabaseAccessor databaseAccessor, QueryOptions queryOptions, IQueryDefine queryDefine)
        {
            _databaseAccessor = databaseAccessor;
            _queryOptions = queryOptions;
            _queryDefine = queryDefine;
        }

        private IDatabaseAccessor _databaseAccessor;
        private QueryOptions _queryOptions;
        private IQueryDefine _queryDefine;
        private Action<IMap<TRootEntity>> _setMap = map => { };
        private Binder _binder = new Binder();

        private enum SaveMode
        {
            Insert,
            Update,
            Delete
        }

        public void Delete(TRootEntity rootEntity)
        {
            Save(rootEntity, out object lastId, SaveMode.Delete);
        }

        public IEnumerable<TRootEntity> Fetch()
        {
            Map<TRootEntity> map = new Map<TRootEntity>(_queryDefine);

            _setMap(map);            

            using (var reader = _databaseAccessor.CreateFetchReader(_queryOptions, out Action dispose))
            {
                TRootEntity rootEntity = null;
                Row prevRow = null;

                while (reader.Read())
                {
                    MapOptions rootMapOptions = map.RootMapOptions;                    

                    Row currentRow = Row.CreateReadRow(reader, rootMapOptions);

                    List<string> uniqueKeys = new List<string>();

                    if (rootMapOptions == null || currentRow.CanCreate(prevRow, uniqueKeys))
                    {
                        if (rootEntity != null) yield return rootEntity;

                        rootEntity = null;
                        rootEntity = rootMapOptions.GetEntity(currentRow, rootEntity) as TRootEntity;                        
                    }

                    uniqueKeys.AddRange(currentRow.UniqueKeys);

                    if (rootMapOptions == null) continue;

                    foreach(var mapOptions in map.MapOptionsListWithoutRoot)
                    {
                        currentRow.SetMapOptions(mapOptions);

                        if (!currentRow.CanCreate(prevRow, uniqueKeys))
                        {
                            uniqueKeys.AddRange(currentRow.UniqueKeys);

                            continue;
                        }

                        uniqueKeys.AddRange(currentRow.UniqueKeys);

                        object parentEntity = rootEntity;
                        PropertyInfo property = null;

                        foreach (var section in mapOptions.ExpressionSections)
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

                        if (mapOptions.IsToMany)
                        {
                            object list = property.Get(parentEntity);

                            if (list == null)
                            {
                                if (property.PropertyType.IsArray)
                                {
                                    list = Array.CreateInstance(mapOptions.EntityType, 0);
                                }
                                else if (property.PropertyType == typeof(IEnumerable<>).MakeGenericType(mapOptions.EntityType))
                                {
                                    list = typeof(List<>).MakeGenericType(mapOptions.EntityType).Create();
                                }
                                else
                                {
                                    list = property.PropertyType.Create();
                                }

                                property.Set(parentEntity, list);
                            }

                            object entity = mapOptions.GetEntity(currentRow, rootEntity);

                            if (property.PropertyType.IsArray)
                            {
                                Array array = (Array)list;
                                Array newArray = Array.CreateInstance(mapOptions.EntityType, array.Length + 1);

                                array.CopyTo(newArray, 0);
                                newArray.SetValue(entity, array.Length);

                                property.Set(parentEntity, newArray);
                            }
                            else if (property.PropertyType == typeof(IEnumerable<>).MakeGenericType(mapOptions.EntityType))
                            {                                
                                dynamic dynamicList = typeof(List<>).MakeGenericType(mapOptions.EntityType).Create();
                                dynamicList.AddRange(list as dynamic);
                                dynamicList.Add(entity as dynamic);

                                property.Set(parentEntity, dynamicList as object);
                            }
                            else
                            {
                                dynamic dynamicList = list;
                                dynamic dynamicEntity = entity;
                                dynamicList.Add(dynamicEntity);
                            }
                        }
                        else
                        {
                            object entity = mapOptions.GetEntity(currentRow, rootEntity);
                            property.Set(parentEntity, entity);
                        }
                    }

                    prevRow = currentRow;
                }

                if (rootEntity != null) yield return rootEntity;

                reader.Close();
                dispose();
            }
        }

        public IEnumerable<TRootEntity> Fetch(int page, int size)
        {
            return Fetch().Skip((page - 1) * size).Take(size);
        }

        public IQuery<TRootEntity> Formats(Action<Dictionary<string, object>> setFormats)
        {
            _queryOptions.SetFormats = setFormats;

            return this;
        }

        public IQuery<TRootEntity> Map(Action<IMap<TRootEntity>> setMap)
        {
            _setMap = setMap;

            return this;
        }

        public IQuery<TRootEntity> Parametes(Action<Dictionary<string, object>> setParameters)
        {
            _queryOptions.SetParameters = setParameters;

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

        private void Save<TId>(TRootEntity rootEntity, out TId lastId, SaveMode saveMode)
        {
            if (rootEntity == null) throw new ArgumentException("rootEntity is null.");
            if (_queryOptions.Transaction == null) throw new InvalidOperationException("Transaction is null.");

            lastId = default(TId);
            _databaseAccessor.BeginSave();

            List<Row> updateRows = GetRows(rootEntity);            

            if (saveMode == SaveMode.Insert)
            {
                _databaseAccessor.Insert(updateRows.First(), _queryOptions, out object lastIdObj);

                if (lastIdObj != null) lastId = (TId)lastIdObj;

                foreach (var updateRow in updateRows.Skip(1))
                {
                    _databaseAccessor.Insert(updateRow, _queryOptions, out lastIdObj);
                }

                return;
            }

            List<TRootEntity> currentEntityList = Fetch().ToList();
            if (currentEntityList.Count == 0) throw new InvalidOperationException("not exists current entity.");
            if (currentEntityList.Count != 1) throw new InvalidOperationException("1 or more current entities.");

            List<Row> currentRows = GetRows(currentEntityList.First());
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

        public IQuery<TRootEntity> TempTables(Action<Dictionary<string, TempTable>> setTempTables)
        {
            _queryOptions.SetTempTables = setTempTables;

            return this;
        }

        private List<Row> GetRows(TRootEntity rootEntity)
        {
            List<Row> rows = new List<Row>();

            if (rootEntity == null) return rows;

            Map<TRootEntity> map = new Map<TRootEntity>(_queryDefine);
            _setMap(map);

            if (map.RootMapOptions.Refer == Refer.Read) throw new InvalidOperationException($"{typeof(TRootEntity).Name} is Refer.Read.");

            using (var reader = _databaseAccessor.CreateTableReader(_queryOptions, map.RootMapOptions, out string[] primaryKeys))
            {
                Row row = Row.CreateWriteRow(reader, map.RootMapOptions, primaryKeys, rootEntity);
                map.RootMapOptions.SetRow(rootEntity, rootEntity, row);
                
                rows.Add(row);

                reader.Close();
            }

            foreach (var mapOptions in map.MapOptionsListWithoutRoot)
            {
                if (mapOptions.Refer == Refer.Read) continue;

                object parentEntity = rootEntity;
                PropertyInfo property = null;                

                foreach (var section in mapOptions.ExpressionSections)
                {
                    if (property != null && !property.PropertyType.IsList())
                    {
                        if (property.PropertyType.IsList())
                        {
                            dynamic list = property.Get(parentEntity);
                            parentEntity = list.Last();
                        }
                        else
                        {
                            parentEntity = property.Get(parentEntity);
                        }
                    }

                    Dictionary<string, PropertyInfo> propertyMap = parentEntity.GetType().GetPropertyMap(BindingFlags.GetProperty | BindingFlags.SetProperty, PropertyTypeFilters.OnlyClass);

                    property = propertyMap[section];
                }                

                if (mapOptions.IsToMany)
                {
                    object list = property.Get(parentEntity);
                    if (list == null) continue;

                    using (var reader = _databaseAccessor.CreateTableReader(_queryOptions, mapOptions, out string[] primaryKeys))
                    {
                        foreach (object entity in list as IEnumerable)
                        {
                            Row row = Row.CreateWriteRow(reader, mapOptions, primaryKeys, entity);
                            mapOptions.SetRow(entity, rootEntity, row);
                            rows.Add(row);
                        }

                        reader.Close();
                    }
                }
                else
                {
                    object entity = property.Get(parentEntity);
                    if (entity == null) continue;

                    using (var reader = _databaseAccessor.CreateTableReader(_queryOptions, mapOptions, out string[] primaryKeys))
                    {
                        Row row = Row.CreateWriteRow(reader, mapOptions, primaryKeys, entity);
                        mapOptions.SetRow(entity, rootEntity, row);
                        rows.Add(row);

                        reader.Close();
                    }
                }
            }

            return rows;
        }

        public IQuery<TRootEntity> Connection(IDbConnection connection)
        {
            _queryOptions.Connection = connection;

            return this;
        }

        public IQuery<TRootEntity> Transaction(IDbTransaction transaction)
        {
            _queryOptions.Transaction = transaction;

            return this;
        }
    }
}
