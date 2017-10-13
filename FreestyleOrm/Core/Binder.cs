using System;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;

namespace FreestyleOrm.Core
{
    internal class Binder
    {
        public void Bind(Row row, object entity)
        {
            if (entity == null) return;

            Dictionary<string, PropertyInfo> propertyMap = entity.GetType().GetPropertyMap(BindingFlags.SetProperty, PropertyTypeFilters.IgonreClass);

            Dictionary<string, string> formatedPropertyNameMap = GetFormatedPropertyNameMap(row);

            foreach (var formatedPropertyName in formatedPropertyNameMap)
            {
                if (propertyMap.TryGetValue(formatedPropertyName.Key, out PropertyInfo property)) property.Set(entity, row[formatedPropertyName.Value]);
            }
        }

        private Dictionary<string, string> GetFormatedPropertyNameMap(Row row)
        {
            List<string> columnWithPrefixList = new List<string>();
            Dictionary<string, string> map = new Dictionary<string, string>();

            foreach (var column in row.Columns)
            {
                if (row.StartWithPrefix(column))
                {
                    columnWithPrefixList.Add(column);
                    continue;
                }

                string columnWithoutPrefix = row.GetColumnWithoutPrefix(column);
                string propertyName = row.FormatPropertyName(columnWithoutPrefix);

                map[propertyName] = column;
            }

            foreach (var column in columnWithPrefixList)
            {
                string columnWithoutPrefix = row.GetColumnWithoutPrefix(column);
                string propertyName = row.FormatPropertyName(columnWithoutPrefix);

                map[propertyName] = column;
            }

            return map;
        }

        public void Bind(object entity, Row row)
        {
            if (entity == null) return;

            Dictionary<string, PropertyInfo> propertyMap = entity.GetType().GetPropertyMap(BindingFlags.GetProperty, PropertyTypeFilters.IgonreClass);
            Dictionary<string, string> formatedPropertyNameMap = GetFormatedPropertyNameMap(row);

            foreach (var formatedPropertyName in formatedPropertyNameMap)
            {
                if (propertyMap.TryGetValue(formatedPropertyName.Key, out PropertyInfo property)) row[formatedPropertyName.Value] = property.Get(entity);
            }
        }
    }
}