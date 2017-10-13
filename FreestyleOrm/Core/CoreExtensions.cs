using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Collections.ObjectModel;

namespace FreestyleOrm.Core
{
    internal static class CoreExtensions
    {
        private static Dictionary<PropertyInfo, PropertyAccessor> _propertyAccessorMap = new Dictionary<PropertyInfo, PropertyAccessor>();

        private class PropertyAccessor
        {
            public PropertyAccessor(PropertyInfo property)
            {
                MethodInfo method = typeof(CoreExtensions)
                    .GetMethod("CreateGetter", BindingFlags.Static | BindingFlags.NonPublic)
                    .MakeGenericMethod(property.ReflectedType, property.PropertyType);

                Func<object, object> getter = (Func<object, object>)method.Invoke(null, new object[] { property });

                Get = getter;

                method = typeof(CoreExtensions)
                    .GetMethod("CreateSetter", BindingFlags.Static | BindingFlags.NonPublic)
                    .MakeGenericMethod(property.ReflectedType, property.PropertyType);

                Action<object, object> setter = (Action<object, object>)method.Invoke(null, new object[] { property });

                Set = setter; ;
            }

            public Func<object, object> Get { get; }
            public Action<object, object> Set { get; }
        }

        private static Func<object, object> CreateGetter<TObj, TValue>(PropertyInfo property)
        {
            if (!property.CanRead)
            {
                return obj => property.Get(obj);
            }

            Func<TObj, TValue> getDelegate =
                (Func<TObj, TValue>)Delegate.CreateDelegate(
                         typeof(Func<TObj, TValue>),
                         property.GetGetMethod(nonPublic: true));

            return obj =>
            {
                TValue value = getDelegate((TObj)obj);

                return value;
            };
        }

        private static Action<object, object> CreateSetter<TObj, TValue>(PropertyInfo property)
        {
            if (!property.CanWrite)
            {
                return (obj, value) => property.Set(obj, value);
            }

            Action<TObj, TValue> setDelegate =
                (Action<TObj, TValue>)Delegate.CreateDelegate(
                         typeof(Action<TObj, TValue>),
                         property.GetSetMethod(nonPublic: true));

            return (obj, value) =>
            {
                if (value is TValue)
                {
                    setDelegate((TObj)obj, (TValue)value);
                }
                else if (CanChangeType<TValue>(value))
                {
                    if (Nullable.GetUnderlyingType(typeof(TValue)) != null)
                    {
                        value = Convert.ChangeType(value, typeof(TValue).GetGenericArguments()[0]);
                    }
                    else
                    {
                        value = Convert.ChangeType(value, typeof(TValue));
                    }

                    setDelegate((TObj)obj, (TValue)value);
                }
            };
        }

        private static bool CanChangeType<TValue>(object value)
        {
            Type conversionType = typeof(TValue);

            if (conversionType == null
                || value == null
                || value == DBNull.Value
                || !(value is IConvertible))
            {
                return false;
            }

            return true;
        }

        public static object Get(this PropertyInfo property, object obj)
        {
            PropertyAccessor accessor;

            if (!_propertyAccessorMap.TryGetValue(property, out accessor))
            {
                accessor = new PropertyAccessor(property);
                _propertyAccessorMap[property] = accessor;
            }

            return accessor.Get(obj);
        }        

        public static void Set(this PropertyInfo property, object obj, object value)
        {
            PropertyAccessor accessor;

            if (!_propertyAccessorMap.TryGetValue(property, out accessor))
            {
                accessor = new PropertyAccessor(property);
                _propertyAccessorMap[property] = accessor;
            }

            accessor.Set(obj, value);
        }

        public static bool IsList(this Type type)
        {
            return IsList(type, out Type elementType);
        }

        public static bool IsList(this Type type, out Type elementType)
        {
            elementType = null;

            if (type == typeof(string)) return false;

            if (type.IsArray)
            {
                elementType = type.GetElementType();
                return true;
            }

            foreach (var interfaceType in type.GetInterfaces())
            {
                if (!interfaceType.IsGenericType) continue;

                var defineType = interfaceType.GetGenericTypeDefinition();

                if (defineType != null && (defineType == typeof(List<>) || defineType == typeof(Collection<>)))
                {
                    elementType = interfaceType.GetGenericArguments()[0];
                    return true;
                }
            }

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                elementType = type.GetGenericArguments()[0];
                return true;
            }

            return false;
        }

        public static Dictionary<string, PropertyInfo> GetPropertyMap(this Type type, BindingFlags bindingFlags, PropertyTypeFilters propertyTypeFilters)
        {
            PropertyInfo[] properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | bindingFlags);            

            Dictionary<string, PropertyInfo> map = new Dictionary<string, PropertyInfo>();

            foreach (var property in properties)
            {
                if (propertyTypeFilters == PropertyTypeFilters.IgonreClass)
                {
                    if (property.PropertyType != typeof(string) && property.PropertyType.IsClass) continue;                    
                }
                else if (propertyTypeFilters == PropertyTypeFilters.OnlyClass)
                {
                    if (property.PropertyType == typeof(string) || property.PropertyType.IsPrimitive || property.PropertyType.IsValueType) continue;                    
                }

                map[property.Name] = property;
            }

            return map;
        }

        public static object Create(this Type type)
        {
            return Activator.CreateInstance(type, true);
        }

        public static string GetExpressionPath<TRoot, TTarget>(this Expression<Func<TRoot, TTarget>> expression) where TRoot : class
        {
            return GetExpressionPath(expression, out PropertyInfo property);
        }

        public static string GetExpressionPath<TRoot, TTarget>(this Expression<Func<TRoot, TTarget>> expression, out PropertyInfo property) where TRoot : class
        {
            property = null;

            string[] sections = expression.Body.ToString().Split('.');
            List<string> path = new List<string>();

            Dictionary<string, PropertyInfo> propertyMap = typeof(TRoot).GetPropertyMap(BindingFlags.Public, PropertyTypeFilters.All);

            foreach (var section in sections)
            {
                var targetSection = section;

                int arrayHolderIndex = targetSection.IndexOf("[");
                if (arrayHolderIndex != -1) targetSection = targetSection.Substring(0, arrayHolderIndex + 1);


                if (propertyMap.TryGetValue(targetSection, out property))
                {
                    path.Add(targetSection);

                    Type type;

                    if (property.PropertyType.IsList(out Type elementType))
                    {
                        type = elementType;
                    }
                    else
                    {
                        type = property.PropertyType;
                    }

                    propertyMap = type.GetPropertyMap(BindingFlags.Public, PropertyTypeFilters.All);
                }
            }

            return string.Join(".", path);
        }
    }

    internal enum PropertyTypeFilters
    {
        All,
        IgonreClass,
        OnlyClass
    }    
}