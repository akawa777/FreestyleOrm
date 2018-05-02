using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Linq;

namespace FreestyleOrm.Core
{
    internal class OptimisticLock : IOptimisticLock
    {
        public string[] GetColumns() => string.IsNullOrEmpty(_columns) ? new string[0] : _columns.Split(',').Select(x => x.Trim()).ToArray();
        public object[] GetCurrentValues(object entity)
        {
            object[] values = _currentValues(entity);

            if (values == null || values.Length == 0) return new object[0];

            return values;

        }
        public object[] GetNewValues(object entity)
        {
            object[] values = _newValues(entity);

            if (values == null || values.Length == 0) return new object[0];

            return values;
        }

        protected string _columns;
        protected Func<object, object[]> _currentValues;
        protected Func<object, object[]> _newValues;

        public IOptimisticLock Columns(string columns)
        {
            _columns = columns;

            return this;
        }

        public IOptimisticLock CurrentValues(Func<object, object[]> values)
        {
            _currentValues = values;

            return this;
        }

        public IOptimisticLock NewValues(Func<object, object[]> values)
        {
            _newValues = values;

            return this;
        }
    }



    internal class OptimisticLock<TEntity> : OptimisticLock, IOptimisticLock, IOptimisticLock<TEntity> where TEntity : class
    {
        IOptimisticLock<TEntity> IOptimisticLock<TEntity>.Columns(string columns)
        {
            _columns = columns;

            return this;
        }

        IOptimisticLock<TEntity> IOptimisticLock<TEntity>.CurrentValues(Func<TEntity, object[]> values)
        {
            _currentValues = entity => values(entity as TEntity);

            return this;
        }

        IOptimisticLock<TEntity> IOptimisticLock<TEntity>.NewValues(Func<TEntity, object[]> values)
        {
            _newValues = entity => values(entity as TEntity);

            return this;
        }
    }
}
