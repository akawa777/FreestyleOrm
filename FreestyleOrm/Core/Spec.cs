using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;

namespace FreestyleOrm.Core
{
    internal class Spec : ISpec
    {
        private List<SpecPredicate> _specPredicates = new List<SpecPredicate>();
        public ISpecPredicate Predicate(string name, Func<string, string> formatPredicate = null)
        {
            var specPredicate = new SpecPredicate(name, formatPredicate);
            _specPredicates.Add(specPredicate);

            return specPredicate;
        }
        public void RemovePredicate(string name)
        {
            var specPredicates = _specPredicates.Where(x => x.Name == name);

            foreach (var specPredicate in specPredicates)
            {
                _specPredicates.Remove(specPredicate);
            }
        }

        public void SetFormats(Dictionary<string, object> formatMap)
        {
            foreach (var specPredicate in _specPredicates)
            {
                formatMap[specPredicate.Name] = specPredicate.GetSql();
            }
        }

        public void SetParams(Dictionary<string, object> paramMap)
        {
            foreach (var specPredicate in _specPredicates)
            {
                foreach (var entry in specPredicate.GetParams())
                {
                    paramMap[entry.Key] = entry.Value;
                }
            }
        }
    }

    internal class SpecPredicateResult
    {
        public string Sql { get; set; } = string.Empty;
        public Dictionary<string, object> Params { get; set; } = new Dictionary<string, object>();
    }

    internal class SpecPredicate : ISpecPredicate
    {   
        public SpecPredicate(string name, Func<string, string> formatPredicate = null)
        {
            Name = name;

            if (formatPredicate == null) _formatPredicate = x => x;
            else _formatPredicate = formatPredicate;
        }

        public string Name { get; }
        private Func<string, string> _formatPredicate;
        private List<SpecPredicateResult> _specPredicateResults = new List<SpecPredicateResult>();

        public string GetSql()
        {
            var sql = string.Join(Environment.NewLine, _specPredicateResults.Select(x => x.Sql));

            if (string.IsNullOrEmpty(sql)) return sql;
            else return _formatPredicate(sql);
        }

        public Dictionary<string, object> GetParams()
        {
            Dictionary<string, object> paramMap = new Dictionary<string, object>();

            foreach (var result in _specPredicateResults)
            {
                foreach (var entry in result.Params)
                {
                    paramMap[entry.Key] = entry.Value;
                }
            }

            return paramMap;
        }

        public bool ValidationParams(Dictionary<string, object> parameters)
        {
            foreach (var value in parameters.Values)
            {
                if (value == null) return false;
                if (value.ToString() == string.Empty) return false;    
                if (value.GetType() != typeof(string) && value is IEnumerable values)
                {
                    bool hasElement = false;
                    foreach (var element in values)
                    {
                        if (hasElement) break;
                        hasElement = true;
                    }

                    return hasElement;
                }
            }

            return true;
        }

        public ISpecPredicate Comma<T>(IEnumerable<T> list, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null, string defaultValue = null)
        {
            var result = new SpecPredicateResult();
            Func<bool> defaultValidation = () => list != null && list.Count() > 0;

            try
            {
                if ((validation == null && defaultValidation()) || (validation != null && validation()))
                {
                    result.Sql = string.Join(", ", list);

                    setParams?.Invoke(result.Params);

                    if (validation == null && !ValidationParams(result.Params))
                    {
                        if (defaultValue != null) result.Sql = defaultValue;
                        else return this;
                    }
                }
                else
                {   
                    if (defaultValue != null) result.Sql = defaultValue;
                }
            }
            catch(Exception e)
            {
                if (validation != null) throw e;
            }

            _specPredicateResults.Add(result);

            return this;
        }

        public ISpecPredicate Expression(LogicalSymbol logicalSymbol, string sql, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null, string defaultValue = null)
        {
            var result = new SpecPredicateResult();
            Func<bool> defaultValidation = () => !string.IsNullOrEmpty(sql);

            try
            {
                if ((validation == null && defaultValidation()) || validation())
                {
                    result.Sql = sql;

                    setParams?.Invoke(result.Params);

                    if ((validation == null && defaultValidation()) || (validation != null && validation()))
                    {
                        if (defaultValue != null) result.Sql = defaultValue;
                        else return this;
                    }
                }
                else
                {
                    if (defaultValue != null) result.Sql = defaultValue;
                }
            }
            catch (Exception e)
            {
                if (validation != null) throw e;
            }

            if (!string.IsNullOrEmpty(result.Sql) && _specPredicateResults.Count > 0)
            {
                if (logicalSymbol == LogicalSymbol.And)
                {
                    result.Sql = $" and {result.Sql}";
                }
                else
                {
                    result.Sql = $" or {result.Sql}";
                }
            }

            _specPredicateResults.Add(result);

            return this;
        }

        public ISpecPredicate Expression(LogicalSymbol logicalSymbol, Action<ISpecPredicate> setSpecPredicate)
        {
            var result = new SpecPredicateResult();
            Func<bool> defaultValidation = () => setSpecPredicate == null;

            var specPredicate = new SpecPredicate(Name);
            setSpecPredicate(specPredicate);

            result.Sql = $"({specPredicate.GetSql()})";
            result.Params = specPredicate.GetParams();

            if (specPredicate != null && _specPredicateResults.Count > 0)
            {
                if (logicalSymbol == LogicalSymbol.And)
                {
                    result.Sql = $" and {result.Sql}";
                }
                else
                {
                    result.Sql = $" or {result.Sql}";
                }
            }

            _specPredicateResults.Add(result);

            return this;
        }

        public ISpecPredicate Sort<T>(IEnumerable<T> list, Func<T, int, bool> isDesc = null, string defaultValue = null, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null)
        {
            var result = new SpecPredicateResult();
            Func<bool> defaultValidation = () => list != null && list.Count() > 0;
            Func<T, int, object> setColumn = (x, i) =>
            {
                if (isDesc != null && isDesc(x, i))
                {
                    return $"{x} desc";
                }
                else
                {
                    return x;
                }
            };

            try
            {                
                if ((validation == null && defaultValidation()) || (validation != null && validation()))
                {
                    result.Sql = string.Join(", ", list.Select((x, i) => setColumn(x, i)));

                    setParams?.Invoke(result.Params);

                    if (validation == null && !ValidationParams(result.Params))
                    {
                        if (defaultValue != null) result.Sql = defaultValue;
                        else return this;
                    }
                }
                else
                {
                    if (defaultValue != null) result.Sql = defaultValue;
                }
            }
            catch (Exception e)
            {
                if (validation != null) throw e;
            }

            _specPredicateResults.Add(result);

            return this;
        }
    }
}
