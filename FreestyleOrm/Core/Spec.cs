﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;

namespace FreestyleOrm.Core
{
    public interface ISqlSpec : IDictionary<string, object>
    {        
        
    }

    public abstract class SpecBase : Dictionary<string, object>, ISqlSpec
    {
        protected SpecBase(Action<Dictionary<string, object>> setParams, Func<bool> validation)
        {
            _validation = validation;
            ValidParam = TrySetParams(setParams, validation);
        }
        
        protected Func<bool> _validation;
        protected bool ValidParam = false;

        protected bool TrySetParams(Action<Dictionary<string, object>> setParams, Func<bool> validation)
        {
            if (setParams == null)
            {
                ValidParam = true;
                return true;
            }

            try
            {
                Dictionary<string, object> map = new Dictionary<string, object>();

                setParams(map);

                bool rtn = true;

                if (validation!= null && !validation())
                {
                    rtn = false;
                }
                if (validation == null)
                {                    
                    foreach (var value in map.Values)
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

                            rtn = hasElement;
                        }
                    }
                }

                if (rtn)
                {
                    ValidParam = true;
                    this.AddMap(map);
                }

                return rtn;
            }
            catch
            {
                return false;
            }
        }
    }

    public class IfSpec : SpecBase
    {
        public IfSpec(string predicate, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null, string defaultPredicate = null) : base(setParams, validation)
        {
            Init(predicate, setParams, validation, defaultPredicate);
        }

        private string _predicate = string.Empty;        

        private void Init(string predicate, Action<Dictionary<string, object>> setParams = null, Func<bool> validation = null, string defaultPredicate = null)
        {
            if ((validation != null && !validation()) || !ValidParam)
            {
                if (string.IsNullOrEmpty(defaultPredicate))
                {
                    _predicate = string.Empty;
                }
                else
                {
                    _predicate = defaultPredicate;
                }
            }
            else
            {
                _predicate = predicate;
            }
        }        

        public override string ToString()
        {
            return _predicate;
        }
    }

    public class LogicalSpec : SpecBase
    {
        public LogicalSpec(string symbol = null, string prefixHolder = null, string suffixHolder = null, Action<Dictionary<string, object>> setParams = null, params ISqlSpec[] specs) : base(setParams, null)
        {
            _symbol = symbol ?? string.Empty;
            _prefixHolder = prefixHolder ?? string.Empty;
            _suffixHolder = suffixHolder ?? string.Empty;
            Specs = specs;

            if (ValidParam)
            {
                List<string> predicates = new List<string>();

                foreach (var spec in Specs)
                {
                    if (string.IsNullOrEmpty(spec.ToString())) continue;

                    predicates.Add($"{_prefixHolder}{spec.ToString()}{_suffixHolder}");
                    this.AddMap(spec);
                }

                _predicate = string.Join(_symbol, predicates);
            }
        }

        private string _symbol;
        private string _prefixHolder;
        private string _suffixHolder;
        protected ISqlSpec[] Specs { get; set; }
        private string _predicate;        

        public override string ToString()
        {
            return _predicate;
        }
    }

    public class AndSpec : LogicalSpec
    {
        public AndSpec(params ISqlSpec[] specs) : base("and ", "( ", ") ", specs: specs)
        {

        }
    }

    public class OrSpec : LogicalSpec
    {
        public OrSpec(params ISqlSpec[] specs) : base("or ", "(", ") ", specs: specs)
        {

        }
    }

    public class WhereSpec : LogicalSpec
    {
        public WhereSpec(params ISqlSpec[] specs) : base(string.Empty, "where ", string.Empty, specs: specs)
        {

        }
    }

    public class SelectSpec : LogicalSpec
    {
        public SelectSpec(IEnumerable<object> items, Action<Dictionary<string, object>> setParams = null, string defaultPredicate = null) 
            : base(", ", string.Empty, string.Empty, setParams, specs: CreateSpecs(items, defaultPredicate))
        {

        }        

        private static ISqlSpec[] CreateSpecs(IEnumerable<object> items, string defaultPredicate)
        {
            var specs = new List<ISqlSpec>();

            if (items == null || items.Count() == 0)
            {
                if (!string.IsNullOrEmpty(defaultPredicate))
                {
                    specs.Add(new IfSpec(defaultPredicate));
                }
            }
            else
            {
                foreach (var item in items)
                {
                    specs.Add(new IfSpec(item.ToString()));
                }
            }

            return specs.ToArray();
        }
    }

    public class SortSpec : SelectSpec
    {
        public SortSpec(IEnumerable<object> items, Action<Dictionary<string, object>> setParams = null, string defaultPredicate = null) 
            : base(items, setParams, defaultPredicate)
        {

        }
    }
}
