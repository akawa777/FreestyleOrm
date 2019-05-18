using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Linq;
using System.Collections;

namespace FreestyleOrm.Core
{
    internal class QueryOptions
    {
        public QueryOptions(IQueryDefine queryDefine, Type rootEntityType, bool isFlatFormat)
        {
            QueryDefine = queryDefine;
            _rootEnttiyType = rootEntityType;
            _setParams = parameters => { };
            _isFlatFormat = isFlatFormat;

            if (_isFlatFormat && !_rootEnttiyType.GetInterfaces().Any(x => x == typeof(IDictionary)))
            {
                throw new ArgumentException($"{nameof(rootEntityType)}({rootEntityType.FullName}) is invalid. {nameof(rootEntityType)} type is not inherited {typeof(IDictionary).FullName}.");
            }
        }

        public IQueryDefine QueryDefine { get; }
        private Type _rootEnttiyType;
        private Action<Dictionary<string, object>> _setParams;
        private bool _isFlatFormat;

        public IDbConnection Connection { get; set; }
        public IDbTransaction Transaction { get; set; }
        public string Sql { get; set; }
        public Action<Dictionary<string, object>> SetParams
        {
            get
            {
                return _setParams;
            }
            set
            {
                _setParams = parameters =>
                {   
                    value(parameters);
                };
            }
        }
        
        public Action<ITempTableSet> SetTempTables { get; set; } = t => { };

        public bool IsFlatFormat => _isFlatFormat;
    }
}
