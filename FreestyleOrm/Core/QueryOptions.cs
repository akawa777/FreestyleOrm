using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace FreestyleOrm.Core
{
    internal class QueryOptions
    {
        public QueryOptions(IQueryDefine queryDefine, Type rootEntityType)
        {
            _queryDefine = queryDefine;
            _rootEnttiyType = rootEntityType;
            _setFormats = formats =>
            {                
                queryDefine.SetFormats(_rootEnttiyType, formats);
                Spec.SetFormats(formats);
            };
            _setParams = parameters =>
            {
                Spec.SetParams(parameters);
            };
        }

        private IQueryDefine _queryDefine;
        private Type _rootEnttiyType;
        private Action<Dictionary<string, object>> _setParams;
        private Action<Dictionary<string, object>> _setFormats;

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
                    Spec.SetParams(parameters);
                    value(parameters);
                };
            }
        }
        public Action<Dictionary<string, object>> SetFormats
        {
            get
            {
                return _setFormats;
            }
            set
            {
                _setFormats = formats =>
                {
                    _queryDefine.SetFormats(_rootEnttiyType, formats);
                    Spec.SetFormats(formats);
                    value(formats);
                };
            }
        }
        public Action<ITempTableSet> SetTempTables { get; set; } = t => { };
        public Spec Spec { get; set; } = new Spec();
    }
}
