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

            _setParams = parameters => { };
        }

        private IQueryDefine _queryDefine;
        private Type _rootEnttiyType;
        private Action<Dictionary<string, object>> _setParams;        

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
    }
}
