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
            _setFormats = formats => queryDefine.SetFormats(_rootEnttiyType, formats);
        }

        private IQueryDefine _queryDefine;
        private Type _rootEnttiyType;
        private Action<Dictionary<string, object>> _setFormats;

        public IDbConnection Connection { get; set; }
        public IDbTransaction Transaction { get; set; }
        public string Sql { get; set; }
        public Action<Dictionary<string, object>> SetParameters { get; set; } =pt => { };
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
                    value(formats);
                };
            }
        }
        public Action<Dictionary<string, TempTable>> SetTempTables { get; set; } = t => { };
    }
}
