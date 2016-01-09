using System.Data;
using System.Transactions;
using Npgsql;
using NpgsqlTypes;

namespace NEventStore.Persistence.Sql.SqlDialects
{
    public class PostgreSqlDbStatement : CommonDbStatement
    {
        private readonly ISqlDialect _dialect;

        public PostgreSqlDbStatement(ISqlDialect dialect, TransactionScope scope, IDbConnection connection,
            IDbTransaction transaction) : base(dialect, scope, connection, transaction)
        {
            _dialect = dialect;
        }

        protected override void SetParameterValue(IDataParameter param, object value, DbType? type)
        {
            base.SetParameterValue(param, value, type);
            if (param.ParameterName == _dialect.Payload || param.ParameterName == _dialect.Headers)
                ((NpgsqlParameter)param).NpgsqlDbType = NpgsqlDbType.Jsonb;
        }
    }
}