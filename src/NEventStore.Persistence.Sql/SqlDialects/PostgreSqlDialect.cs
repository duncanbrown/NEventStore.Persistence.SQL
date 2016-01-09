using System.Data;
using System.IO;
using System.Text;
using System.Transactions;

namespace NEventStore.Persistence.Sql.SqlDialects
{
    using System;

    public class PostgreSqlDialect : CommonSqlDialect
    {
        public override string InitializeStorage
        {
            get { return PostgreSqlStatements.InitializeStorage; }
        }

        public override string MarkCommitAsDispatched
        {
            get { return base.MarkCommitAsDispatched.Replace("1", "true"); }
        }

        public override string PersistCommit
        {
            get { return PostgreSqlStatements.PersistCommits; }
        }

        public override string GetUndispatchedCommits
        {
            get { return base.GetUndispatchedCommits.Replace("0", "false"); }
        }

        public override bool IsDuplicate(Exception exception)
        {
            string message = exception.Message.ToUpperInvariant();
            return message.Contains("23505") || message.Contains("IX_COMMITS_COMMITSEQUENCE");
        }

        public override void AddPayloadParamater(IConnectionFactory connectionFactory, IDbConnection connection, IDbStatement cmd,
            byte[] payload)
        {
            cmd.AddParameter(Payload, ToString(payload), DbType.Object);
        }

        public override void AddHeadersParameter(IConnectionFactory connectionFactory, IDbConnection connection, IDbStatement cmd,
            byte[] headers)
        {
            cmd.AddParameter(Headers, ToString(headers), DbType.Object);
        }

        public override IDbStatement BuildStatement(TransactionScope scope, IDbConnection connection, IDbTransaction transaction) =>
            new PostgreSqlDbStatement(this, scope, connection, transaction);

        private static string ToString(byte[] bs)
        {
            using (var sr = new StreamReader(new MemoryStream(bs), Encoding.UTF8))
                return sr.ReadToEnd();
        }
    }
}