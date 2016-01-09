using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Dapper;
using Newtonsoft.Json;
using NEventStore;
using NEventStore.Persistence.Sql.SqlDialects;
using Npgsql;

namespace Test
{
    class Foo
    {
        public int Id { get; set; }
        public string Value { get; set; }
    }


    class Program
    {
        static void Main(string[] args)
        {
            var store = Wireup
                .Init()
                .UsingSqlPersistence("db")
                .WithDialect(new PostgreSqlDialect())
                .InitializeStorageEngine()
                .Build();

            var id = Guid.NewGuid().ToString();

            //Insert some events
            using (var stream = store.OpenStream(id))
            {
                var events = Enumerable.Range(1, 10).Select(i => new EventMessage
                {
                    Body = new Foo {Id = i, Value = "Bar"},
                    Headers = new Dictionary<string, object>
                    {
                        ["UserName"] = i < 8 ? "Bob" : "John"
                    }
                });
                foreach (var e in events)
                {
                    stream.Add(e);
                    stream.CommitChanges(Guid.NewGuid());
                }
            }

            //Direct sql query of json
            using (var con = new NpgsqlConnection(ConfigurationManager.ConnectionStrings["db"].ConnectionString))
            {
                var countOfUpdatesByJohn = con
                    .ExecuteScalar<int>(@"
                        select count(1)
                        from commits, jsonb_array_elements(commits.payload) as events
                        where
                         streamidoriginal = @id
                         and
                         events->'Headers'->>'UserName' = 'John'", new { id });
            }

            //Load events directly, bypassing neventstore
            using (var con = new NpgsqlConnection(ConfigurationManager.ConnectionStrings["db"].ConnectionString))
            {
                var events = con
                    .Query<string>(@"
                        select payload
                        from commits
                        where
                         streamidoriginal = @id
                        order by streamrevision", new { id })
                    .SelectMany(json =>
                        JsonConvert.DeserializeObject<List<EventMessage>>(json, new JsonSerializerSettings
                        {
                            TypeNameHandling = TypeNameHandling.Auto,
                            DefaultValueHandling = DefaultValueHandling.Ignore,
                            MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead,
                            NullValueHandling = NullValueHandling.Ignore
                        }))
                    .ToList();
                
            }
        }
    }
}
