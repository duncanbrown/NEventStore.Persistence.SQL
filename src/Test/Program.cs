using System;
using System.Collections.Generic;
using NEventStore;
using NEventStore.Persistence.Sql.SqlDialects;

namespace Test
{
    class Foo
    {
        public int Id { get; set; }
        public string Name { get; set; }
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

            using (var stream = store.OpenStream("Foo", "Bar", 0, int.MaxValue))
            {
                
                stream.Add(new EventMessage
                {
                    Body = new Foo { Id = 27, Name = "Bob" },
                    Headers = new Dictionary<string, object>
                    {
                        ["Baz"] = "Quux"
                    }
                });
                stream.CommitChanges(Guid.NewGuid());
            }
        }
    }
}
