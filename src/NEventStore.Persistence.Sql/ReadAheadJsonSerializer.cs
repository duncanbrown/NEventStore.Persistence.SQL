using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using NEventStore.Logging;
using NEventStore.Serialization;

namespace NEventStore.Persistence.Sql
{
    public class ReadAheadJsonSerializer : ISerialize
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (ReadAheadJsonSerializer));

        private readonly IEnumerable<Type> _knownTypes = new[] {typeof (List<EventMessage>), typeof (Dictionary<string, object>)};

        private readonly Newtonsoft.Json.JsonSerializer _typedSerializer = new Newtonsoft.Json.JsonSerializer
        {
            TypeNameHandling = TypeNameHandling.All,
            DefaultValueHandling = DefaultValueHandling.Ignore,
            MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead,
            NullValueHandling = NullValueHandling.Ignore
        };

        private readonly Newtonsoft.Json.JsonSerializer _untypedSerializer = new Newtonsoft.Json.JsonSerializer
        {
            TypeNameHandling = TypeNameHandling.Auto,
            DefaultValueHandling = DefaultValueHandling.Ignore,
            MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead,
            NullValueHandling = NullValueHandling.Ignore
        };

        public ReadAheadJsonSerializer(params Type[] knownTypes)
        {
            if (knownTypes != null && knownTypes.Length == 0)
            {
                knownTypes = null;
            }

            _knownTypes = knownTypes ?? _knownTypes;

            foreach (var type in _knownTypes)
            {
                Logger.Debug(GetNEventStoreResource("RegisteringKnownType"), type);
            }
        }

        public virtual void Serialize<T>(Stream output, T graph)
        {
            Logger.Verbose(GetNEventStoreResource("SerializingGraph"), typeof (T));
            using (var streamWriter = new StreamWriter(output, Encoding.UTF8))
                Serialize(new JsonTextWriter(streamWriter), (object) graph);
        }

        public virtual T Deserialize<T>(Stream input)
        {
            Logger.Verbose(GetNEventStoreResource("DeserializingStream"), typeof (T));
            using (var streamReader = new StreamReader(input, Encoding.UTF8))
                return Deserialize<T>(new JsonTextReader(streamReader));
        }

        protected virtual void Serialize(JsonWriter writer, object graph)
        {
            using (writer)
                GetSerializer(graph.GetType()).Serialize(writer, graph);
        }

        protected virtual T Deserialize<T>(JsonReader reader)
        {
            Type type = typeof (T);

            using (reader)
                return (T) GetSerializer(type).Deserialize(reader, type);
        }

        protected virtual Newtonsoft.Json.JsonSerializer GetSerializer(Type typeToSerialize)
        {
            if (_knownTypes.Contains(typeToSerialize))
            {
                Logger.Verbose(GetNEventStoreResource("UsingUntypedSerializer"), typeToSerialize);
                return _untypedSerializer;
            }

            Logger.Verbose(GetNEventStoreResource("UsingTypedSerializer"), typeToSerialize);
            return _typedSerializer;
        }

        private static string GetNEventStoreResource(string name) =>
            Type.GetType("NEventStore.Serialization.Messages, NEventStore")
                .GetProperty(name, BindingFlags.NonPublic | BindingFlags.Static)
                .GetValue(null) as string;
    }
}