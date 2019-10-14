namespace NEventStore
{
    using NEventStore.Serialization;
    using System.Runtime.Serialization.Formatters;

    public static class SerializationWireupExtensions
    {
        /// <summary>
        /// ueses BinaryFormatter for serialization
        /// </summary>
        /// <param name="assemblyFormat">set to simple for version agnostic deserialization</param>
        public static SerializationWireup UsingBinarySerialization(this PersistenceWireup wireup, FormatterAssemblyStyle assemblyFormat = FormatterAssemblyStyle.Full)
        {
            return wireup.UsingCustomSerialization(new BinarySerializer(assemblyFormat));
        }

        public static SerializationWireup UsingCustomSerialization(this PersistenceWireup wireup, ISerialize serializer)
        {
            return new SerializationWireup(wireup, serializer);
        }

        public static SerializationWireup UsingJsonSerialization(this PersistenceWireup wireup)
        {
            return wireup.UsingCustomSerialization(new JsonSerializer());
        }

        public static SerializationWireup UsingBsonSerialization(this PersistenceWireup wireup)
        {
            return wireup.UsingCustomSerialization(new BsonSerializer());
        }
    }
}