using NEventStore.Serialization;

namespace NEventStore.Persistence.AzureBlob
{
    /// <summary>
    /// Class to create the Azure blob persistence engine.
    /// </summary>
    public class AzureBlobPersistenceFactory : IPersistenceFactory
    {
        private readonly string _connectionString;
        private readonly ISerialize _serializer;
        private readonly AzureBlobPersistenceOptions _options;

        /// <summary>
        /// Creates a new factory.
        /// </summary>
        /// <param name="connectionString">The Azure blob storage connection string.</param>
        /// <param name="serializer">The serializer to use.</param>
        /// <param name="options">Options for the Azure blob storage.</param>
        public AzureBlobPersistenceFactory(string connectionString, ISerialize serializer, AzureBlobPersistenceOptions options = null)
        {
            _connectionString = connectionString;
            _serializer = serializer;
            _options = options ?? new AzureBlobPersistenceOptions();
        }

        /// <summary>
        /// Builds a new persistence engine.
        /// </summary>
        /// <returns></returns>
        public IPersistStreams Build()
        { return new AzureBlobPersistenceEngine(_connectionString, _serializer, _options); }
    }
}
