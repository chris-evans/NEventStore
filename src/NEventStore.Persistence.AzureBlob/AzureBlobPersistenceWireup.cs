using System.Transactions;
using NEventStore.Logging;
using NEventStore.Serialization;

namespace NEventStore.Persistence.AzureBlob
{
    /// <summary>
    /// Wireup for the Azure blob persistence.
    /// </summary>
    public class AzureBlobPersistenceWireup : PersistenceWireup
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(AzureBlobPersistenceWireup));

        /// <summary>
        /// Create a new wireup.
        /// </summary>
        /// <param name="inner">The wireup to be used.</param>
        /// <param name="connectionString">The Azure blob storage connection string.</param>
        /// <param name="persistenceOptions">Options for the Azure blob storage.</param>
        public AzureBlobPersistenceWireup(Wireup inner, string connectionString, AzureBlobPersistenceOptions persistenceOptions)
            : base(inner)
        {
            Logger.Debug("Configuring Azure blob persistence engine.");

            var options = Container.Resolve<TransactionScopeOption>();
            if (options != TransactionScopeOption.Suppress)
            { Logger.Warn(Messages.TransactionScopeNotSupportedSettingIgnored); }

            Container.Register(c => new AzureBlobPersistenceFactory(connectionString, c.Resolve<ISerialize>(), persistenceOptions).Build());
        }
    }
}
