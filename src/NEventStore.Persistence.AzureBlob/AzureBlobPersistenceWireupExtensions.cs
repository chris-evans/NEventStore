namespace NEventStore.Persistence.AzureBlob
{
	public static class AzureBlobPersistenceWireupExtensions
	{
		/// <summary>
		/// Extenstion for the Azure blob persistence wireup.
		/// </summary>
		/// <param name="wireup">wireup being extended</param>
		/// <param name="azureConnectionString">the connection string for the azure storage</param>
		/// <param name="serializer">type of serializer to use</param>
		/// <param name="options">options for the azure persistence engine</param>
		/// <returns>An AzureBlobPersistenceWireup</returns>
		public static AzureBlobPersistenceWireup UsingAzureBlobPersistence(this Wireup wireup, string azureConnectionString, AzureBlobPersistenceOptions options = null)
		{ return new AzureBlobPersistenceWireup(wireup, azureConnectionString, options); }
	}
}
