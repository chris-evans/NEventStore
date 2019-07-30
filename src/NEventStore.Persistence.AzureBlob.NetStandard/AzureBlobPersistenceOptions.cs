using System;
using System.Text.RegularExpressions;

namespace NEventStore.Persistence.AzureBlob
{
    /// <summary>
    /// Holds options to be used initializing the engine.
    /// </summary>
    public class AzureBlobPersistenceOptions
    {
        /// <summary>
        /// Name of the container to be created/used.
        /// </summary>
        public string ContainerName
        { get; private set; }

        /// <summary>
        /// Gets if undispatched commits should be ignored.
        /// </summary>
        /// <remarks>
        /// The process of checking for undispatched commits in Azure Blob is slow today.  This does not need to be done constantly and instead
        /// can be done as a part of an offline processor rather than every time you are loading aggregates.
        /// </remarks>
        public bool LoadUndispatchedCommits
        { get; private set; }

        /// <summary>
        /// Get the maximum number of parallel rest connections that can be made to the blob storage at once.
        /// </summary>
        /// <remarks>
        /// this value is actually a .NET API limitor derived from the ServicePointManager class.  This value will
        /// update the ServicePointManager connection limit just for the blob storage URI requests.  No other system
        /// requests will be effected
        /// </remarks>
        public int ParallelConnectionLimit
        { get; set; }

        /// <summary>
        /// The number of pages a new blob will be created with.
        /// </summary>
        public int BlobNumPages
        { get; set; }

        /// <summary>
        /// Create a new AzureBlobPersistenceOptions
        /// </summary>
        /// <param name="containerName">name of the container within the azure storage account</param>
        /// <param name="containerType">type of container</param>
        /// <param name="parallelConnectionLimit">maximum parallel connection that can be made to the storage account at once</param>
        /// <param name="blobNumPages">the number of pages a new blob will be created with</param>
        public AzureBlobPersistenceOptions(
            string containerName = "default",
            int parallelConnectionLimit = 10,
            int blobNumPages = 2000)
            : this(true, containerName, parallelConnectionLimit, blobNumPages)
        { }

        /// <summary>
        /// Create a new AzureBlobPersistenceOptions
        /// </summary>
        /// <param name="loadUndispatchedCommits">maximum amount of history to go back in for looking for undispatched commits.  smaller values will imrpove performance, but increase the risk of missing a commit</param>
        /// <param name="containerName">name of the container within the azure storage account</param>
        /// <param name="containerType">typeof container</param>
        /// <param name="parallelConnectionLimit">maximum parallel connection that can be made to the storage account at once</param>
        public AzureBlobPersistenceOptions(
            bool loadUndispatchedCommits,
            string containerName = "default",
            int parallelConnectionLimit = 10,
            int blobNumPages = 2000)
        {
            // Only allow container names that begin with a lowercase letter and contains
            // 3 to 63 lowercase letters and numbers.
            containerName = containerName.ToLower();
            var containerRegex = new Regex(@"^[a-z][a-z0-9]{2,62}$");
            if (!containerRegex.Match(containerName).Success)
            {
                throw new ArgumentException(
                    String.Format(
                        "Container must start with lowercase letter, contain only lowercase letters and numbers, and have length between 3 and 63 characters. Failed container name was [{0}]"
                        , containerName));
            }
            ContainerName = containerName;
            ParallelConnectionLimit = parallelConnectionLimit;
            LoadUndispatchedCommits = loadUndispatchedCommits;
            BlobNumPages = blobNumPages;
        }
    }
}
