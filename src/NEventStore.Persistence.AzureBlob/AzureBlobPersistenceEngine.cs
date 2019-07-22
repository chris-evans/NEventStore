using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NEventStore.Logging;
using NEventStore.Serialization;

namespace NEventStore.Persistence.AzureBlob
{
    /// <summary>
    /// Main engine for using Azure blob storage for event sourcing.
    /// The general pattern is that all commits for a given stream live within a single page blob.
    /// As new commits are added, they are placed at the end of the page blob.
    /// </summary>
    public class AzureBlobPersistenceEngine : IPersistStreams
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(AzureBlobPersistenceEngine));
        private static int _connectionLimitSet;

        private const string _eventSourcePrefix = "evsrc";
        private const string _rootContainerName = "$root";
        private const string _checkpointBlobName = "checkpoint";
        private const string _hasUndispatchedCommitsKey = "hasUndispatchedCommits";
        private const string _isEventStreamAggregateKey = "isEventStreamAggregate";
        private const string _firstWriteCompletedKey = "firstWriteCompleted";

        // because of the two phase commit nature of this system, we always work with two header definitions.  the primary
        // is the header definition that is what we hope will be correct.  The fallback is there for the case where we write the
        // definition, but fail to write the header correctly.
        private const string _primaryHeaderDefinitionKey = "primaryHeaderDefinition";
        private const string _secondaryHeaderDefinitionKey = "fallbackHeaderDefinition";
        private const string _terciaryHeaderDefintionKey = "terciaryHeaderDefintionKey";

        private const int _blobPageSize = 512;
        private readonly ISerialize _serializer;
        private readonly AzureBlobPersistenceOptions _options;
        private readonly CloudBlobClient _blobClient;
        private readonly CloudTableClient _checkpointTableClient;
        private readonly CloudBlobContainer _primaryContainer;
        private readonly string _connectionString;
        private int _initialized;
        private bool _disposed;

        /// <summary>
        /// Create a new engine.
        /// </summary>
        /// <param name="connectionString">The Azure blob storage connection string.</param>
        /// <param name="serializer">The serializer to use.</param>
        /// <param name="options">Options for the Azure blob storage.</param>
        public AzureBlobPersistenceEngine(string connectionString, ISerialize serializer, AzureBlobPersistenceOptions options = null)
        {
            if (String.IsNullOrEmpty(connectionString))
            { throw new ArgumentException("connectionString cannot be null or empty"); }
            if (serializer == null)
            { throw new ArgumentNullException("serializer"); }
            if (options == null)
            { throw new ArgumentNullException("options"); }

            _serializer = serializer;
            _options = options;

            _connectionString = connectionString;
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            _blobClient = storageAccount.CreateCloudBlobClient();
            _checkpointTableClient = storageAccount.CreateCloudTableClient();
            _primaryContainer = _blobClient.GetContainerReference(GetContainerName());
        }

        /// <summary>
        /// Is the engine disposed?
        /// </summary>
        public bool IsDisposed
        { get { return _disposed; } }

        /// <summary>
        /// Connect to Azure storage and get a reference to the container object,
        /// creating it if it does not exist.
        /// </summary>
        public void Initialize()
        {
            // we want to increase the connection limit used to communicate with via HTTP rest
            // calls otherwise we will feel significant performance degradation.  This can also
            // be modified via configuration but that would be a very leaky concern to the
            // application developer.
            if (Interlocked.Increment(ref _connectionLimitSet) < 2)
            {
                Uri uri = new Uri(_primaryContainer.Uri.AbsoluteUri);
                var sp = ServicePointManager.FindServicePoint(uri);
                sp.ConnectionLimit = _options.ParallelConnectionLimit;

                // make sure we have a checkpoint aggregate
                var blobContainer = _blobClient.GetContainerReference(_rootContainerName);
                blobContainer.CreateIfNotExists();

                var pageBlobReference = blobContainer.GetPageBlobReference(_checkpointBlobName);
                try
                { pageBlobReference.Create(512, accessCondition: AccessCondition.GenerateIfNoneMatchCondition("*")); }
                catch (Microsoft.WindowsAzure.Storage.StorageException ex)
                {
                    // 409 means it was already there
                    if (!ex.Message.Contains("409"))
                    { throw; }
                }
            }

            if (Interlocked.Increment(ref _initialized) < 2)
            { _primaryContainer.CreateIfNotExists(); }
        }

        /// <summary>
        /// Not Implemented.
        /// </summary>
        /// <param name="checkpointToken"></param>
        /// <returns></returns>
        public ICheckpoint GetCheckpoint(string checkpointToken = null)
        { throw new NotImplementedException(); }

        /// <summary>
        /// Gets the list of commits from a given blobEntry, starting from a given date
        /// until the present.
        /// </summary>
        /// <param name="bucketId">The blobEntry id to pull commits from.</param>
        /// <param name="start">The starting date for commits.</param>
        /// <returns>The list of commits from the given blobEntry and greater than or equal to the start date.</returns>
        public IEnumerable<ICommit> GetFrom(string bucketId, DateTime start)
        { return GetFromTo(bucketId, start, DateTime.MaxValue); }

        /// <summary>
        /// Ordered fetch by checkpoint
        /// </summary>
        /// <param name="checkpointToken"></param>
        /// <returns>this method will get very slow as the number of aggregates increase</returns>
        public IEnumerable<ICommit> GetFrom(string checkpointToken = null)
        {
            var containers = _blobClient.ListContainers(_eventSourcePrefix);
            var allCommitDefinitions = new List<Tuple<WrappedPageBlob, PageBlobCommitDefinition>>();

            foreach (var container in containers)
            {
                var blobs = WrappedPageBlob.GetAllMatchinPrefix(container, GetContainerName());

                // this could be a tremendous amount of data.  Depending on system used
                // this may not be performant enough and may require some sort of index be built.
                foreach (var pageBlob in blobs)
                {
                    HeaderDefinitionMetadata headerDefinitionMetadata = null;
                    var header = GetHeaderWithRetry(pageBlob, out headerDefinitionMetadata);
                    foreach (var definition in header.PageBlobCommitDefinitions)
                    { allCommitDefinitions.Add(new Tuple<WrappedPageBlob, PageBlobCommitDefinition>(pageBlob, definition)); }
                }
            }

            // now sort the definitions so we can return out sorted
            var orderedCommitDefinitions = allCommitDefinitions.OrderBy((x) => x.Item2.Checkpoint);
            foreach (var orderedCommitDefinition in orderedCommitDefinitions)
            { yield return CreateCommitFromDefinition(orderedCommitDefinition.Item1, orderedCommitDefinition.Item2); }
        }

        /// <summary>
        /// Gets the list of commits from a given blobEntry, starting from a given date
        /// until the end date.
        /// </summary>
        /// <param name="bucketId">The blobEntry id to pull commits from.</param>
        /// <param name="start">The starting date for commits.</param>
        /// <param name="end">The ending date for commits.</param>
        /// <returns>The list of commits from the given blobEntry and greater than or equal to the start date and less than or equal to the end date.</returns>
        public IEnumerable<ICommit> GetFromTo(string bucketId, DateTime start, DateTime end)
        {
            var pageBlobs = WrappedPageBlob.GetAllMatchinPrefix(_primaryContainer, GetContainerName() + "/" + bucketId);

            // this could be a tremendous amount of data.  Depending on system used
            // this may not be performant enough and may require some sort of index be built.
            var allCommitDefinitions = new List<Tuple<WrappedPageBlob, PageBlobCommitDefinition>>();
            foreach (var pageBlob in pageBlobs)
            {
                HeaderDefinitionMetadata headerDefinitionMetadata = null;
                var header = GetHeaderWithRetry(pageBlob, out headerDefinitionMetadata);
                foreach (var definition in header.PageBlobCommitDefinitions)
                {
                    if (definition.CommitStampUtc >= start && definition.CommitStampUtc <= end)
                    { allCommitDefinitions.Add(new Tuple<WrappedPageBlob, PageBlobCommitDefinition>(pageBlob, definition)); }
                }
            }

            // now sort the definitions so we can return out sorted
            var orderedCommitDefinitions = allCommitDefinitions.OrderBy((x) => x.Item2.CommitStampUtc);
            foreach (var orderedCommitDefinition in orderedCommitDefinitions)
            { yield return CreateCommitFromDefinition(orderedCommitDefinition.Item1, orderedCommitDefinition.Item2); }
        }

        /// <summary>
        /// Gets commits from a given blobEntry and stream id that fall within min and max revisions.
        /// </summary>
        /// <param name="bucketId">The blobEntry id to pull from.</param>
        /// <param name="streamId">The stream id.</param>
        /// <param name="minRevision">The minimum revision.</param>
        /// <param name="maxRevision">The maximum revision.</param>
        /// <returns></returns>
        public IEnumerable<ICommit> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
        {
            var pageBlob = WrappedPageBlob.CreateNewIfNotExists(_primaryContainer, bucketId + "/" + streamId, _options.BlobNumPages);

            HeaderDefinitionMetadata headerDefinitionMetadata = null;
            var header = GetHeaderWithRetry(pageBlob, out headerDefinitionMetadata);

            // find out how many pages we are reading
            int startPage = 0;
            int endPage = startPage + 1;
            int startIndex = 0;
            int numberOfCommits = 0;
            foreach (var commitDefinition in header.PageBlobCommitDefinitions)
            {
                if (minRevision > commitDefinition.Revision)
                {
                    ++startIndex;
                    startPage += commitDefinition.TotalPagesUsed;
                }
                else if (maxRevision < commitDefinition.Revision)
                { break; }
                else
                { ++numberOfCommits; }

                endPage += commitDefinition.TotalPagesUsed;
            }

            // download all the data
            var downloadedData = pageBlob.DownloadBytes(startPage * _blobPageSize, endPage * _blobPageSize);

            // process the downloaded data
            var commits = new List<ICommit>();
            for (var i = startIndex; i != startIndex + numberOfCommits; i++)
            {
                var commitStartIndex = (header.PageBlobCommitDefinitions[i].StartPage - startPage) * _blobPageSize;
                var commitSize = header.PageBlobCommitDefinitions[i].DataSizeBytes;
                using (var ms = new MemoryStream(downloadedData, commitStartIndex, commitSize, false))
                {
                    var commit = _serializer.Deserialize<AzureBlobCommit>(ms);
                    commits.Add(CreateCommitFromAzureBlobCommit(commit));
                }
            }

            return commits;
        }

        /// <summary>
        /// Gets all undispatched commits across all buckets.
        /// </summary>
        /// <returns>A list of all undispatched commits.</returns>
        public IEnumerable<ICommit> GetUndispatchedCommits()
        {
            if (!_options.LoadUndispatchedCommits)
            {
                Logger.Info("Flag set to ignore undispatched commits.  Make sure you have a process to dispatch occasionally of event not being dispatched");
                yield break;
            }

            Logger.Info("Getting undispatched commits.  This is only done during initialization.  This may take a while...");
            var allCommitDefinitions = new List<Tuple<WrappedPageBlob, PageBlobCommitDefinition>>();

            // this container is fetched lazily.  so actually filtering down at this level will improve our performance,
            // assuming the options dictate a date range that filters down our set.
            var pageBlobs = WrappedPageBlob.GetAllMatchinPrefix(_primaryContainer, null);
            Logger.Info("Checking [{0}] blobs for undispatched commits... this may take a while", pageBlobs.Count());

            // this could be a tremendous amount of data.  Depending on system used
            // this may not be performant enough and may require some sort of index be built.
            foreach (var pageBlob in pageBlobs)
            {
                var temp = pageBlob;
                if (temp.Metadata.ContainsKey(_isEventStreamAggregateKey))
                {
                    // we only care about guys who may be dirty
                    bool isDirty = false;
                    string isDirtyString;
                    if (temp.Metadata.TryGetValue(_hasUndispatchedCommitsKey, out isDirtyString))
                    { isDirty = Boolean.Parse(isDirtyString); }

                    if (isDirty)
                    {
                        Logger.Info("undispatched commit possibly found with aggregate [{0}]", temp.Name);

                        // Because fetching the header for a specific blob is a two phase operation it may take a couple tries if we are working with the
                        // blob.  This is just a quality of life improvement for the user of the store so loading of the store does not hit frequent optimistic
                        // concurrency hits that cause the store to have to re-initialize.
                        var maxTries = 0;
                        while (true)
                        {
                            try
                            {
                                HeaderDefinitionMetadata headerDefinitionMetadata = null;
                                var header = GetHeaderWithRetry(temp, out headerDefinitionMetadata);
                                bool wasActuallyDirty = false;
                                if (header.UndispatchedCommitCount > 0)
                                {
                                    foreach (var definition in header.PageBlobCommitDefinitions)
                                    {
                                        if (!definition.IsDispatched)
                                        {
                                            Logger.Warn("Found undispatched commit for stream [{0}] revision [{1}]", temp.Name, definition.Revision);
                                            wasActuallyDirty = true;
                                            allCommitDefinitions.Add(new Tuple<WrappedPageBlob, PageBlobCommitDefinition>(temp, definition));
                                        }
                                    }
                                }

                                if (!wasActuallyDirty)
                                {
                                    temp.Metadata[_hasUndispatchedCommitsKey] = false.ToString();
                                    temp.SetMetadata();
                                }

                                break;
                            }
                            catch (ConcurrencyException)
                            {
                                if (maxTries++ > 20)
                                {
                                    Logger.Error("Reached max tries for getting undispatched commits and keep receiving concurrency exception.  throwing out.");
                                    throw;
                                }
                                else
                                {
                                    Logger.Info("Concurrency issue detected while processing undispatched commits.  going to retry to load container");
                                    try
                                    { temp = WrappedPageBlob.GetAllMatchinPrefix(_primaryContainer, pageBlob.Name).Single(); }
                                    catch (Exception ex)
                                    { Logger.Warn("Attempted to reload during concurrency and failed... will retry.  [{0}]", ex.Message); }
                                }
                            }
                            catch (CryptographicException ex)
                            {
                                Logger.Fatal("Received a CryptographicException while processing aggregate with id [{0}].  The header is possibly be corrupt.  Error is [{1}]",
                                    pageBlob.Name, ex.ToString());

                                break;
                            }
                            catch (InvalidHeaderDataException ex)
                            {
                                Logger.Fatal("Received a InvalidHeaderDataException while processing aggregate with id [{0}].  The header is possibly be corrupt.  Error is [{1}]",
                                    pageBlob.Name, ex.ToString());

                                break;
                            }
                        }
                    }
                }
            }

            // now sort the definitions so we can return out sorted
            Logger.Info("Found [{0}] undispatched commits", allCommitDefinitions.Count);
            var orderedCommitDefinitions = allCommitDefinitions.OrderBy((x) => x.Item2.Checkpoint);
            foreach (var orderedCommitDefinition in orderedCommitDefinitions)
            { yield return CreateCommitFromDefinition(orderedCommitDefinition.Item1, orderedCommitDefinition.Item2); }
        }

        /// <summary>
        /// Marks a stream Id's commit as dispatched.
        /// </summary>
        /// <param name="commit">The commit object to mark as dispatched.</param>
        public void MarkCommitAsDispatched(ICommit commit)
        {
            AddCheckpointTableEntry(commit);

            var pageBlob = WrappedPageBlob.GetAssumingExists(_primaryContainer, commit.BucketId + "/" + commit.StreamId);
            HeaderDefinitionMetadata headerDefinition = null;
            var header = GetHeaderWithRetry(pageBlob, out headerDefinition);

            // we must commit at a page offset, we will just track how many pages in we must start writing at
            foreach (var commitDefinition in header.PageBlobCommitDefinitions)
            {
                if (commit.CommitId == commitDefinition.CommitId)
                {
                    commitDefinition.IsDispatched = true;
                    --header.UndispatchedCommitCount;
                }
            }

            CommitNewMessage(pageBlob, null, header, headerDefinition,
                headerDefinition.HeaderStartLocationOffsetBytes);
        }

        /// <summary>
        /// Purge a container.
        /// </summary>
        public void Purge()
        { _primaryContainer.Delete(); }

        /// <summary>
        /// Purge a series of streams
        /// </summary>
        /// <param name="bucketId"></param>
        public void Purge(string bucketId)
        {
            var blobs = _primaryContainer.ListBlobs(GetContainerName() + "/" + bucketId, true, BlobListingDetails.Metadata).OfType<CloudPageBlob>();
            foreach (var blob in blobs)
            { blob.Delete(); }
        }

        /// <summary>
        /// Drop a container.
        /// </summary>
        public void Drop()
        { _primaryContainer.Delete(); }

        /// <summary>
        /// Deletes a stream by blobEntry and stream id.
        /// </summary>
        /// <param name="bucketId">The blobEntry id.</param>
        /// <param name="streamId">The stream id.</param>
        public void DeleteStream(string bucketId, string streamId)
        {
            var pageBlobReference = _primaryContainer.GetPageBlobReference(bucketId + "/" + streamId);
            string leaseId = pageBlobReference.AcquireLease(new TimeSpan(0, 0, 60), null);
            pageBlobReference.Delete(accessCondition: AccessCondition.GenerateLeaseCondition(leaseId));
            pageBlobReference.ReleaseLease(AccessCondition.GenerateLeaseCondition(leaseId));
        }

        /// <summary>
        /// Disposes this object.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || _disposed)
            { return; }

            Logger.Debug("Disposing...");
            _disposed = true;
        }

        /// <summary>
        /// Adds a commit to a stream.
        /// </summary>
        /// <param name="attempt">The commit attempt to be added.</param>
        /// <returns>An Commit if successful.</returns>
        public ICommit Commit(CommitAttempt attempt)
        {
            var pageBlob = WrappedPageBlob.CreateNewIfNotExists(_primaryContainer, attempt.BucketId + "/" + attempt.StreamId, _options.BlobNumPages);
            HeaderDefinitionMetadata headerDefinitionMetadata = null;
            var header = GetHeaderWithRetry(pageBlob, out headerDefinitionMetadata);

            // we must commit at a page offset, we will just track how many pages in we must start writing at
            var startPage = 0;
            foreach (var commit in header.PageBlobCommitDefinitions)
            {
                if (commit.CommitId == attempt.CommitId)
                { throw new DuplicateCommitException("Duplicate Commit Attempt"); }

                startPage += commit.TotalPagesUsed;
            }

            if (attempt.CommitSequence <= header.LastCommitSequence)
            { throw new ConcurrencyException("Concurrency exception in Commit"); }

            var blobCommit = new AzureBlobCommit();
            blobCommit.BucketId = attempt.BucketId;
            blobCommit.CommitId = attempt.CommitId;
            blobCommit.CommitSequence = attempt.CommitSequence;
            blobCommit.CommitStampUtc = attempt.CommitStamp;
            blobCommit.Events = attempt.Events.ToList();
            blobCommit.Headers = attempt.Headers;
            blobCommit.StreamId = attempt.StreamId;
            blobCommit.StreamRevision = attempt.StreamRevision;
            blobCommit.Checkpoint = GetNextCheckpoint();
            var serializedBlobCommit = _serializer.Serialize(blobCommit);

            header.AppendPageBlobCommitDefinition(new PageBlobCommitDefinition(serializedBlobCommit.Length, attempt.CommitId, attempt.StreamRevision,
                attempt.CommitStamp, header.PageBlobCommitDefinitions.Count, startPage, blobCommit.Checkpoint));
            ++header.UndispatchedCommitCount;
            header.LastCommitSequence = attempt.CommitSequence;

            CommitNewMessage(pageBlob, serializedBlobCommit, header, headerDefinitionMetadata, startPage * _blobPageSize);
            return CreateCommitFromAzureBlobCommit(blobCommit);
        }

        /// <summary>
        /// Gets the snapshot for the given stream/bucket
        /// </summary>
        /// <param name="bucketId"></param>
        /// <param name="streamId"></param>
        /// <param name="maxRevision"></param>
        /// <returns></returns>
        public ISnapshot GetSnapshot(string bucketId, string streamId, int maxRevision)
        {
            ISnapshot snapshot = null;
            var snapshotPageBlob = WrappedPageBlob.GetAssumingExists(_primaryContainer, bucketId + "/ss/" + streamId);
            if (snapshotPageBlob != null)
            {
                var snapshotSize = Convert.ToInt32(snapshotPageBlob.Metadata["ss_data_size_bytes"]);
                var snapshotRevision = Convert.ToInt32(snapshotPageBlob.Metadata["ss_stream_revision"]);

                if (snapshotRevision <= maxRevision && snapshotSize != 0)
                {
                    var snapshotBytes = snapshotPageBlob.DownloadBytes(0, snapshotSize);
                    var snapshotObject = _serializer.Deserialize<object>(snapshotBytes);
                    snapshot = new Snapshot(bucketId, streamId, snapshotRevision, snapshotObject);
                }
                else
                {
                    Logger.Info("Snapshot exists for stream [{0}] at revision [{1}] but not being returned because max revision desired was [{2}]",
                        streamId, snapshotRevision, maxRevision);
                }
            }

            return snapshot;
        }

        /// <summary>
        /// Not yet implemented.
        /// </summary>
        /// <param name="snapshot"></param>
        /// <returns></returns>
        public bool AddSnapshot(ISnapshot snapshot)
        {
            var pageBlob = WrappedPageBlob.CreateNewIfNotExists(_primaryContainer, snapshot.BucketId + "/ss/" + snapshot.StreamId, _options.BlobNumPages);

            var serializedSnapshot = _serializer.Serialize(snapshot.Payload);
            pageBlob.Metadata["ss_data_size_bytes"] = "0";
            pageBlob.Metadata["ss_stream_revision"] = "0";
            pageBlob.SetMetadata();

            var pageAlignedSize = GetPageAlignedSize(serializedSnapshot.Length);
            using (var serializedSnapshotStream = CreateAndFillStreamAligned(pageAlignedSize, serializedSnapshot))
            { pageBlob.Write(serializedSnapshotStream, 0, 0, new HeaderDefinitionMetadata()); }

            pageBlob.Metadata["ss_data_size_bytes"] = serializedSnapshot.Length.ToString();
            pageBlob.Metadata["ss_stream_revision"] = snapshot.StreamRevision.ToString();
            pageBlob.SetMetadata();

            return true;
        }

        /// <summary>
        /// Not yet implemented.
        /// </summary>
        /// <param name="bucketId"></param>
        /// <param name="maxThreshold"></param>
        /// <returns></returns>
        public IEnumerable<IStreamHead> GetStreamsToSnapshot(string bucketId, int maxThreshold)
        { throw new NotImplementedException(); }

        #region private helpers

        /// <summary>
        /// Creates a commit from the provided definition
        /// </summary>
        /// <param name="streamId"></param>
        /// <param name="commitDefinition"></param>
        /// <returns></returns>
        private ICommit CreateCommitFromDefinition(WrappedPageBlob blob, PageBlobCommitDefinition commitDefinition)
        {
            var startIndex = commitDefinition.StartPage * _blobPageSize;
            var endIndex = startIndex + commitDefinition.DataSizeBytes;
            var downloadedData = blob.DownloadBytes(startIndex, endIndex, true);
            using (var ms = new MemoryStream(downloadedData, false))
            {
                AzureBlobCommit azureBlobCommit;
                try
                { azureBlobCommit = _serializer.Deserialize<AzureBlobCommit>(ms); }
                catch (Exception ex)
                {
                    var message = string.Format("Blob with uri [{0}] is corrupt.", ((CloudPageBlob)blob).Uri);
                    Logger.Fatal(message);
                    throw new InvalidDataException(message, ex);
                }

                return CreateCommitFromAzureBlobCommit(azureBlobCommit);
            }
        }

        /// <summary>
        /// Creates a Commit object from an AzureBlobEntry.
        /// </summary>
        /// <param name="blobEntry">The source AzureBlobEntry.</param>
        /// <returns>The populated Commit.</returns>
        private ICommit CreateCommitFromAzureBlobCommit(AzureBlobCommit blobEntry)
        {
            return new Commit(blobEntry.BucketId, blobEntry.StreamId, blobEntry.StreamRevision, blobEntry.CommitId,
                                blobEntry.CommitSequence, blobEntry.CommitStampUtc, blobEntry.Checkpoint.ToString(),
                                blobEntry.Headers, blobEntry.Events);
        }

        /// <summary>
        /// Gets the metadata header
        /// </summary>
        /// <param name="blob"></param>
        /// <returns></returns>
        private HeaderDefinitionMetadata GetHeaderDefinitionMetadata(WrappedPageBlob blob, int index)
        {
            string keyToUse = null;
            if (index == 0)
            { keyToUse = _primaryHeaderDefinitionKey; }
            else if (index == 1)
            { keyToUse = _secondaryHeaderDefinitionKey; }
            else if (index == 2)
            { keyToUse = _terciaryHeaderDefintionKey; }
            else
            { throw new ArgumentException("value must be 0, 1, or 2 for primary, secondary, or terciary respectively.", "index"); }

            HeaderDefinitionMetadata headerDefinition = null;
            string serializedHeaderDefinition;
            if (blob.Metadata.TryGetValue(keyToUse, out serializedHeaderDefinition))
            { headerDefinition = HeaderDefinitionMetadata.FromRaw(Convert.FromBase64String(serializedHeaderDefinition)); }

            return headerDefinition;
        }

        /// <summary>
        /// Gets the deserialized header from the blob.  Uses
        /// </summary>
        /// <param name="blob">The Blob.</param>
        /// <param name="assumedValidHeaderDefinition">will output the header definition that is valid, null if there is not currently one</param>
        /// <returns>A populated StreamBlobHeader.</returns>
        private StreamBlobHeader GetHeaderWithRetry(WrappedPageBlob blob, out HeaderDefinitionMetadata validHeaderDefinition)
        {
            HeaderDefinitionMetadata assumedValidHeaderDefinition = null;
            StreamBlobHeader header = null;
            Exception lastException = null;

            // first, if the primary header metadata key does not exist then we default
            if (!blob.Metadata.ContainsKey(_primaryHeaderDefinitionKey))
            {
                assumedValidHeaderDefinition = new HeaderDefinitionMetadata();
                header = new StreamBlobHeader();
            }

            // do the fallback logic to try and find a valid header
            if (header == null)
            {
                for (int i = 0; i != 3; ++i)
                {
                    assumedValidHeaderDefinition = GetHeaderDefinitionMetadata(blob, i);
                    header = SafeGetHeader(blob, assumedValidHeaderDefinition, out lastException);

                    if (header != null)
                    { break; }
                }
            }

            // It is possible we will still have no header here and still be in an ok state.  This is a case where the aggregates first
            // commit set some metadata (specifically) the _primaryHeaderDefinitionKey, but then failed to write the header.  This case
            // will have a _primaryHeaderDefinitionKey but no secondary or terciary.  In addition it will have a key of first write succeeded
            // set to false.  If it does not have any key at all, it is a "legacy" one and will get the key set upon a future successful write.
            // legacy ones with issue will continue to fail and require manual intervention currently
            if (header == null)
            {
                string firstWriteCompleted;
                if (blob.Metadata.TryGetValue(_firstWriteCompletedKey, out firstWriteCompleted))
                {
                    if (firstWriteCompleted == "f")
                    {
                        header = new StreamBlobHeader();
                        assumedValidHeaderDefinition = new HeaderDefinitionMetadata();
                    }
                }
            }

            if (header != null)
            {
                validHeaderDefinition = assumedValidHeaderDefinition;
                return header;
            }

            throw lastException ?? new Exception("No header could be created"); ;
        }

        /// <summary>
        /// Safely receive the header.
        /// </summary>
        /// <param name="blob">blob</param>
        /// <param name="headerDefinition">header definition</param>
        /// <param name="exception">exception that occured</param>
        /// <returns>the header, otherwise null if it could not be fetched</returns>
        private StreamBlobHeader SafeGetHeader(WrappedPageBlob blob, HeaderDefinitionMetadata headerDefinition, out Exception exception)
        {
            StreamBlobHeader header = null;
            exception = null;

            try
            {
                if (headerDefinition == null || headerDefinition.HeaderSizeInBytes == 0)
                { throw new InvalidHeaderDataException(string.Format("Attempted to download a header, but the size specified is zero.  This aggregate with id [{0}] may be corrupt.", blob.Name)); }

                var downloadedData = blob.DownloadBytes(headerDefinition.HeaderStartLocationOffsetBytes,
                                    headerDefinition.HeaderStartLocationOffsetBytes + headerDefinition.HeaderSizeInBytes);

                using (var ms = new MemoryStream(downloadedData, false))
                { header = _serializer.Deserialize<StreamBlobHeader>(ms.ToArray()); }
            }
            catch (Exception ex)
            { exception = ex; }

            return header;
        }

        /// <summary>
        /// Get page aligned number of bytes from a non page aligned number
        /// </summary>
        /// <param name="nonAligned"></param>
        /// <returns></returns>
        private int GetPageAlignedSize(int nonAligned)
        {
            var remainder = nonAligned % _blobPageSize;
            return (remainder == 0) ? nonAligned : nonAligned + (_blobPageSize - remainder);
        }

        /// <summary>
        /// Commits the header information which essentially commits any transactions that occurred
        /// related to that header.
        /// </summary>
        /// <param name="newCommit">the new commit to write</param>
        /// <param name="blob">blob header applies to</param>
        /// <param name="updatedHeader">the new header to be serialized out</param>
        /// <param name="currentGoodHeaderDefinition">the definition for the current header, before this change is committed</param>
        /// <param name="nonAlignedBytesUsedAlready">non aligned offset of index where last commit data is stored (not inclusive of header)</param>
        /// <returns></returns>
        private void CommitNewMessage(WrappedPageBlob blob, byte[] newCommit,
            StreamBlobHeader updatedHeader, HeaderDefinitionMetadata currentGoodHeaderDefinition,
            int nonAlignedBytesUsedAlready)
        {
            newCommit = newCommit ?? new byte[0];
            var serializedHeader = _serializer.Serialize(updatedHeader);
            var writeStartLocationAligned = GetPageAlignedSize(nonAlignedBytesUsedAlready);
            var amountToWriteAligned = GetPageAlignedSize(serializedHeader.Length + newCommit.Length);
            var totalSpaceNeeded = writeStartLocationAligned + amountToWriteAligned;
            var newHeaderStartLocationNonAligned = writeStartLocationAligned + newCommit.Length;

            var totalBlobLength = blob.Properties.Length;
            if (totalBlobLength < totalSpaceNeeded)
            {
                blob.Resize(totalSpaceNeeded);
                totalBlobLength = blob.Properties.Length;
            }

            // set the header definition to make it all official
            bool isFirstWrite = currentGoodHeaderDefinition.HeaderSizeInBytes == 0;
            var headerDefinitionMetadata = new HeaderDefinitionMetadata();
            headerDefinitionMetadata.HeaderSizeInBytes = serializedHeader.Length;
            headerDefinitionMetadata.HeaderStartLocationOffsetBytes = writeStartLocationAligned + newCommit.Length;
            blob.Metadata[_isEventStreamAggregateKey] = "yes";
            blob.Metadata[_hasUndispatchedCommitsKey] = updatedHeader.PageBlobCommitDefinitions.Any((x) => !x.IsDispatched).ToString();

            if (!isFirstWrite)
            {
                blob.Metadata[_secondaryHeaderDefinitionKey] = Convert.ToBase64String(currentGoodHeaderDefinition.GetRaw());

                // this is a thirt layer backup in the case we have a issue in the middle of this upcoming write operation.
                var tempHeaderDefinition = currentGoodHeaderDefinition.Clone();
                tempHeaderDefinition.HeaderStartLocationOffsetBytes = newHeaderStartLocationNonAligned;
                blob.Metadata[_terciaryHeaderDefintionKey] = Convert.ToBase64String(tempHeaderDefinition.GetRaw());
                blob.Metadata[_firstWriteCompletedKey] = "t";
            }
            else
            { blob.Metadata[_firstWriteCompletedKey] = "f"; }

            blob.Metadata[_primaryHeaderDefinitionKey] = Convert.ToBase64String(headerDefinitionMetadata.GetRaw());
            blob.SetMetadata();

            using (var ms = CreateAndFillStreamAligned(amountToWriteAligned, newCommit, serializedHeader))
            { blob.Write(ms, writeStartLocationAligned, newHeaderStartLocationNonAligned, currentGoodHeaderDefinition); }

            // we pay the cost of an extra call for our first ever write (this is effectively creation of the aggregate.
            // we do this because we actually host our header in the blob, but the reference to that header in our metadata.
            // we set the metadata with the potential states prior to actually writing the new header.  If this was the first
            // ever write and we set the metadata, but then fail to write the header, we can get in a state where the aggregate
            // becomes unusable because it believes there should be a header according to the metadata.
            // For that reason we must record when our first write completes
            if (isFirstWrite)
            {
                blob.Metadata[_firstWriteCompletedKey] = "t";
                blob.SetMetadata();
            }
        }

        /// <summary>
        /// Returns a memory stream of alignedamount size.
        /// </summary>
        /// <param name="alignedAmount">page aligned size</param>
        /// <param name="orderedFillData">data to fill into the stream</param>
        /// <returns>A memory stream with ordered fill data contents aligned as the size specified</returns>
        private MemoryStream CreateAndFillStreamAligned(int alignedAmount, params byte[][] orderedFillData)
        {
            var totalFillDataLength = 0;
            var ms = new MemoryStream(alignedAmount);
            for (int i = 0; i != orderedFillData.Length; ++i)
            {
                totalFillDataLength += orderedFillData[i].Length;
                ms.Write(orderedFillData[i], 0, orderedFillData[i].Length);
            }

            var remainder = (ms.Position % _blobPageSize);
            var fillSpace = alignedAmount - totalFillDataLength;

            if (fillSpace != 0)
            {
                ms.Position += fillSpace - 1;
                ms.WriteByte(0);
            }

            ms.Position = 0;
            return ms;
        }

        /// <summary>
        /// Build the container name.
        /// </summary>
        /// <returns>The container name.</returns>
        private string GetContainerName()
        { return _eventSourcePrefix + _options.ContainerName.ToLower(); }

        /// <summary>
        /// Gets the next checkpoint id
        /// </summary>
        /// <returns></returns>
        private ulong GetNextCheckpoint()
        {
            var blobContainer = _blobClient.GetContainerReference(_rootContainerName);
            var checkpointBlob = WrappedPageBlob.CreateNewIfNotExists(blobContainer, _checkpointBlobName, 1);
            ((CloudPageBlob)checkpointBlob).SetSequenceNumber(SequenceNumberAction.Increment, null);
            return (ulong)((CloudPageBlob)checkpointBlob).Properties.PageBlobSequenceNumber.Value;
        }

        /// <summary>
        /// Adds a checkpoint to our table storage account allowing for simple
        /// commit replay at a later time.
        /// </summary>
        /// <param name="commit"></param>
        private void AddCheckpointTableEntry(ICommit commit)
        {
            var tableName = string.Format("chpt{0}{1}", GetContainerName(), commit.BucketId);
            var table = _checkpointTableClient.GetTableReference(tableName);

            Action addCheckpointDelegate = () => {
                var entity = new CheckpointTableEntity(commit);
                var insertOperation = TableOperation.InsertOrReplace(entity);
                table.Execute(insertOperation);
            };

            try
            { addCheckpointDelegate(); }
            catch (Microsoft.WindowsAzure.Storage.StorageException ex)
            {
                if (ex.InnerException != null && ex.InnerException is WebException &&
                    ((WebException)(ex.InnerException)).Status == WebExceptionStatus.ProtocolError &&
                    ((HttpWebResponse)(((WebException)(ex.InnerException)).Response)).StatusCode == HttpStatusCode.NotFound)
                {
                    table.CreateIfNotExists();
                    addCheckpointDelegate();
                }
                else
                { throw; }
            }
        }

        #endregion
    }
}