using System;
using System.Collections.Generic;

namespace NEventStore.Persistence.AzureBlob
{
	/// <summary>
	/// This holds the serialized data from commits
	/// </summary>
	[Serializable]
	public class AzureBlobCommit
	{
		/// <summary>
		/// The value which identifies blobEntry to which the the stream and the the commit belongs.
		/// </summary>
		public string BucketId
		{ get; set; }

		/// <summary>
		/// The value which uniquely identifies the stream in a blobEntry to which the commit belongs.
		/// </summary>
		public string StreamId
		{ get; set; }

		/// <summary>
		/// The value which indicates the revision of the most recent event in the stream to which this commit applies.
		/// </summary>
		public int StreamRevision
		{ get; set; }

        /// <summary>
        /// Get the Checkpoint for this blob entry
        /// </summary>
        public ulong Checkpoint
        { get; set; }

		/// <summary>
		/// The value which uniquely identifies the commit within the stream.
		/// </summary>
		public Guid CommitId
		{ get; set; }

		/// <summary>
		/// The value which indicates the sequence (or position) in the stream to which this commit applies.
		/// </summary>
		public int CommitSequence
		{ get; set; }

		/// <summary>
		/// The point in time at which the commit was persisted.
		/// </summary>
		public DateTime CommitStampUtc
		{ get; set; }

		/// <summary>
		/// The metadata which provides additional, unstructured information about this commit.
		/// </summary>
		public IDictionary<string, object> Headers
		{ get; set; }

		/// <summary>
		/// The collection of event messages to be committed as a single unit.
		/// </summary>
		public List<EventMessage> Events
		{ get; set; }
	}
}
