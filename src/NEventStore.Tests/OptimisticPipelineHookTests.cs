
#pragma warning disable 169
// ReSharper disable InconsistentNaming

namespace NEventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FluentAssertions;
    using NEventStore.Persistence;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using Xunit;

    public class OptimisticPipelineHookTests
    {
        public class when_committing_with_a_sequence_beyond_the_known_end_of_a_stream : using_commit_hooks
        {
            private const int HeadStreamRevision = 5;
            private const int HeadCommitSequence = 1;
            private const int ExpectedNextCommitSequence = HeadCommitSequence + 1;
            private const int BeyondEndOfStreamCommitSequence = ExpectedNextCommitSequence + 1;
            private ICommit _alreadyCommitted;
            private CommitAttempt _beyondEndOfStream;
            private Exception _thrown;

            public when_committing_with_a_sequence_beyond_the_known_end_of_a_stream(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _alreadyCommitted = BuildCommitStub(HeadStreamRevision, HeadCommitSequence);
                _beyondEndOfStream = BuildCommitAttemptStub(HeadStreamRevision + 1, BeyondEndOfStreamCommitSequence);

                Hook.PostCommit(_alreadyCommitted);
            }

            protected override void Because()
            {
                _thrown = Catch.Exception(() => Hook.PreCommit(_beyondEndOfStream));
            }

            [Fact]
            public void should_throw_a_PersistenceException()
            {
                _thrown.Should().BeOfType<StorageException>();
            }
        }

        public class when_committing_with_a_revision_beyond_the_known_end_of_a_stream : using_commit_hooks
        {
            private const int HeadCommitSequence = 1;
            private const int HeadStreamRevision = 1;
            private const int NumberOfEventsBeingCommitted = 1;
            private const int ExpectedNextStreamRevision = HeadStreamRevision + 1 + NumberOfEventsBeingCommitted;
            private const int BeyondEndOfStreamRevision = ExpectedNextStreamRevision + 1;
            private ICommit _alreadyCommitted;
            private CommitAttempt _beyondEndOfStream;
            private Exception _thrown;

            public when_committing_with_a_revision_beyond_the_known_end_of_a_stream(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _alreadyCommitted = BuildCommitStub(HeadStreamRevision, HeadCommitSequence);
                _beyondEndOfStream = BuildCommitAttemptStub(BeyondEndOfStreamRevision, HeadCommitSequence + 1);

                Hook.PostCommit(_alreadyCommitted);
            }

            protected override void Because()
            {
                _thrown = Catch.Exception(() => Hook.PreCommit(_beyondEndOfStream));
            }

            [Fact]
            public void should_throw_a_PersistenceException()
            {
                _thrown.Should().BeOfType<StorageException>();
            }
        }

    public class when_committing_with_a_sequence_less_or_equal_to_the_most_recent_sequence_for_the_stream : using_commit_hooks
        {
            private const int HeadStreamRevision = 42;
            private const int HeadCommitSequence = 42;
            private const int DupliateCommitSequence = HeadCommitSequence;
            private CommitAttempt Attempt;
            private ICommit Committed;

            private Exception thrown;

            public when_committing_with_a_sequence_less_or_equal_to_the_most_recent_sequence_for_the_stream(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                Committed = BuildCommitStub(HeadStreamRevision, HeadCommitSequence);
                Attempt = BuildCommitAttemptStub(HeadStreamRevision + 1, DupliateCommitSequence);

                Hook.PostCommit(Committed);
            }

            protected override void Because()
            {
                thrown = Catch.Exception(() => Hook.PreCommit(Attempt));
            }

            [Fact]
            public void should_throw_a_ConcurrencyException()
            {
                thrown.Should().BeOfType<ConcurrencyException>();
            }
        }

    public class when_committing_with_a_revision_less_or_equal_to_than_the_most_recent_revision_read_for_the_stream : using_commit_hooks
        {
            private const int HeadStreamRevision = 3;
            private const int HeadCommitSequence = 2;
            private const int DuplicateStreamRevision = HeadStreamRevision;
            private ICommit _committed;
            private CommitAttempt _failedAttempt;
            private Exception _thrown;

            public when_committing_with_a_revision_less_or_equal_to_than_the_most_recent_revision_read_for_the_stream(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _committed = BuildCommitStub(HeadStreamRevision, HeadCommitSequence);
                _failedAttempt = BuildCommitAttemptStub(DuplicateStreamRevision, HeadCommitSequence + 1);

                Hook.PostCommit(_committed);
            }

            protected override void Because()
            {
                _thrown = Catch.Exception(() => Hook.PreCommit(_failedAttempt));
            }

            [Fact]
            public void should_throw_a_ConcurrencyException()
            {
                _thrown.Should().BeOfType<ConcurrencyException>();
            }
        }

    public class when_committing_with_a_commit_sequence_less_than_or_equal_to_the_most_recent_commit_for_the_stream : using_commit_hooks
        {
            private const int DuplicateCommitSequence = 1;
            private CommitAttempt _failedAttempt;
            private ICommit _successfulAttempt;
            private Exception _thrown;

            public when_committing_with_a_commit_sequence_less_than_or_equal_to_the_most_recent_commit_for_the_stream(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _successfulAttempt = BuildCommitStub(1, DuplicateCommitSequence);
                _failedAttempt = BuildCommitAttemptStub(2, DuplicateCommitSequence);

                Hook.PostCommit(_successfulAttempt);
            }

            protected override void Because()
            {
                _thrown = Catch.Exception(() => Hook.PreCommit(_failedAttempt));
            }

            [Fact]
            public void should_throw_a_ConcurrencyException()
            {
                _thrown.Should().BeOfType<ConcurrencyException>();
            }
        }

    public class when_committing_with_a_stream_revision_less_than_or_equal_to_the_most_recent_commit_for_the_stream : using_commit_hooks
        {
            private const int DuplicateStreamRevision = 2;

            private CommitAttempt _failedAttempt;
            private ICommit _successfulAttempt;
            private Exception _thrown;

            public when_committing_with_a_stream_revision_less_than_or_equal_to_the_most_recent_commit_for_the_stream(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _successfulAttempt = BuildCommitStub(DuplicateStreamRevision, 1);
                _failedAttempt = BuildCommitAttemptStub(DuplicateStreamRevision, 2);

                Hook.PostCommit(_successfulAttempt);
            }

            protected override void Because()
            {
                _thrown = Catch.Exception(() => Hook.PreCommit(_failedAttempt));
            }

            [Fact]
            public void should_throw_a_ConcurrencyException()
            {
                _thrown.Should().BeOfType<ConcurrencyException>();
            }
        }

        public class when_tracking_commits : SpecificationBase<TestFixture>
        {
            private const int MaxStreamsToTrack = 2;

            private ICommit[] TrackedCommitAttempts
            {
                get
                { return Fixture.Variables[nameof(TrackedCommitAttempts)] as ICommit[]; }
                set
                { Fixture.Variables[nameof(TrackedCommitAttempts)] = value; }
            }

            private OptimisticPipelineHook Hook
            {
                get
                { return Fixture.Variables[nameof(Hook)] as OptimisticPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook)] = value; }
            }

            public when_tracking_commits(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                TrackedCommitAttempts = new[]
                {
                    BuildCommit(Guid.NewGuid(), Guid.NewGuid()),
                    BuildCommit(Guid.NewGuid(), Guid.NewGuid()),
                    BuildCommit(Guid.NewGuid(), Guid.NewGuid())
                };

                Hook = new OptimisticPipelineHook(MaxStreamsToTrack);
            }

            protected override void Because()
            {
                foreach (var commit in TrackedCommitAttempts)
                {
                    Hook.Track(commit);
                }
            }

            [Fact]
            public void should_only_contain_streams_explicitly_tracked()
            {
                ICommit untracked = BuildCommit(Guid.Empty, TrackedCommitAttempts[0].CommitId);
                Hook.Contains(untracked).Should().BeFalse();
            }

            [Fact]
            public void should_find_tracked_streams()
            {
                ICommit stillTracked = BuildCommit(TrackedCommitAttempts.Last().StreamId, TrackedCommitAttempts.Last().CommitId);
                Hook.Contains(stillTracked).Should().BeTrue();
            }

            [Fact]
            public void should_only_track_the_specified_number_of_streams()
            {
                ICommit droppedFromTracking = BuildCommit(
                    TrackedCommitAttempts.First().StreamId, TrackedCommitAttempts.First().CommitId);
                Hook.Contains(droppedFromTracking).Should().BeFalse();
            }

            private ICommit BuildCommit(Guid streamId, Guid commitId)
            {
                return BuildCommit(streamId.ToString(), commitId);
            }

            private ICommit BuildCommit(string streamId, Guid commitId)
            {
                return new Commit(Bucket.Default, streamId, 0, commitId, 0, SystemTime.UtcNow, new LongCheckpoint(0).Value, null, null);
            }
        }

        public class when_purging : SpecificationBase<TestFixture>
        {
            private ICommit _trackedCommit;
            private OptimisticPipelineHook _hook;

            public when_purging(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _trackedCommit = BuildCommit(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
                _hook = new OptimisticPipelineHook();
                _hook.Track(_trackedCommit);
            }

            protected override void Because()
            {
                _hook.OnPurge();
            }

            [Fact]
            public void should_not_track_commit()
            {
                _hook.Contains(_trackedCommit).Should().BeFalse();
            }

            private ICommit BuildCommit(Guid bucketId, Guid streamId, Guid commitId)
            {
                return new Commit(bucketId.ToString(), streamId.ToString(), 0, commitId, 0, SystemTime.UtcNow,
                    new LongCheckpoint(0).Value, null, null);
            }
        }

        public class when_purging_a_bucket : SpecificationBase<TestFixture>
        {
            private ICommit TrackedCommitBucket1
            {
                get
                { return Fixture.Variables[nameof(TrackedCommitBucket1)] as ICommit; }
                set
                { Fixture.Variables[nameof(TrackedCommitBucket1)] = value; }
            }

            private ICommit TrackedCommitBucket2
            {
                get
                { return Fixture.Variables[nameof(TrackedCommitBucket2)] as ICommit; }
                set
                { Fixture.Variables[nameof(TrackedCommitBucket2)] = value; }
            }

            private OptimisticPipelineHook Hook
            {
                get
                { return Fixture.Variables[nameof(Hook)] as OptimisticPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook)] = value; }
            }

            public when_purging_a_bucket(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                TrackedCommitBucket1 = BuildCommit(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
                TrackedCommitBucket2 = BuildCommit(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
                Hook = new OptimisticPipelineHook();
                Hook.Track(TrackedCommitBucket1);
                Hook.Track(TrackedCommitBucket2);
            }

            protected override void Because()
            {
                Hook.OnPurge(TrackedCommitBucket1.BucketId);
            }

            [Fact]
            public void should_not_track_the_commit_in_bucket()
            {
                Hook.Contains(TrackedCommitBucket1).Should().BeFalse();
            }

            [Fact]
            public void should_track_the_commit_in_other_bucket()
            {
                Hook.Contains(TrackedCommitBucket2).Should().BeTrue();
            }

            private ICommit BuildCommit(Guid bucketId, Guid streamId, Guid commitId)
            {
                return new Commit(bucketId.ToString(), streamId.ToString(), 0, commitId, 0, SystemTime.UtcNow,
                    new LongCheckpoint(0).Value, null, null);
            }
        }

        public class when_deleting_a_stream : SpecificationBase<TestFixture>
        {
            private ICommit TrackedCommit
            {
                get
                { return Fixture.Variables[nameof(TrackedCommit)] as ICommit; }
                set
                { Fixture.Variables[nameof(TrackedCommit)] = value; }
            }

            private ICommit TrackedCommitDeleted
            {
                get
                { return Fixture.Variables[nameof(TrackedCommitDeleted)] as ICommit; }
                set
                { Fixture.Variables[nameof(TrackedCommitDeleted)] = value; }
            }

            private OptimisticPipelineHook Hook
            {
                get
                { return Fixture.Variables[nameof(Hook)] as OptimisticPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook)] = value; }
            }

            private Guid BucketId
            {
                get
                { return (Guid)Fixture.Variables[nameof(BucketId)]; }
                set
                { Fixture.Variables[nameof(BucketId)] = value; }
            }

            private Guid StreamIdDeleted
            {
                get
                { return (Guid)Fixture.Variables[nameof(StreamIdDeleted)]; }
                set
                { Fixture.Variables[nameof(StreamIdDeleted)] = value; }
            }

            public when_deleting_a_stream(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                base.Context();
                BucketId = Guid.NewGuid();
                StreamIdDeleted = Guid.NewGuid();

                TrackedCommit = BuildCommit(BucketId, Guid.NewGuid(), Guid.NewGuid());
                TrackedCommitDeleted = BuildCommit(BucketId, StreamIdDeleted, Guid.NewGuid());
                Hook = new OptimisticPipelineHook();
                Hook.Track(TrackedCommit);
                Hook.Track(TrackedCommitDeleted);
            }

            protected override void Because()
            {
                Hook.OnDeleteStream(TrackedCommitDeleted.BucketId, TrackedCommitDeleted.StreamId);
            }

            [Fact]
            public void should_not_track_the_commit_in_the_deleted_stream()
            {
                Hook.Contains(TrackedCommitDeleted).Should().BeFalse();
            }

            [Fact]
            public void should_track_the_commit_that_is_not_in_the_deleted_stream()
            {
                Hook.Contains(TrackedCommit).Should().BeTrue();
            }

            private ICommit BuildCommit(Guid bucketId, Guid streamId, Guid commitId)
            {
                return new Commit(bucketId.ToString(), streamId.ToString(), 0, commitId, 0, SystemTime.UtcNow,
                    new LongCheckpoint(0).Value, null, null);
            }
        }

        public abstract class using_commit_hooks : SpecificationBase<TestFixture>
        {
            protected readonly OptimisticPipelineHook Hook = new OptimisticPipelineHook();
            private readonly string _streamId = Guid.NewGuid().ToString();

            public using_commit_hooks(TestFixture fixture)
                : base(fixture)
            { }

            protected CommitAttempt BuildCommitStub(Guid commitId)
            {
                return new CommitAttempt(_streamId, 1, commitId, 1, SystemTime.UtcNow, null, null);
            }

            protected ICommit BuildCommitStub(int streamRevision, int commitSequence)
            {
                List<EventMessage> events = new[] {new EventMessage()}.ToList();
                return new Commit(Bucket.Default, _streamId, streamRevision, Guid.NewGuid(), commitSequence, SystemTime.UtcNow, new LongCheckpoint(0).Value, null, events);
            }

            protected CommitAttempt BuildCommitAttemptStub(int streamRevision, int commitSequence)
            {
                List<EventMessage> events = new[] {new EventMessage()}.ToList();
                return new CommitAttempt(Bucket.Default, _streamId, streamRevision, Guid.NewGuid(), commitSequence, SystemTime.UtcNow, null, events);
            }

            protected ICommit BuildCommitStub(Guid commitId, int streamRevision, int commitSequence)
            {
                List<EventMessage> events = new[] {new EventMessage()}.ToList();
                return new Commit(Bucket.Default, _streamId, streamRevision, commitId, commitSequence, SystemTime.UtcNow, new LongCheckpoint(0).Value, null, events);
            }
        }
    }
}

// ReSharper enable InconsistentNaming
#pragma warning restore 169