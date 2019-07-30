
#pragma warning disable 169
// ReSharper disable InconsistentNaming

namespace NEventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using FakeItEasy;
    using FluentAssertions;
    using NEventStore.Persistence;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using Xunit;

    public class when_creating_a_new_stream : using_persistence
    {
        private IEventStream Stream
        {
            get
            { return Fixture.Variables[nameof(Stream)] as IEventStream; }
            set
            { Fixture.Variables[nameof(Stream)] = value; }
        }

        public when_creating_a_new_stream(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            Stream = Store.CreateStream(StreamId);
        }

        [Fact]
        public void should_return_a_new_stream()
        {
            Stream.Should().NotBeNull();
        }

        [Fact]
        public void should_return_a_stream_with_the_correct_stream_identifier()
        {
            Stream.StreamId.Should().Be(StreamId);
        }

        [Fact]
        public void should_return_a_stream_with_a_zero_stream_revision()
        {
            Stream.StreamRevision.Should().Be(0);
        }

        [Fact]
        public void should_return_a_stream_with_a_zero_commit_sequence()
        {
            Stream.CommitSequence.Should().Be(0);
        }

        [Fact]
        public void should_return_a_stream_with_no_uncommitted_events()
        {
            Stream.UncommittedEvents.Should().BeEmpty();
        }

        [Fact]
        public void should_return_a_stream_with_no_committed_events()
        {
            Stream.CommittedEvents.Should().BeEmpty();
        }

        [Fact]
        public void should_return_a_stream_with_empty_headers()
        {
            Stream.UncommittedHeaders.Should().BeEmpty();
        }
    }

    public class when_opening_an_empty_stream_starting_at_revision_zero : using_persistence
    {
        private IEventStream Stream
        {
            get
            { return Fixture.Variables["stream"] as IEventStream; }
            set
            { Fixture.Variables["stream"] = value; }
        }

        public when_opening_an_empty_stream_starting_at_revision_zero(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, 0, 0)).Returns(new ICommit[0]);
        }

        protected override void Because()
        {
            Stream = Store.OpenStream(StreamId, 0, 0);
        }

        [Fact]
        public void should_return_a_new_stream()
        {
            Stream.Should().NotBeNull();
        }

        [Fact]
        public void should_return_a_stream_with_the_correct_stream_identifier()
        {
            Stream.StreamId.Should().Be(StreamId);
        }

        [Fact]
        public void should_return_a_stream_with_a_zero_stream_revision()
        {
            Stream.StreamRevision.Should().Be(0);
        }

        [Fact]
        public void should_return_a_stream_with_a_zero_commit_sequence()
        {
            Stream.CommitSequence.Should().Be(0);
        }

        [Fact]
        public void should_return_a_stream_with_no_uncommitted_events()
        {
            Stream.UncommittedEvents.Should().BeEmpty();
        }

        [Fact]
        public void should_return_a_stream_with_no_committed_events()
        {
            Stream.CommittedEvents.Should().BeEmpty();
        }

        [Fact]
        public void should_return_a_stream_with_empty_headers()
        {
            Stream.UncommittedHeaders.Should().BeEmpty();
        }
    }

    public class when_opening_an_empty_stream_starting_above_revision_zero : using_persistence
    {
        private const int MinRevision = 1;

        private Exception Thrown
        {
            get
            { return Fixture.Variables[nameof(Thrown)] as Exception; }
            set
            { Fixture.Variables[nameof(Thrown)] = value; }
        }

        public when_opening_an_empty_stream_starting_above_revision_zero(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, MinRevision, int.MaxValue))
                .Returns(Enumerable.Empty<ICommit>());
        }

        protected override void Because()
        {
            Thrown = Catch.Exception(() => Store.OpenStream(StreamId, MinRevision));
        }

        [Fact]
        public void should_throw_a_StreamNotFoundException()
        {
            Thrown.Should().BeOfType<StreamNotFoundException>();
        }
    }

    public class when_opening_a_populated_stream : using_persistence
    {
        private const int MinRevision = 17;
        private const int MaxRevision = 42;

        private ICommit Committed
        {
            get
            { return Fixture.Variables[nameof(Committed)] as ICommit; }
            set
            { Fixture.Variables[nameof(Committed)] = value; }
        }

        private IEventStream Stream
        {
            get
            { return Fixture.Variables[nameof(Stream)] as IEventStream; }
            set
            { Fixture.Variables[nameof(Stream)] = value; }
        }

        public when_opening_a_populated_stream(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            Committed = BuildCommitStub(MinRevision, 1);

            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, MinRevision, MaxRevision))
                .Returns(new[] { Committed });

            var hook = A.Fake<IPipelineHook>();
            A.CallTo(() => hook.Select(Committed)).Returns(Committed);
            PipelineHooks.Add(hook);
        }

        protected override void Because()
        {
            Stream = Store.OpenStream(StreamId, MinRevision, MaxRevision);
        }

        [Fact]
        public void should_invoke_the_underlying_infrastructure_with_the_values_provided()
        {
            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, MinRevision, MaxRevision)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public void should_provide_the_commits_to_the_selection_hooks()
        {
            PipelineHooks.ForEach(x => A.CallTo(() => x.Select(Committed)).MustHaveHappenedOnceExactly());
        }

        [Fact]
        public void should_return_an_event_stream_containing_the_correct_stream_identifer()
        {
            Stream.StreamId.Should().Be(StreamId);
        }
    }

    public class when_opening_a_populated_stream_from_a_snapshot : using_persistence
    {
        private const int MaxRevision = int.MaxValue;
        private ICommit[] _committed;
        private Snapshot _snapshot;

        public when_opening_a_populated_stream_from_a_snapshot(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            _snapshot = new Snapshot(StreamId, 42, "snapshot");
            _committed = new[] { BuildCommitStub(42, 0)};

            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, 42, MaxRevision)).Returns(_committed);
        }

        protected override void Because()
        {
            Store.OpenStream(_snapshot, MaxRevision);
        }

        [Fact]
        public void should_query_the_underlying_storage_using_the_revision_of_the_snapshot()
        {
            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, 42, MaxRevision)).MustHaveHappenedOnceExactly();
        }
    }

    public class when_opening_a_stream_from_a_snapshot_that_is_at_the_revision_of_the_stream_head : using_persistence
    {
        private const int HeadStreamRevision = 42;
        private const int HeadCommitSequence = 15;
        
        private EnumerableCounter<ICommit> Committed
        {
            get
            { return Fixture.Variables[nameof(Committed)] as EnumerableCounter<ICommit>; }
            set
            { Fixture.Variables[nameof(Committed)] = value; }
        }

        private Snapshot Snapshot
        {
            get
            { return Fixture.Variables[nameof(Snapshot)] as Snapshot; }
            set
            { Fixture.Variables[nameof(Snapshot)] = value; }
        }

        private IEventStream Stream
        {
            get
            { return Fixture.Variables[nameof(Stream)] as IEventStream; }
            set
            { Fixture.Variables[nameof(Stream)] = value; }
        }

        public when_opening_a_stream_from_a_snapshot_that_is_at_the_revision_of_the_stream_head(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            Snapshot = new Snapshot(StreamId, HeadStreamRevision, "snapshot");
            Committed = new EnumerableCounter<ICommit>(
                new[] { BuildCommitStub(HeadStreamRevision, HeadCommitSequence)});

            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, HeadStreamRevision, int.MaxValue))
                .Returns(Committed);
        }

        protected override void Because()
        {
            Stream = Store.OpenStream(Snapshot, int.MaxValue);
        }

        [Fact]
        public void should_return_a_stream_with_the_correct_stream_identifier()
        {
            Stream.StreamId.Should().Be(StreamId);
        }

        [Fact]
        public void should_return_a_stream_with_revision_of_the_stream_head()
        {
            Stream.StreamRevision.Should().Be(HeadStreamRevision);
        }

        [Fact]
        public void should_return_a_stream_with_a_commit_sequence_of_the_stream_head()
        {
            Stream.CommitSequence.Should().Be(HeadCommitSequence);
        }

        [Fact]
        public void should_return_a_stream_with_no_committed_events()
        {
            Stream.CommittedEvents.Count.Should().Be(0);
        }

        [Fact]
        public void should_return_a_stream_with_no_uncommitted_events()
        {
            Stream.UncommittedEvents.Count.Should().Be(0);
        }

        [Fact]
        public void should_only_enumerate_the_set_of_commits_once()
        {
            Committed.GetEnumeratorCallCount.Should().Be(1);
        }
    }

    public class when_reading_from_revision_zero : using_persistence
    {
        public when_reading_from_revision_zero(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, 0, int.MaxValue))
                .Returns(Enumerable.Empty<ICommit>());
        }

        protected override void Because()
        {
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            // This forces the enumeration of the commits.
            Store.GetFrom(StreamId, 0, int.MaxValue).ToList();
        }

        [Fact]
        public void should_pass_a_revision_range_to_the_persistence_infrastructure()
        {
            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, 0, int.MaxValue)).MustHaveHappenedOnceExactly();
        }
    }

    public class when_reading_up_to_revision_revision_zero : using_persistence
    {
        private ICommit _committed;

        public when_reading_up_to_revision_revision_zero(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            _committed = BuildCommitStub(1, 1);

            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, 0, int.MaxValue)).Returns(new[] { _committed });
        }

        protected override void Because()
        {
            Store.OpenStream(StreamId, 0, 0);
        }

        [Fact]
        public void should_pass_the_maximum_possible_revision_to_the_persistence_infrastructure()
        {
            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, 0, int.MaxValue)).MustHaveHappenedOnceExactly();
        }
    }

    public class when_reading_from_a_null_snapshot : using_persistence
    {
        private Exception thrown;

        public when_reading_from_a_null_snapshot(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            thrown = Catch.Exception(() => Store.OpenStream(null, int.MaxValue));
        }

        [Fact]
        public void should_throw_an_ArgumentNullException()
        {
            thrown.Should().BeOfType<ArgumentNullException>();
        }
    }

    public class when_reading_from_a_snapshot_up_to_revision_revision_zero : using_persistence
    {
        private ICommit _committed;
        private Snapshot snapshot;

        public when_reading_from_a_snapshot_up_to_revision_revision_zero(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            snapshot = new Snapshot(StreamId, 1, "snapshot");
            _committed = BuildCommitStub(1, 1);

            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, snapshot.StreamRevision, int.MaxValue))
                .Returns(new[] { _committed });
        }

        protected override void Because()
        {
            Store.OpenStream(snapshot, 0);
        }

        [Fact]
        public void should_pass_the_maximum_possible_revision_to_the_persistence_infrastructure()
        {
            A.CallTo(() => Persistence.GetFrom(Bucket.Default, StreamId, snapshot.StreamRevision, int.MaxValue)).MustHaveHappenedOnceExactly();
        }
    }

    public class when_committing_a_null_attempt_back_to_the_stream : using_persistence
    {
        private Exception thrown;

        public when_committing_a_null_attempt_back_to_the_stream(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            thrown = Catch.Exception(() => ((ICommitEvents) Store).Commit(null));
        }

        [Fact]
        public void should_throw_an_ArgumentNullException()
        {
            thrown.Should().BeOfType<ArgumentNullException>();
        }
    }

    public class when_committing_with_a_valid_and_populated_attempt_to_a_stream : using_persistence
    {
        private CommitAttempt PopulatedAttempt
        {
            get
            { return Fixture.Variables[nameof(PopulatedAttempt)] as CommitAttempt; }
            set
            { Fixture.Variables[nameof(PopulatedAttempt)] = value; }
        }

        private ICommit PopulatedCommit
        {
            get
            { return Fixture.Variables[nameof(PopulatedCommit)] as ICommit; }
            set
            { Fixture.Variables[nameof(PopulatedCommit)] = value; }
        }

        public when_committing_with_a_valid_and_populated_attempt_to_a_stream(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            PopulatedAttempt = BuildCommitAttemptStub(1, 1);

            A.CallTo(() => Persistence.Commit(PopulatedAttempt))
                .ReturnsLazily((CommitAttempt attempt) =>
                {
                    PopulatedCommit = new Commit(attempt.BucketId,
                        attempt.StreamId,
                        attempt.StreamRevision,
                        attempt.CommitId,
                        attempt.CommitSequence,
                        attempt.CommitStamp,
                        new LongCheckpoint(0).Value,
                        attempt.Headers,
                        attempt.Events);
                    return PopulatedCommit;
                });

            var hook = A.Fake<IPipelineHook>();
            A.CallTo(() => hook.PreCommit(PopulatedAttempt)).Returns(true);

            PipelineHooks.Add(hook);
        }

        protected override void Because()
        {
            ((ICommitEvents) Store).Commit(PopulatedAttempt);
        }

        [Fact]
        public void should_provide_the_commit_to_the_precommit_hooks()
        {
            PipelineHooks.ForEach(x => A.CallTo(() => x.PreCommit(PopulatedAttempt)).MustHaveHappenedOnceExactly());
        }

        [Fact]
        public void should_provide_the_commit_attempt_to_the_configured_persistence_mechanism()
        {
            A.CallTo(() => Persistence.Commit(PopulatedAttempt)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public void should_provide_the_commit_to_the_postcommit_hooks()
        {
            PipelineHooks.ForEach(x => A.CallTo(() => x.PostCommit(PopulatedCommit)).MustHaveHappenedOnceExactly());
        }
    }

    public class when_a_precommit_hook_rejects_a_commit : using_persistence
    {
        private CommitAttempt Attempt
        {
            get
            { return Fixture.Variables[nameof(Attempt)] as CommitAttempt; }
            set
            { Fixture.Variables[nameof(Attempt)] = value; }
        }

        private ICommit Commit
        {
            get
            { return Fixture.Variables[nameof(Commit)] as ICommit; }
            set
            { Fixture.Variables[nameof(Commit)] = value; }
        }

        public when_a_precommit_hook_rejects_a_commit(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            Attempt = BuildCommitAttemptStub(1, 1);
            Commit = BuildCommitStub(1, 1);

            var hook = A.Fake<IPipelineHook>();
            A.CallTo(() => hook.PreCommit(Attempt)).Returns(false);

            PipelineHooks.Add(hook);
        }

        protected override void Because()
        {
            ((ICommitEvents) Store).Commit(Attempt);
        }

        [Fact]
        public void should_not_call_the_underlying_infrastructure()
        {
            A.CallTo(() => Persistence.Commit(Attempt)).MustNotHaveHappened();
        }

        [Fact]
        public void should_not_provide_the_commit_to_the_postcommit_hooks()
        {
            PipelineHooks.ForEach(x => A.CallTo(() => x.PostCommit(Commit)).MustNotHaveHappened());
        }
    }

    public class when_accessing_the_underlying_persistence : using_persistence
    {
        public when_accessing_the_underlying_persistence(TestFixture fixture)
            : base(fixture)
        { }

        public void should_return_a_reference_to_the_underlying_persistence_infrastructure_decorator()
        {
            Store.Advanced.Should().BeOfType<PipelineHooksAwarePersistanceDecorator>();
        }
    }

    public class when_disposing_the_event_store : using_persistence
    {
        public when_disposing_the_event_store(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            Store.Dispose();
        }

        [Fact]
        public void should_dispose_the_underlying_persistence()
        {
            A.CallTo(() => Persistence.Dispose()).MustHaveHappenedOnceExactly();
        }
    }

    public abstract class using_persistence : SpecificationBase<TestFixture>
    {
        protected string StreamId
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(StreamId), out var streamId))
                {
                    streamId = Guid.NewGuid().ToString();
                    StreamId = streamId as string;
                }

                return streamId as string;
            }
            set
            { Fixture.Variables[nameof(StreamId)] = value; }
        }

        protected IPersistStreams Persistence
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(Persistence), out var persistence))
                {
                    persistence = A.Fake<IPersistStreams>();
                    Persistence = persistence as IPersistStreams;
                }

                return persistence as IPersistStreams;
            }
            set
            { Fixture.Variables[nameof(Persistence)] = value; }
        }

        protected List<IPipelineHook> PipelineHooks
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(PipelineHooks), out var pipelineHooks))
                {
                    pipelineHooks = new List<IPipelineHook>();
                    PipelineHooks = pipelineHooks as List<IPipelineHook>;
                }

                return pipelineHooks as List<IPipelineHook>;
            }
            set
            { Fixture.Variables[nameof(PipelineHooks)] = value; }
        }

        protected OptimisticEventStore Store
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(Store), out var store))
                {
                    store = new OptimisticEventStore(Persistence, PipelineHooks.Select(x => x));
                    Store = store as OptimisticEventStore;
                }

                return store as OptimisticEventStore;
            }
            set
            { Fixture.Variables[nameof(Store)] = value; }
        }

        public using_persistence(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        { base.Context(); }

        protected override void Cleanup()
        { }

        protected CommitAttempt BuildCommitAttemptStub(Guid commitId)
        {
            return new CommitAttempt(Bucket.Default, StreamId, 1, commitId, 1, SystemTime.UtcNow, null, null);
        }

        protected ICommit BuildCommitStub(int streamRevision, int commitSequence)
        {
            List<EventMessage> events = new[] {new EventMessage()}.ToList();
            return new Commit(Bucket.Default, StreamId, streamRevision, Guid.NewGuid(), commitSequence, SystemTime.UtcNow, new LongCheckpoint(0).Value, null, events);
        }

        protected CommitAttempt BuildCommitAttemptStub(int streamRevision, int commitSequence)
        {
            List<EventMessage> events = new[] { new EventMessage() }.ToList();
            return new CommitAttempt(Bucket.Default, StreamId, streamRevision, Guid.NewGuid(), commitSequence, SystemTime.UtcNow, null, events);
        }

        protected ICommit BuildCommitStub(Guid commitId, int streamRevision, int commitSequence)
        {
            List<EventMessage> events = new[] {new EventMessage()}.ToList();
            return new Commit(Bucket.Default, StreamId, streamRevision, commitId, commitSequence, SystemTime.UtcNow, new LongCheckpoint(0).Value, null, events);
        }
    }
}

// ReSharper enable InconsistentNaming
#pragma warning restore 169