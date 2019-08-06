#pragma warning disable 169

namespace NEventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using FakeItEasy;

    using NEventStore.Persistence;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using Xunit;
    using FluentAssertions;

    public class when_building_a_stream : on_the_event_stream
    {
        private const int MinRevision = 2;
        private const int MaxRevision = 7;

        private readonly int _eachCommitHas = 2.Events();

        private ICommit[] Committed
        {
            get
            { return Fixture.Variables[nameof(Committed)] as ICommit[]; }
            set
            { Fixture.Variables[nameof(Committed)] = value; }
        }

        public when_building_a_stream(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();

            Committed = new[]
            {
                BuildCommitStub(2, 1, _eachCommitHas), // 1-2
                BuildCommitStub(4, 2, _eachCommitHas), // 3-4
                BuildCommitStub(6, 3, _eachCommitHas), // 5-6
                BuildCommitStub(8, 3, _eachCommitHas) // 7-8
            };

            Committed[0].Headers["Common"] = string.Empty;
            Committed[1].Headers["Common"] = string.Empty;
            Committed[2].Headers["Common"] = string.Empty;
            Committed[3].Headers["Common"] = string.Empty;
            Committed[0].Headers["Unique"] = string.Empty;

            A.CallTo(() => Persistence.GetFrom(BucketId, StreamId, MinRevision, MaxRevision)).Returns(Committed);
        }

        protected override void Because()
        {
            Stream = new OptimisticEventStream(BucketId, StreamId, Persistence, MinRevision, MaxRevision);
        }

        [Fact]
        public void should_have_the_correct_stream_identifier()
        {
            Stream.StreamId.Should().Be(StreamId);
        }

        [Fact]
        public void should_have_the_correct_head_stream_revision()
        {
            Stream.StreamRevision.Should().Be(MaxRevision);
        }

        [Fact]
        public void should_have_the_correct_head_commit_sequence()
        {
            Stream.CommitSequence.Should().Be(Committed.Last().CommitSequence);
        }

        [Fact]
        public void should_not_include_events_below_the_minimum_revision_indicated()
        {
            Stream.CommittedEvents.First().Should().Be(Committed.First().Events.Last());
        }

        [Fact]
        public void should_not_include_events_above_the_maximum_revision_indicated()
        {
            Stream.CommittedEvents.Last().Should().Be(Committed.Last().Events.First());
        }

        [Fact]
        public void should_have_all_of_the_committed_events_up_to_the_stream_revision_specified()
        {
            Stream.CommittedEvents.Count.Should().Be(MaxRevision - MinRevision + 1);
        }

        [Fact]
        public void should_contain_the_headers_from_the_underlying_commits()
        {
            Stream.CommittedHeaders.Count.Should().Be(2);
        }
    }

    public class when_the_head_event_revision_is_less_than_the_max_desired_revision : on_the_event_stream
    {
        private readonly int _eventsPerCommit = 2.Events();
        private ICommit[] _committed;

        public when_the_head_event_revision_is_less_than_the_max_desired_revision(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            _committed = new[]
            {
                BuildCommitStub(2, 1, _eventsPerCommit), // 1-2
                BuildCommitStub(4, 2, _eventsPerCommit), // 3-4
                BuildCommitStub(6, 3, _eventsPerCommit), // 5-6
                BuildCommitStub(8, 3, _eventsPerCommit) // 7-8
            };

            A.CallTo(() => Persistence.GetFrom(BucketId, StreamId, 0, int.MaxValue)).Returns(_committed);
        }

        protected override void Because()
        {
            Stream = new OptimisticEventStream(BucketId, StreamId, Persistence, 0, int.MaxValue);
        }

        [Fact]
        public void should_set_the_stream_revision_to_the_revision_of_the_most_recent_event()
        {
            Stream.StreamRevision.Should().Be(_committed.Last().StreamRevision);
        }
    }

    public class when_adding_a_null_event_message : on_the_event_stream
    {
        public when_adding_a_null_event_message(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            Stream.Add(null);
        }

        [Fact]
        public void should_be_ignored()
        {
            Stream.UncommittedEvents.Should().BeEmpty();
        }
    }

    public class when_adding_an_unpopulated_event_message : on_the_event_stream
    {
        public when_adding_an_unpopulated_event_message(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            Stream.Add(new EventMessage { Body = null });
        }

        [Fact]
        public void should_be_ignored()
        {
            Stream.UncommittedEvents.Should().BeEmpty();
        }
    }

    public class when_adding_a_fully_populated_event_message : on_the_event_stream
    {
        public when_adding_a_fully_populated_event_message(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            Stream.Add(new EventMessage { Body = "populated" });
        }

        [Fact]
        public void should_add_the_event_to_the_set_of_uncommitted_events()
        {
            Stream.UncommittedEvents.Count.Should().Be(1);
        }
    }

    public class when_adding_multiple_populated_event_messages : on_the_event_stream
    {
        public when_adding_multiple_populated_event_messages(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            Stream.Add(new EventMessage { Body = "populated" });
            Stream.Add(new EventMessage { Body = "also populated" });
        }

        [Fact]
        public void should_add_all_of_the_events_provided_to_the_set_of_uncommitted_events()
        {
            Stream.UncommittedEvents.Count.Should().Be(2);
        }
    }

    public class when_adding_a_simple_object_as_an_event_message : on_the_event_stream
    {
        private const string MyEvent = "some event data";

        public when_adding_a_simple_object_as_an_event_message(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            Stream.Add(new EventMessage { Body = MyEvent });
        }

        [Fact]
        public void should_add_the_uncommited_event_to_the_set_of_uncommitted_events()
        {
            Stream.UncommittedEvents.Count.Should().Be(1);
        }

        [Fact]
        public void should_wrap_the_uncommited_event_in_an_EventMessage_object()
        {
            Stream.UncommittedEvents.First().Body.Should().Be(MyEvent);
        }
    }

    public class when_clearing_any_uncommitted_changes : on_the_event_stream
    {
        public when_clearing_any_uncommitted_changes(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Stream.Add(new EventMessage { Body = string.Empty });
        }

        protected override void Because()
        {
            Stream.ClearChanges();
        }

        [Fact]
        public void should_clear_all_uncommitted_events()
        {
            Stream.UncommittedEvents.Count.Should().Be(0);
        }
    }

    public class when_committing_an_empty_changeset : on_the_event_stream
    {
        public when_committing_an_empty_changeset(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            Stream.CommitChanges(Guid.NewGuid());
        }

        [Fact]
        public void should_not_call_the_underlying_infrastructure()
        {
            A.CallTo(() => Persistence.Commit(A<CommitAttempt>._)).MustNotHaveHappened();
        }

        [Fact]
        public void should_not_increment_the_current_stream_revision()
        {
            Stream.StreamRevision.Should().Be(0);
        }

        [Fact]
        public void should_not_increment_the_current_commit_sequence()
        {
            Stream.CommitSequence.Should().Be(0);
        }
    }

    public class when_committing_any_uncommitted_changes : on_the_event_stream
    {
        private Guid CommitId
        {
            get
            { return (Guid)Fixture.Variables[nameof(CommitId)]; }
            set
            { Fixture.Variables[nameof(CommitId)] = value; }
        }

        private Dictionary<string, object> Headers
        {
            get
            { return Fixture.Variables[nameof(Headers)] as Dictionary<string, object>; }
            set
            { Fixture.Variables[nameof(Headers)] = value; }
        }

        private EventMessage Uncommitted
        {
            get
            { return Fixture.Variables[nameof(Uncommitted)] as EventMessage; }
            set
            { Fixture.Variables[nameof(Uncommitted)] = value; }
        }

        private CommitAttempt Constructed
        {
            get
            { return Fixture.Variables[nameof(Constructed)] as CommitAttempt; }
            set
            { Fixture.Variables[nameof(Constructed)] = value; }
        }

        public when_committing_any_uncommitted_changes(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();

            CommitId = Guid.NewGuid();
            Headers = new Dictionary<string, object> { { "key", "value" } };
            Uncommitted = new EventMessage { Body = string.Empty };

            A.CallTo(() => Persistence.Commit(A<CommitAttempt>._))
                .Invokes((CommitAttempt _) => Constructed = _)
                .ReturnsLazily((CommitAttempt attempt) => new Commit(
                    attempt.BucketId,
                    attempt.StreamId,
                    attempt.StreamRevision,
                    attempt.CommitId,
                    attempt.CommitSequence,
                    attempt.CommitStamp,
                    new LongCheckpoint(0).Value,
                    attempt.Headers,
                    attempt.Events));
            Stream.Add(Uncommitted);
            foreach (var item in Headers)
            {
                Stream.UncommittedHeaders[item.Key] = item.Value;
            }
        }

        protected override void Because()
        {
            Stream.CommitChanges(CommitId);
        }

        [Fact]
        public void should_provide_a_commit_to_the_underlying_infrastructure()
        {
            A.CallTo(() => Persistence.Commit(A<CommitAttempt>._)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public void should_build_the_commit_with_the_correct_bucket_identifier()
        {
            Constructed.BucketId.Should().Be(BucketId);
        }

        [Fact]
        public void should_build_the_commit_with_the_correct_stream_identifier()
        {
            Constructed.StreamId.Should().Be(StreamId);
        }

        [Fact]
        public void should_build_the_commit_with_the_correct_stream_revision()
        {
            Constructed.StreamRevision.Should().Be(DefaultStreamRevision);
        }

        [Fact]
        public void should_build_the_commit_with_the_correct_commit_identifier()
        {
            Constructed.CommitId.Should().Be(CommitId);
        }

        [Fact]
        public void should_build_the_commit_with_an_incremented_commit_sequence()
        {
            Constructed.CommitSequence.Should().Be(DefaultCommitSequence);
        }

        [Fact]
        public void should_build_the_commit_with_the_correct_commit_stamp()
        {
            SystemTime.UtcNow.Should().BeCloseTo(Constructed.CommitStamp, 250);
        }

        [Fact]
        public void should_build_the_commit_with_the_headers_provided()
        {
            Constructed.Headers[Headers.First().Key].Should().Be(Headers.First().Value);
        }

        [Fact]
        public void should_build_the_commit_containing_all_uncommitted_events()
        {
            Constructed.Events.Count.Should().Be(Headers.Count);
        }

        [Fact]
        public void should_build_the_commit_using_the_event_messages_provided()
        {
            Constructed.Events.First().Should().Be(Uncommitted);
        }

        [Fact]
        public void should_contain_a_copy_of_the_headers_provided()
        {
            Constructed.Headers.Should().NotBeEmpty();
        }

        [Fact]
        public void should_update_the_stream_revision()
        {
            Stream.StreamRevision.Should().Be(Constructed.StreamRevision);
        }

        [Fact]
        public void should_update_the_commit_sequence()
        {
            Stream.CommitSequence.Should().Be(Constructed.CommitSequence);
        }

        [Fact]
        public void should_add_the_uncommitted_events_the_committed_events()
        {
            Stream.CommittedEvents.Last().Should().Be(Uncommitted);
        }

        [Fact]
        public void should_clear_the_uncommitted_events_on_the_stream()
        {
            Stream.UncommittedEvents.Should().BeEmpty();
        }

        [Fact]
        public void should_clear_the_uncommitted_headers_on_the_stream()
        {
            Stream.UncommittedHeaders.Should().BeEmpty();
        }

        [Fact]
        public void should_copy_the_uncommitted_headers_to_the_committed_stream_headers()
        {
            Stream.CommittedHeaders.Count.Should().Be(Headers.Count);
        }
    }

    /// <summary>
    ///     This behavior is primarily to support a NoSQL storage solution where CommitId is not being used as the "primary key"
    ///     in a NoSQL environment, we'll most likely use StreamId + CommitSequence, which also enables optimistic concurrency.
    /// </summary>
    public class when_committing_with_an_identifier_that_was_previously_read : on_the_event_stream
    {
        private ICommit[] _committed;
        private Guid _dupliateCommitId;
        private Exception _thrown;

        public when_committing_with_an_identifier_that_was_previously_read(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            _committed = new[] { BuildCommitStub(1, 1, 1) };
            _dupliateCommitId = _committed[0].CommitId;

            A.CallTo(() => Persistence.GetFrom(BucketId, StreamId, 0, int.MaxValue)).Returns(_committed);

            Stream = new OptimisticEventStream(BucketId, StreamId, Persistence, 0, int.MaxValue);
        }

        protected override void Because()
        {
            _thrown = Catch.Exception(() => Stream.CommitChanges(_dupliateCommitId));
        }

        [Fact]
        public void should_throw_a_DuplicateCommitException()
        {
            _thrown.Should().BeOfType<DuplicateCommitException>();
        }
    }

    public class when_committing_after_another_thread_or_process_has_moved_the_stream_head : on_the_event_stream
    {
        private const int StreamRevision = 1;

        private EventMessage Uncommitted
        {
            get
            { return Fixture.Variables[nameof(Uncommitted)] as EventMessage; }
            set
            { Fixture.Variables[nameof(Uncommitted)] = value; }
        }

        private ICommit[] Committed
        {
            get
            { return Fixture.Variables[nameof(Committed)] as ICommit[]; }
            set
            { Fixture.Variables[nameof(Committed)] = value; }
        }

        private ICommit[] DiscoveredOnCommit
        {
            get
            { return Fixture.Variables[nameof(DiscoveredOnCommit)] as ICommit[]; }
            set
            { Fixture.Variables[nameof(DiscoveredOnCommit)] = value; }
        }

        private Exception Thrown
        {
            get
            { return Fixture.Variables[nameof(Thrown)] as Exception; }
            set
            { Fixture.Variables[nameof(Thrown)] = value; }
        }

        public when_committing_after_another_thread_or_process_has_moved_the_stream_head(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            Uncommitted = new EventMessage { Body = string.Empty };

            Committed = new[] { BuildCommitStub(1, 1, 1) };
            DiscoveredOnCommit = new[] { BuildCommitStub(3, 2, 2) };

            A.CallTo(() => Persistence.Commit(A<CommitAttempt>._)).Throws(new ConcurrencyException());
            A.CallTo(() => Persistence.GetFrom(BucketId, StreamId, StreamRevision, int.MaxValue)).Returns(Committed);
            A.CallTo(() => Persistence.GetFrom(BucketId, StreamId, StreamRevision + 1, int.MaxValue)).Returns(DiscoveredOnCommit);

            Stream = new OptimisticEventStream(BucketId, StreamId, Persistence, StreamRevision, int.MaxValue);
            Stream.Add(Uncommitted);
        }

        protected override void Because()
        {
            Thrown = Catch.Exception(() => Stream.CommitChanges(Guid.NewGuid()));
        }

        [Fact]
        public void should_throw_a_ConcurrencyException()
        {
            Thrown.Should().BeOfType<ConcurrencyException>();
        }

        [Fact]
        public void should_query_the_underlying_storage_to_discover_the_new_commits()
        {
            A.CallTo(() => Persistence.GetFrom(BucketId, StreamId, StreamRevision + 1, int.MaxValue)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public void should_update_the_stream_revision_accordingly()
        {
            Stream.StreamRevision.Should().Be(DiscoveredOnCommit[0].StreamRevision);
        }

        [Fact]
        public void should_update_the_commit_sequence_accordingly()
        {
            Stream.CommitSequence.Should().Be(DiscoveredOnCommit[0].CommitSequence);
        }

        [Fact]
        public void should_add_the_newly_discovered_committed_events_to_the_set_of_committed_events_accordingly()
        {
            Stream.CommittedEvents.Count.Should().Be(DiscoveredOnCommit[0].Events.Count + 1);
        }
    }

    public class when_attempting_to_invoke_behavior_on_a_disposed_stream : on_the_event_stream
    {
        private Exception _thrown;

        public when_attempting_to_invoke_behavior_on_a_disposed_stream(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Stream.Dispose();
        }

        protected override void Because()
        {
            _thrown = Catch.Exception(() => Stream.CommitChanges(Guid.NewGuid()));
        }

        [Fact]
        public void should_throw_a_ObjectDisposedException()
        {
            _thrown.Should().BeOfType<ObjectDisposedException>();
        }
    }

    public class when_attempting_to_modify_the_event_collections : on_the_event_stream
    {
        public when_attempting_to_modify_the_event_collections(FakeTimeFixture fixture)
            : base(fixture)
        { }

        [Fact]
        public void should_throw_an_exception_when_adding_to_the_committed_collection()
        {
            Catch.Exception(() => Stream.CommittedEvents.Add(null)).Should().BeOfType<NotSupportedException>();

        }

        [Fact]
        public void should_throw_an_exception_when_adding_to_the_uncommitted_collection()
        {
            Catch.Exception(() => Stream.UncommittedEvents.Add(null)).Should().BeOfType<NotSupportedException>();
        }

        [Fact]
        public void should_throw_an_exception_when_clearing_the_committed_collection()
        {
            Catch.Exception(() => Stream.CommittedEvents.Clear()).Should().BeOfType<NotSupportedException>();
        }

        [Fact]
        public void should_throw_an_exception_when_clearing_the_uncommitted_collection()
        {
            Catch.Exception(() => Stream.UncommittedEvents.Clear()).Should().BeOfType<NotSupportedException>();
        }

        [Fact]
        public void should_throw_an_exception_when_removing_from_the_committed_collection()
        {
            Catch.Exception(() => Stream.CommittedEvents.Remove(null)).Should().BeOfType<NotSupportedException>();
        }

        [Fact]
        public void should_throw_an_exception_when_removing_from_the_uncommitted_collection()
        {
            Catch.Exception(() => Stream.UncommittedEvents.Remove(null)).Should().BeOfType<NotSupportedException>();
        }
    }

    public abstract class on_the_event_stream : SpecificationBase<FakeTimeFixture>
    {
        protected const int DefaultStreamRevision = 1;
        protected const int DefaultCommitSequence = 1;
        protected const string BucketId = "bucket";

        protected ICommitEvents Persistence
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(Persistence), out var persistence))
                {
                    persistence = A.Fake<ICommitEvents>();
                    Persistence = persistence as ICommitEvents;
                }

                return persistence as ICommitEvents;
            }
            private set
            { Fixture.Variables[nameof(Persistence)] = value; }
        }

        protected OptimisticEventStream Stream
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(Stream), out var stream))
                {
                    stream = new OptimisticEventStream(BucketId, StreamId, Persistence);
                    Stream = stream as OptimisticEventStream;
                }

                return stream as OptimisticEventStream;
            }
            set
            { Fixture.Variables[nameof(Stream)] = value; }
        }

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
            private set
            { Fixture.Variables[nameof(StreamId)] = value; }
        }

        public on_the_event_stream(FakeTimeFixture fixture)
            : base(fixture)
        { }

        protected ICommit BuildCommitStub(int revision, int sequence, int eventCount)
        {
            var events = new List<EventMessage>(eventCount);
            for (int i = 0; i < eventCount; i++)
            {
                events.Add(new EventMessage());
            }

            return new Commit(Bucket.Default, StreamId, revision, Guid.NewGuid(), sequence, SystemTime.UtcNow, new LongCheckpoint(0).Value, null, events);
        }
    }

    public class FakeTimeFixture : TestFixture, IDisposable
    {
        public FakeTimeFixture()
        {
            SystemTime.Resolver = () => new DateTime(2012, 1, 1, 13, 0, 0);
        }

        public void Dispose()
        {
            SystemTime.Resolver = null;
        }
    }
}

#pragma warning restore 169