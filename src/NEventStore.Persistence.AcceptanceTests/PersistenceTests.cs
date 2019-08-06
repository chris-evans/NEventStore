#pragma warning disable 169
// ReSharper disable InconsistentNaming

namespace NEventStore.Persistence.AcceptanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FluentAssertions;
    using NEventStore.Diagnostics;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using NEventStore.Persistence.InMemory;
    using Xunit;

    public class when_a_commit_header_has_a_name_that_contains_a_period : PersistenceEngineConcern
    {
        private ICommit _persisted;
        private string _streamId;

        public when_a_commit_header_has_a_name_that_contains_a_period(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            _streamId = Guid.NewGuid().ToString();
            var attempt = new CommitAttempt(_streamId,
                2,
                Guid.NewGuid(),
                1,
                DateTime.Now,
                new Dictionary<string, object> { { "key.1", "value" } },
                new List<EventMessage> { new EventMessage { Body = new ExtensionMethods.SomeDomainEvent { SomeProperty = "Test" } } });
            Persistence.Commit(attempt);
        }

        protected override void Because()
        {
            _persisted = Persistence.GetFrom(_streamId, 0, int.MaxValue).First();
        }

        [Fact]
        public void should_correctly_deserialize_headers()
        {
            _persisted.Headers.Keys.Should().Contain("key.1");
        }
    }

    public class when_a_commit_is_successfully_persisted : PersistenceEngineConcern
    {
        private CommitAttempt Attempt
        {
            get
            { return Fixture.Variables[nameof(Attempt)] as CommitAttempt; }
            set
            { Fixture.Variables[nameof(Attempt)] = value; }
        }
        private DateTime Now
        {
            get
            { return (DateTime)Fixture.Variables[nameof(Now)]; }
            set
            { Fixture.Variables[nameof(Now)] = value; }
        }

        private ICommit Persisted
        {
            get
            { return Fixture.Variables[nameof(Persisted)] as ICommit; }
            set
            { Fixture.Variables[nameof(Persisted)] = value; }
        }

        private string StreamId
        {
            get
            { return Fixture.Variables[nameof(StreamId)] as string; }
            set
            { Fixture.Variables[nameof(StreamId)] = value; }
        }

        public when_a_commit_is_successfully_persisted(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Now = SystemTime.UtcNow.AddYears(1);
            StreamId = Guid.NewGuid().ToString();
            Attempt = StreamId.BuildAttempt(Now);

            Persistence.Commit(Attempt);
        }

        protected override void Because()
        { Persisted = Persistence.GetFrom(StreamId, 0, int.MaxValue).First(); }

        [Fact]
        public void should_correctly_persist_the_stream_identifier()
        { Persisted.StreamId.Should().Be(Attempt.StreamId); }

        [Fact]
        public void should_correctly_persist_the_stream_stream_revision()
        { Persisted.StreamRevision.Should().Be(Attempt.StreamRevision); }

        [Fact]
        public void should_correctly_persist_the_commit_identifier()
        { Persisted.CommitId.Should().Be(Attempt.CommitId); }

        [Fact]
        public void should_correctly_persist_the_commit_sequence()
        { Persisted.CommitSequence.Should().Be(Attempt.CommitSequence); }

        // persistence engines have varying levels of precision with respect to time.
        [Fact]
        public void should_correctly_persist_the_commit_stamp()
        {
            var difference = Persisted.CommitStamp.Subtract(Now);
            difference.Days.Should().Be(0);
            difference.Hours.Should().Be(0);
            difference.Minutes.Should().Be(0);
            difference.Should().BeLessOrEqualTo(TimeSpan.FromSeconds(1));
        }

        [Fact]
        public void should_correctly_persist_the_headers()
        { Persisted.Headers.Count.Should().Be(Attempt.Headers.Count); }

        [Fact]
        public void should_correctly_persist_the_events()
        { Persisted.Events.Count.Should().Be(Attempt.Events.Count); }

        [Fact]
        public void should_add_the_commit_to_the_set_of_undispatched_commits()
        { Persistence.GetUndispatchedCommits().FirstOrDefault(x => x.CommitId == Attempt.CommitId).Should().NotBeNull(); }

        [Fact]
        public void should_cause_the_stream_to_be_found_in_the_list_of_streams_to_snapshot()
        { Persistence.GetStreamsToSnapshot(1).FirstOrDefault(x => x.StreamId == StreamId).Should().NotBeNull();}
    }

    public class when_reading_from_a_given_revision : PersistenceEngineConcern
    {
        private const int LoadFromCommitContainingRevision = 3;
        private const int UpToCommitWithContainingRevision = 5;

        private ICommit[] Committed
        {
            get
            { return Fixture.Variables["committed"] as ICommit[]; }
            set
            { Fixture.Variables["committed"] = value; }
        }

        private ICommit Oldest
        {
            get
            { return Fixture.Variables["oldest"] as ICommit; }
            set
            { Fixture.Variables["oldest"] = value; }
        }

        private ICommit Oldest2
        {
            get
            { return Fixture.Variables["oldest2"] as ICommit; }
            set
            { Fixture.Variables["oldest2"] = value; }
        }

        private ICommit Oldest3
        {
            get
            { return Fixture.Variables["oldest3"] as ICommit; }
            set
            { Fixture.Variables["oldest3"] = value; }
        }

        private string StreamId
        {
            get
            { return Fixture.Variables["streamId"] as string; }
            set
            { Fixture.Variables["streamId"] = value; }
        }

        public when_reading_from_a_given_revision(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Oldest = Persistence.CommitSingle(); // 2 events, revision 1-2
            Oldest2 = Persistence.CommitNext(Oldest); // 2 events, revision 3-4
            Oldest3 = Persistence.CommitNext(Oldest2); // 2 events, revision 5-6
            Persistence.CommitNext(Oldest3); // 2 events, revision 7-8

            StreamId = Oldest.StreamId;
        }

        protected override void Because()
        {
            Committed = Persistence.GetFrom(StreamId, LoadFromCommitContainingRevision, UpToCommitWithContainingRevision).ToArray();
        }

        [Fact]
        public void should_start_from_the_commit_which_contains_the_min_stream_revision_specified()
        {
            Committed.First().CommitId.Should().Be(Oldest2.CommitId); // contains revision 3
        }

        [Fact]
        public void should_read_up_to_the_commit_which_contains_the_max_stream_revision_specified()
        {
            Committed.Last().CommitId.Should().Be(Oldest3.CommitId); // contains revision 5
        }
    }

    public class when_reading_from_a_given_revision_to_commit_revision : PersistenceEngineConcern
    {
        private const int LoadFromCommitContainingRevision = 3;
        private const int UpToCommitWithContainingRevision = 6;

        private ICommit[] Committed
        {
            get
            { return Fixture.Variables["committed"] as ICommit[]; }
            set
            { Fixture.Variables["committed"] = value; }
        }

        private ICommit Oldest
        {
            get
            { return Fixture.Variables["oldest"] as ICommit; }
            set
            { Fixture.Variables["oldest"] = value; }
        }

        private ICommit Oldest2
        {
            get
            { return Fixture.Variables["oldest2"] as ICommit; }
            set
            { Fixture.Variables["oldest2"] = value; }
        }

        private ICommit Oldest3
        {
            get
            { return Fixture.Variables["oldest3"] as ICommit; }
            set
            { Fixture.Variables["oldest3"] = value; }
        }

        private string StreamId
        {
            get
            { return Fixture.Variables["streamId"] as string; }
            set
            { Fixture.Variables["streamId"] = value; }
        }

        public when_reading_from_a_given_revision_to_commit_revision(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Oldest = Persistence.CommitSingle(); // 2 events, revision 1-2
            Oldest2 = Persistence.CommitNext(Oldest); // 2 events, revision 3-4
            Oldest3 = Persistence.CommitNext(Oldest2); // 2 events, revision 5-6
            Persistence.CommitNext(Oldest3); // 2 events, revision 7-8

            StreamId = Oldest.StreamId;
        }

        protected override void Because()
        {
            Committed = Persistence.GetFrom(StreamId, LoadFromCommitContainingRevision, UpToCommitWithContainingRevision).ToArray();
        }

        [Fact]
        public void should_start_from_the_commit_which_contains_the_min_stream_revision_specified()
        {
            Committed.First().CommitId.Should().Be(Oldest2.CommitId); // contains revision 3
        }

        [Fact]
        public void should_read_up_to_the_commit_which_contains_the_max_stream_revision_specified()
        {
            Committed.Last().CommitId.Should().Be(Oldest3.CommitId); // contains revision 6
        }
    }

    public class when_committing_a_stream_with_the_same_revision : PersistenceEngineConcern

    {
        private CommitAttempt _attemptWithSameRevision;
        private Exception _thrown;

        public when_committing_a_stream_with_the_same_revision(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            ICommit commit = Persistence.CommitSingle();
            _attemptWithSameRevision = commit.StreamId.BuildAttempt();
        }

        protected override void Because()
        {
            _thrown = Catch.Exception(() => Persistence.Commit(_attemptWithSameRevision));
        }

        [Fact]
        public void should_throw_a_ConcurrencyException()
        {
            _thrown.Should().BeOfType<ConcurrencyException>();
        }
    }

    //TODO:This test looks exactly like the one above. What are we trying to prove?
    public class when_committing_a_stream_with_the_same_sequence : PersistenceEngineConcern

    {
        private CommitAttempt _attempt1, _attempt2;
        private Exception _thrown;

        public when_committing_a_stream_with_the_same_sequence(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            string streamId = Guid.NewGuid().ToString();
            _attempt1 = streamId.BuildAttempt();
            _attempt2 = streamId.BuildAttempt(); //TODO mutate a bit

            Persistence.Commit(_attempt1);
        }

        protected override void Because()
        {
            _thrown = Catch.Exception(() => Persistence.Commit(_attempt2));
        }

        [Fact]
        public void should_throw_a_ConcurrencyException()
        {
            _thrown.Should().BeOfType<ConcurrencyException>();
        }
    }

    //TODO:This test looks exactly like the one above. What are we trying to prove?
    public class when_attempting_to_overwrite_a_committed_sequence : PersistenceEngineConcern
    {
        private CommitAttempt _failedAttempt;
        private Exception _thrown;

        public when_attempting_to_overwrite_a_committed_sequence(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            string streamId = Guid.NewGuid().ToString();
            CommitAttempt successfulAttempt = streamId.BuildAttempt();
            Persistence.Commit(successfulAttempt);
            _failedAttempt = streamId.BuildAttempt();
        }

        protected override void Because()
        {
            _thrown = Catch.Exception(() => Persistence.Commit(_failedAttempt));
        }

        [Fact]
        public void should_throw_a_ConcurrencyException()
        {
            _thrown.Should().BeOfType<ConcurrencyException>();
        }
    }

    public class when_attempting_to_persist_a_commit_twice : PersistenceEngineConcern
    {
        private CommitAttempt _attemptTwice;
        private Exception _thrown;

        public when_attempting_to_persist_a_commit_twice(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            var commit = Persistence.CommitSingle();
            _attemptTwice = new CommitAttempt(
                commit.BucketId,
                commit.StreamId,
                commit.StreamRevision,
                commit.CommitId,
                commit.CommitSequence,
                commit.CommitStamp,
                commit.Headers,
                commit.Events);
        }

        protected override void Because()
        {
            _thrown = Catch.Exception(() => Persistence.Commit(_attemptTwice));
        }

        [Fact]
        public void should_throw_a_DuplicateCommitException()
        {
            _thrown.Should().BeOfType<DuplicateCommitException>();
        }
    }

    public class when_a_commit_has_been_marked_as_dispatched : PersistenceEngineConcern
    {
        private ICommit _commit;

        public when_a_commit_has_been_marked_as_dispatched(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            _commit = Persistence.CommitSingle();
        }

        protected override void Because()
        {
            Persistence.MarkCommitAsDispatched(_commit);
        }

        [Fact]
        public void should_no_longer_be_found_in_the_set_of_undispatched_commits()
        {
            Persistence.GetUndispatchedCommits().FirstOrDefault(x => x.CommitId == _commit.CommitId).Should().BeNull();
        }
    }

    public class when_committing_more_events_than_the_configured_page_size : PersistenceEngineConcern
    {
        private string StreamId
        {
            get
            { return Fixture.Variables["streamId"] as string; }
            set
            { Fixture.Variables["streamId"] = value; }
        }

        private ICommit[] Loaded
        {
            get
            { return Fixture.Variables["loaded"] as ICommit[]; }
            set
            { Fixture.Variables["loaded"] = value; }
        }

        private CommitAttempt[] Committed
        {
            get
            { return Fixture.Variables["committed"] as CommitAttempt[]; }
            set
            { Fixture.Variables["committed"] = value; }
        }

        public when_committing_more_events_than_the_configured_page_size(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            StreamId = Guid.NewGuid().ToString();
            Committed = Persistence.CommitMany(ConfiguredPageSizeForTesting + 2, StreamId).ToArray();
        }

        protected override void Because()
        {
            Loaded = Persistence.GetFrom(StreamId, 0, int.MaxValue).ToArray();
        }

        [Fact]
        public void should_load_the_same_number_of_commits_which_have_been_persisted()
        {
            Loaded.Length.Should().Be(Committed.Length);
        }

        [Fact]
        public void should_load_the_same_commits_which_have_been_persisted()
        {
            Committed
                .All(commit => Loaded.SingleOrDefault(loaded => loaded.CommitId == commit.CommitId) != null)
                .Should().BeTrue();
        }
    }

    public class when_saving_a_snapshot : PersistenceEngineConcern
    {
        private string StreamId
        {
            get
            { return Fixture.Variables["streamId"] as string; }
            set
            { Fixture.Variables["streamId"] = value; }
        }

        private bool Added
        {
            get
            { return (bool) Fixture.Variables["added"]; }
            set
            { Fixture.Variables["added"] = value; }
        }

        private Snapshot Snapshot
        {
            get
            { return Fixture.Variables["snapshot"] as Snapshot; }
            set
            { Fixture.Variables["snapshot"] = value; }
        }

        public when_saving_a_snapshot(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            StreamId = Guid.NewGuid().ToString();
            Snapshot = new Snapshot(StreamId, 1, "Snapshot");
            Persistence.CommitSingle(StreamId);
        }

        protected override void Because()
        {
            Added = Persistence.AddSnapshot(Snapshot);
        }

        [Fact]
        public void should_indicate_the_snapshot_was_added()
        {
            Added.Should().BeTrue();
        }

        [Fact]
        public void should_be_able_to_retrieve_the_snapshot()
        {
            Persistence.GetSnapshot(StreamId, Snapshot.StreamRevision).Should().NotBeNull();
        }
    }

    public class when_retrieving_a_snapshot : PersistenceEngineConcern
    {
        public ISnapshot Correct
        {
            get
            { return Fixture.Variables["correct"] as ISnapshot; }
            set
            { Fixture.Variables["correct"] = value; }
        }

        public ISnapshot Snapshot
        {
            get
            { return Fixture.Variables["snapshot"] as ISnapshot; }
            set
            { Fixture.Variables["snapshot"] = value; }
        }

        public ISnapshot TooFarForward
        {
            get
            { return Fixture.Variables["tooFarForward"] as ISnapshot; }
            set
            { Fixture.Variables["tooFarForward"] = value; }
        }

        public string StreamId
        {
            get
            { return Fixture.Variables["streamId"] as string; }
            set
            { Fixture.Variables["streamId"] = value; }
        }

        public when_retrieving_a_snapshot(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            StreamId = Guid.NewGuid().ToString();
            ICommit commit1 = Persistence.CommitSingle(StreamId); // rev 1-2
            ICommit commit2 = Persistence.CommitNext(commit1); // rev 3-4
            Persistence.CommitNext(commit2); // rev 5-6

            Persistence.AddSnapshot(new Snapshot(StreamId, 1, string.Empty)); //Too far back
            Persistence.AddSnapshot(Correct = new Snapshot(StreamId, 3, "Snapshot"));
            Persistence.AddSnapshot(TooFarForward = new Snapshot(StreamId, 5, string.Empty));
        }

        protected override void Because()
        {
            Snapshot = Persistence.GetSnapshot(StreamId, TooFarForward.StreamRevision - 1);
        }

        [Fact]
        public void should_load_the_most_recent_prior_snapshot()
        {
            Snapshot.StreamRevision.Should().Be(Correct.StreamRevision);
        }

        [Fact]
        public void should_have_the_correct_snapshot_payload()
        {
            Snapshot.Payload.Should().Be(Correct.Payload);
        }
    }

    public class when_a_snapshot_has_been_added_to_the_most_recent_commit_of_a_stream : PersistenceEngineConcern
    {
        private const string SnapshotData = "snapshot";
        private ICommit _newest;
        private ICommit _oldest, _oldest2;
        private string _streamId;

        public when_a_snapshot_has_been_added_to_the_most_recent_commit_of_a_stream(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            _streamId = Guid.NewGuid().ToString();
            _oldest = Persistence.CommitSingle(_streamId);
            _oldest2 = Persistence.CommitNext(_oldest);
            _newest = Persistence.CommitNext(_oldest2);
        }

        protected override void Because()
        {
            Persistence.AddSnapshot(new Snapshot(_streamId, _newest.StreamRevision, SnapshotData));
        }

        [Fact]
        public void should_no_longer_find_the_stream_in_the_set_of_streams_to_be_snapshot()
        {
            Persistence.GetStreamsToSnapshot(1).Any(x => x.StreamId == _streamId).Should().BeFalse();
        }
    }

    public class when_adding_a_commit_after_a_snapshot : PersistenceEngineConcern
    {
        private const string SnapshotData = "snapshot";
        private const int WithinThreshold = 2;
        private const int OverThreshold = 3;

        private string StreamId
        {
            get
            { return Fixture.Variables["streamId"] as string; }
            set
            { Fixture.Variables["streamId"] = value; }
        }

        private ICommit Oldest
        {
            get
            { return Fixture.Variables["oldest"] as ICommit; }
            set
            { Fixture.Variables["oldest"] = value; }
        }

        private ICommit Oldest2
        {
            get
            { return Fixture.Variables["oldest2"] as ICommit; }
            set
            { Fixture.Variables["oldest2"] = value; }
        }



        public when_adding_a_commit_after_a_snapshot(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            StreamId = Guid.NewGuid().ToString();
            Oldest = Persistence.CommitSingle(StreamId);
            Oldest2 = Persistence.CommitNext(Oldest);
            Persistence.AddSnapshot(new Snapshot(StreamId, Oldest2.StreamRevision, SnapshotData));
        }

        protected override void Because()
        {
            Persistence.Commit(Oldest2.BuildNextAttempt());
        }

        // Because Raven and Mongo update the stream head asynchronously, occasionally will fail this test
        [Fact]
        public void should_find_the_stream_in_the_set_of_streams_to_be_snapshot_when_within_the_threshold()
        {
            Persistence.GetStreamsToSnapshot(WithinThreshold).FirstOrDefault(x => x.StreamId == StreamId).Should().NotBeNull();
        }

        [Fact]
        public void should_not_find_the_stream_in_the_set_of_streams_to_be_snapshot_when_over_the_threshold()
        {
            Persistence.GetStreamsToSnapshot(OverThreshold).Any(x => x.StreamId == StreamId).Should().BeFalse();
        }
    }

    public class when_reading_all_commits_from_a_particular_point_in_time : PersistenceEngineConcern
    {
        private ICommit[] _committed;
        private CommitAttempt _first;
        private DateTime _now;
        private ICommit _second;
        private string _streamId;
        private ICommit _third;

        public when_reading_all_commits_from_a_particular_point_in_time(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            _streamId = Guid.NewGuid().ToString();

            _now = SystemTime.UtcNow.AddYears(1);
            _first = _streamId.BuildAttempt(_now.AddSeconds(1));
            Persistence.Commit(_first);

            _second = Persistence.CommitNext(_first);
            _third = Persistence.CommitNext(_second);
            Persistence.CommitNext(_third);
        }

        protected override void Because()
        {
            _committed = Persistence.GetFrom(_now).ToArray();
        }

        [Fact]
        public void should_return_all_commits_on_or_after_the_point_in_time_specified()
        {
            _committed.Length.Should().Be(4);
        }
    }

    public class when_paging_over_all_commits_from_a_particular_point_in_time : PersistenceEngineConcern
    {
        private DateTime Start
        {
            get
            { return (DateTime)Fixture.Variables["start"]; }
            set
            { Fixture.Variables["start"] = value; }
        }

        private CommitAttempt[] Committed
        {
            get
            { return Fixture.Variables["committed"] as CommitAttempt[]; }
            set
            { Fixture.Variables["committed"] = value; }
        }

        private ICommit[] Loaded
        {
            get
            { return Fixture.Variables["loaded"] as ICommit[]; }
            set
            { Fixture.Variables["loaded"] = value; }
        }

        public when_paging_over_all_commits_from_a_particular_point_in_time(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Start = SystemTime.UtcNow;
            // Due to loss in precision in various storage engines, we're rounding down to the
            // nearest second to ensure include all commits from the 'start'.
            Start = Start.AddSeconds(-1);
            Committed = Persistence.CommitMany(ConfiguredPageSizeForTesting + 2).ToArray();
        }

        protected override void Because()
        {
            Loaded = Persistence.GetFrom(Start).ToArray();
        }

        [Fact]
        public void should_load_the_same_number_of_commits_which_have_been_persisted()
        {
            Loaded.Length.Should().Be(Committed.Length);
        }

        [Fact]
        public void should_load_the_same_commits_which_have_been_persisted()
        {
            Committed
                .All(commit => Loaded.SingleOrDefault(loaded => loaded.CommitId == commit.CommitId) != null)
                .Should().BeTrue();
        }
    }

    public class when_paging_over_all_commits_from_a_particular_checkpoint : PersistenceEngineConcern
    {
        private const int checkPoint = 2;

        public when_paging_over_all_commits_from_a_particular_checkpoint(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            var committed = Persistence.CommitMany(ConfiguredPageSizeForTesting + 1).Select(c => c.CommitId).ToList();
            Fixture.Variables.Add("committed", committed);
        }

        protected override void Because()
        {
            var loaded = Persistence.GetFrom(checkPoint.ToString()).Select(c => c.CommitId).ToList();
            Fixture.Variables.Add("loaded", loaded);
        }

        [Fact]
        public void should_load_the_same_number_of_commits_which_have_been_persisted_starting_from_the_checkpoint()
        {
            var loaded = Fixture.Variables["loaded"] as List<Guid>;
            var committed = Fixture.Variables["committed"] as List<Guid>;
            loaded.Count.Should().Be(committed.Count() - checkPoint);
        }

        [Fact]
        public void should_load_only_the_commits_starting_from_the_checkpoint()
        {
            var loaded = Fixture.Variables["loaded"] as List<Guid>;
            var committed = Fixture.Variables["committed"] as List<Guid>;
            committed.Skip(checkPoint).All(x => loaded.Contains(x)).Should().BeTrue(); // all commits should be found in loaded collection
        }
    }

    public class when_reading_all_commits_from_the_year_1_AD : PersistenceEngineConcern
    {
        private Exception _thrown;

        public when_reading_all_commits_from_the_year_1_AD(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            _thrown = Catch.Exception(() => Persistence.GetFrom(DateTime.MinValue).FirstOrDefault());
        }

        [Fact]
        public void should_NOT_throw_an_exception()
        {
            _thrown.Should().BeNull();
        }
    }

    public class when_purging_all_commits : PersistenceEngineConcern
    {
        public when_purging_all_commits(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Persistence.CommitSingle();
        }

        protected override void Because()
        {
            Persistence.Purge();
        }

        [Fact]
        public void should_not_find_any_commits_stored()
        {
            Persistence.GetFrom(DateTime.MinValue).Count().Should().Be(0);
        }

        [Fact]
        public void should_not_find_any_streams_to_snapshot()
        {
            Persistence.GetStreamsToSnapshot(0).Count().Should().Be(0);
        }

        [Fact]
        public void should_not_find_any_undispatched_commits()
        {
            Persistence.GetUndispatchedCommits().Count().Should().Be(0);
        }
    }

    public class when_invoking_after_disposal : PersistenceEngineConcern
    {
        private Exception _thrown;

        public when_invoking_after_disposal(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Persistence.Dispose();
        }

        protected override void Because()
        {
            _thrown = Catch.Exception(() => Persistence.CommitSingle());
        }

        [Fact]
        public void should_throw_an_ObjectDisposedException()
        {
            _thrown.Should().BeOfType<ObjectDisposedException>();
        }
    }

    public class when_committing_a_stream_with_the_same_id_as_a_stream_in_another_bucket : PersistenceEngineConcern
    {
        const string _bucketAId = "a";
        const string _bucketBId = "b";

        private string StreamId
        {
            get
            { return Fixture.Variables["streamId"] as string; }
            set
            { Fixture.Variables["streamId"] = value; }
        }

        private CommitAttempt AttemptForBucketB
        {
            get
            { return Fixture.Variables["attemptForBucketB"] as CommitAttempt; }
            set
            { Fixture.Variables["attemptForBucketB"] = value; }
        }

        private Exception Thrown
        {
            get
            { return Fixture.Variables["thrown"] as Exception; }
            set
            { Fixture.Variables["thrown"] = value; }
        }

        private DateTime AttemptACommitStamp
        {
            get
            { return (DateTime) Fixture.Variables["attemptACommitStamp"]; }
            set
            { Fixture.Variables["attemptACommitStamp"] = value; }
        }

        public when_committing_a_stream_with_the_same_id_as_a_stream_in_another_bucket(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            StreamId = Guid.NewGuid().ToString();
            DateTime now = SystemTime.UtcNow;
            Persistence.Commit(StreamId.BuildAttempt(now, _bucketAId));
            AttemptACommitStamp = Persistence.GetFrom(_bucketAId, StreamId, 0, int.MaxValue).First().CommitStamp;
            AttemptForBucketB = StreamId.BuildAttempt(now.Subtract(TimeSpan.FromDays(1)), _bucketBId);
        }

        protected override void Because()
        {
            Thrown = Catch.Exception(() => Persistence.Commit(AttemptForBucketB));
        }

        [Fact]
        public void should_succeed()
        {
            Thrown.Should().BeNull();
        }

        [Fact]
        public void should_persist_to_the_correct_bucket()
        {
            ICommit[] stream = Persistence.GetFrom(_bucketBId, StreamId, 0, int.MaxValue).ToArray();
            stream.Should().NotBeNull();
            stream.Count().Should().Be(1);
        }

        [Fact]
        public void should_not_affect_the_stream_from_the_other_bucket()
        {
            ICommit[] stream = Persistence.GetFrom(_bucketAId, StreamId, 0, int.MaxValue).ToArray();
            stream.Should().NotBeNull();
            stream.Count().Should().Be(1);
            stream.First().CommitStamp.Should().Be(AttemptACommitStamp);
        }
    }

    public class when_saving_a_snapshot_for_a_stream_with_the_same_id_as_a_stream_in_another_bucket : PersistenceEngineConcern
    {
        const string _bucketAId = "a";
        const string _bucketBId = "b";

        string _streamId;

        private static Snapshot _snapshot;

        public when_saving_a_snapshot_for_a_stream_with_the_same_id_as_a_stream_in_another_bucket(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            _streamId = Guid.NewGuid().ToString();
            _snapshot = new Snapshot(_bucketBId, _streamId, 1, "Snapshot");
            Persistence.Commit(_streamId.BuildAttempt(bucketId: _bucketAId));
            Persistence.Commit(_streamId.BuildAttempt(bucketId: _bucketBId));
        }

        protected override void Because()
        {
            Persistence.AddSnapshot(_snapshot);
        }

        [Fact]
        public void should_affect_snapshots_from_another_bucket()
        {
            Persistence.GetSnapshot(_bucketAId, _streamId, _snapshot.StreamRevision).Should().BeNull();
        }
    }

    public class when_reading_all_commits_from_a_particular_point_in_time_and_there_are_streams_in_multiple_buckets : PersistenceEngineConcern
    {
        const string _bucketAId = "a";
        const string _bucketBId = "b";

        private static DateTime _now;
        private static ICommit[] _returnedCommits;
        private CommitAttempt _commitToBucketB;

        public when_reading_all_commits_from_a_particular_point_in_time_and_there_are_streams_in_multiple_buckets(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            _now = SystemTime.UtcNow.AddYears(1);

            var commitToBucketA = Guid.NewGuid().ToString().BuildAttempt(_now.AddSeconds(1), _bucketAId);

            Persistence.Commit(commitToBucketA);
            Persistence.Commit(commitToBucketA = commitToBucketA.BuildNextAttempt());
            Persistence.Commit(commitToBucketA = commitToBucketA.BuildNextAttempt());
            Persistence.Commit(commitToBucketA.BuildNextAttempt());

            _commitToBucketB = Guid.NewGuid().ToString().BuildAttempt(_now.AddSeconds(1), _bucketBId);

            Persistence.Commit(_commitToBucketB);
        }

        protected override void Because()
        {
            _returnedCommits = Persistence.GetFrom(_bucketAId, _now).ToArray();
        }

        [Fact]
        public void should_not_return_commits_from_other_buckets()
        {
            _returnedCommits.Any(c => c.CommitId.Equals(_commitToBucketB.CommitId)).Should().BeFalse();
        }
    }

    public class when_getting_all_commits_since_checkpoint_and_there_are_streams_in_multiple_buckets : PersistenceEngineConcern
    {
        private ICommit[] Commits
        {
            get
            { return Fixture.Variables[nameof(Commits)] as ICommit[]; }
            set
            { Fixture.Variables[nameof(Commits)] = value; }
        }

        public when_getting_all_commits_since_checkpoint_and_there_are_streams_in_multiple_buckets(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            const string bucketAId = "a";
            const string bucketBId = "b";
            Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt(bucketId: bucketAId));
            Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt(bucketId: bucketBId));
            Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt(bucketId: bucketAId));
        }

        protected override void Because()
        {
            Commits = Persistence.GetFromStart().ToArray();
        }

        [Fact]
        public void should_not_be_empty()
        {
            Commits.Should().NotBeNull();
        }

        [Fact]
        public void should_be_in_order_by_checkpoint()
        {
            ICheckpoint checkpoint = Persistence.GetCheckpoint();
            foreach (var commit in Commits)
            {
                ICheckpoint commitCheckpoint = Persistence.GetCheckpoint(commit.CheckpointToken);
                commitCheckpoint.Should().BeGreaterThan(checkpoint);
                checkpoint = Persistence.GetCheckpoint(commit.CheckpointToken);
            }
        }
    }

    public class when_purging_all_commits_and_there_are_streams_in_multiple_buckets : PersistenceEngineConcern
    {
        const string _bucketAId = "a";
        const string _bucketBId = "b";

        string _streamId;

        public when_purging_all_commits_and_there_are_streams_in_multiple_buckets(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            _streamId = Guid.NewGuid().ToString();
            Persistence.Commit(_streamId.BuildAttempt(bucketId: _bucketAId));
            Persistence.Commit(_streamId.BuildAttempt(bucketId: _bucketBId));
        }

        protected override void Because()
        {
            Persistence.Purge();
        }

        [Fact]
        public void should_purge_all_commits_stored_in_bucket_a()
        {
            Persistence.GetFrom(_bucketAId, DateTime.MinValue).Count().Should().Be(0);
        }

        [Fact]
        public void should_purge_all_commits_stored_in_bucket_b()
        {
            Persistence.GetFrom(_bucketBId, DateTime.MinValue).Count().Should().Be(0);
        }

        [Fact]
        public void should_purge_all_streams_to_snapshot_in_bucket_a()
        {
            Persistence.GetStreamsToSnapshot(_bucketAId, 0).Count().Should().Be(0);
        }

        [Fact]
        public void should_purge_all_streams_to_snapshot_in_bucket_b()
        {
            Persistence.GetStreamsToSnapshot(_bucketBId, 0).Count().Should().Be(0);
        }

        [Fact]
        public void should_purge_all_undispatched_commits()
        {
            Persistence.GetUndispatchedCommits().Count().Should().Be(0);
        }
    }

    public class when_gettingfromcheckpoint_amount_of_commits_exceeds_pagesize : PersistenceEngineConcern
    {
        private ICommit[] _commits;
        private int _moreThanPageSize;

        public when_gettingfromcheckpoint_amount_of_commits_exceeds_pagesize(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            _moreThanPageSize = ConfiguredPageSizeForTesting + 1;
            var eventStore = new OptimisticEventStore(Persistence, null);
            // TODO: Not sure how to set the actual pagesize to the const defined above
            for (int i = 0; i < _moreThanPageSize; i++)
            {
                using (IEventStream stream = eventStore.OpenStream(Guid.NewGuid()))
                {
                    stream.Add(new EventMessage { Body = i });
                    stream.CommitChanges(Guid.NewGuid());
                }
            }
            ICommit[] commits = Persistence.GetFrom(DateTime.MinValue).ToArray();
            _commits = Persistence.GetFrom().ToArray();
        }

        [Fact]
        public void Should_have_expected_number_of_commits()
        {
            _commits.Length.Should().Be(_moreThanPageSize);
        }
    }

    public class when_a_payload_is_large : PersistenceEngineConcern
    {
        public when_a_payload_is_large(TestFixture fixture)
            : base(fixture)
        { }

        [Fact]
        public void can_commit()
        {
            const int bodyLength = 100000;
            var attempt = new CommitAttempt(
                Bucket.Default,
                Guid.NewGuid().ToString(),
                1,
                Guid.NewGuid(),
                1,
                DateTime.UtcNow,
                new Dictionary<string, object>(),
                new List<EventMessage> { new EventMessage { Body = new string('a', bodyLength) } });
            Persistence.Commit(attempt);

            ICommit commits = Persistence.GetFrom().Single();
            commits.Events.Single().Body.ToString().Length.Should().Be(bodyLength);
        }
    }

    public abstract class PersistenceEngineConcern : SpecificationBase<TestFixture>
    {
        public PersistenceEngineConcern(TestFixture fixture)
            : base(fixture)
        { }

        protected IPersistStreams Persistence
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(Persistence), out var persistence))
                {
                    persistence = new PerformanceCounterPersistenceEngine(new InMemoryPersistenceEngine(), "tests");
                    ((IPersistStreams)persistence).Initialize();
                    Persistence = persistence as IPersistStreams;
                }

                return persistence as IPersistStreams;
            }
            set
            { Fixture.Variables[nameof(Persistence)] = value; }
        }

        protected int ConfiguredPageSizeForTesting
        {
            get { return 2; }
        }
    }
}
