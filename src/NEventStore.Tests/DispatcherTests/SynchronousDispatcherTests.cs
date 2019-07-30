namespace NEventStore.DispatcherTests
{
    using FakeItEasy;
    using NEventStore.Dispatcher;
    using NEventStore.Persistence;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using Xunit;

    public class when_instantiating_the_synchronous_dispatch_scheduler : SpecificationBase<TestFixture>
    {
        public IDispatchCommits Dispatcher
        {
            get
            { return Fixture.Variables["dispatcher"] as IDispatchCommits; }
            set
            { Fixture.Variables["dispatcher"] = value; }
        }

        public IPersistStreams Persistence
        {
            get
            { return Fixture.Variables["persistence"] as IPersistStreams; }
            set
            { Fixture.Variables["persistence"] = value; }
        }

        private SynchronousDispatchScheduler DispatchScheduler
        {
            get
            { return Fixture.Variables["dispatchScheduler"] as SynchronousDispatchScheduler; }
            set
            { Fixture.Variables["dispatchScheduler"] = value; }
        }

        public ICommit[] Commits
        {
            get
            { return Fixture.Variables["commits"] as ICommit[]; }
            set
            { Fixture.Variables["commits"] = value; }
        }

        public ICommit FirstCommit
        {
            get
            { return Fixture.Variables["firstCommit"] as ICommit; }
            set
            { Fixture.Variables["firstCommit"] = value; }
        }

        public ICommit LastCommit
        {
            get
            { return Fixture.Variables["lastCommit"] as ICommit; }
            set
            { Fixture.Variables["lastCommit"] = value; }
        }

        public when_instantiating_the_synchronous_dispatch_scheduler(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Dispatcher = A.Fake<IDispatchCommits>();
            Persistence = A.Fake<IPersistStreams>();

            Commits = new[]
            {
                FirstCommit = CommitHelper.Create(),
                LastCommit = CommitHelper.Create()
            };

            A.CallTo(() => Persistence.GetUndispatchedCommits()).Returns(Commits);
        }

        protected override void Because()
        {
            DispatchScheduler = new SynchronousDispatchScheduler(Dispatcher, Persistence);
            DispatchScheduler.Start();
        }

        [Fact]
        public void should_initialize_the_persistence_engine()
        {
            A.CallTo(() => Persistence.Initialize()).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public void should_get_the_set_of_undispatched_commits()
        {
            A.CallTo(() => Persistence.GetUndispatchedCommits()).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public void should_provide_the_commits_to_the_dispatcher()
        {
            A.CallTo(() => Dispatcher.Dispatch(FirstCommit)).MustHaveHappened();
            A.CallTo(() => Dispatcher.Dispatch(LastCommit)).MustHaveHappened();
        }
    }

    public class when_synchronously_scheduling_a_commit_for_dispatch : SpecificationBase<TestFixture>
    {
        public IDispatchCommits Dispatcher
        {
            get
            { return Fixture.Variables["dispatcher"] as IDispatchCommits; }
            set
            { Fixture.Variables["dispatcher"] = value; }
        }

        public IPersistStreams Persistence
        {
            get
            { return Fixture.Variables["persistence"] as IPersistStreams; }
            set
            { Fixture.Variables["persistence"] = value; }
        }

        private SynchronousDispatchScheduler DispatchScheduler
        {
            get
            { return Fixture.Variables["dispatchScheduler"] as SynchronousDispatchScheduler; }
            set
            { Fixture.Variables["dispatchScheduler"] = value; }
        }

        public ICommit Commit
        {
            get
            { return Fixture.Variables["commit"] as ICommit; }
            set
            { Fixture.Variables["commit"] = value; }
        }

        public when_synchronously_scheduling_a_commit_for_dispatch(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Dispatcher = A.Fake<IDispatchCommits>();
            Persistence = A.Fake<IPersistStreams>();
            Commit = CommitHelper.Create();

            DispatchScheduler = new SynchronousDispatchScheduler(Dispatcher, Persistence);
            DispatchScheduler.Start();
        }

        protected override void Because()
        {
            DispatchScheduler.ScheduleDispatch(Commit);
        }

        [Fact]
        public void should_provide_the_commit_to_the_dispatcher()
        {
            A.CallTo(() => Dispatcher.Dispatch(Commit)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public void should_mark_the_commit_as_dispatched()
        {
            A.CallTo(() => Persistence.MarkCommitAsDispatched(Commit)).MustHaveHappenedOnceExactly();
        }
    }

    public class when_disposing_the_synchronous_dispatch_scheduler : SpecificationBase<TestFixture>
    {
        public IDispatchCommits Dispatcher
        {
            get
            { return Fixture.Variables["dispatcher"] as IDispatchCommits; }
            set
            { Fixture.Variables["dispatcher"] = value; }
        }

        public IPersistStreams Persistence
        {
            get
            { return Fixture.Variables["persistence"] as IPersistStreams; }
            set
            { Fixture.Variables["persistence"] = value; }
        }

        private SynchronousDispatchScheduler DispatchScheduler
        {
            get
            { return Fixture.Variables["dispatchScheduler"] as SynchronousDispatchScheduler; }
            set
            { Fixture.Variables["dispatchScheduler"] = value; }
        }

        public when_disposing_the_synchronous_dispatch_scheduler(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Dispatcher = A.Fake<IDispatchCommits>();
            Persistence = A.Fake<IPersistStreams>();
            DispatchScheduler = new SynchronousDispatchScheduler(Dispatcher, Persistence);
        }

        protected override void Because()
        {
            DispatchScheduler.Dispose();
            DispatchScheduler.Dispose();
        }

        [Fact]
        public void should_dispose_the_underlying_dispatcher_exactly_once()
        {
            A.CallTo(() => Dispatcher.Dispose()).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public void should_dispose_the_underlying_persistence_infrastructure_exactly_once()
        {
            A.CallTo(() => Persistence.Dispose()).MustHaveHappenedOnceExactly();
        }
    }
}