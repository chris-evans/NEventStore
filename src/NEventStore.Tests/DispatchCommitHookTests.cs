
#pragma warning disable 169

namespace NEventStore
{
    using System;

    using FakeItEasy;
    using FluentAssertions;
    using NEventStore.Dispatcher;
    using NEventStore.Persistence;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using Xunit;

    public class when_a_commit_has_been_persisted : SpecificationBase<TestFixture>
    {
        private readonly ICommit _commit = new Commit(Bucket.Default, Guid.NewGuid().ToString(), 0, Guid.NewGuid(), 0, DateTime.MinValue, new LongCheckpoint(0).Value, null, null);
        
        private readonly IScheduleDispatches _dispatcher = A.Fake<IScheduleDispatches>();

        private DispatchSchedulerPipelineHook _dispatchSchedulerHook;

        public when_a_commit_has_been_persisted(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            _dispatchSchedulerHook = new DispatchSchedulerPipelineHook(_dispatcher);
        }

        protected override void Because()
        {
            _dispatchSchedulerHook.PostCommit(_commit);
        }

        [Fact]
        public void should_invoke_the_configured_dispatcher()
        {
            A.CallTo(() => _dispatcher.ScheduleDispatch(_commit)).MustHaveHappenedOnceExactly();
        }
    }

    public class when_the_hook_has_no_dispatcher_configured : SpecificationBase<TestFixture>
    {
        private readonly DispatchSchedulerPipelineHook _dispatchSchedulerHook = new DispatchSchedulerPipelineHook();

        private readonly ICommit _commit = new Commit(Bucket.Default, Guid.NewGuid().ToString(), 0, Guid.NewGuid(), 0, DateTime.MinValue, new LongCheckpoint(0).Value, null, null);

        private Exception _thrown;

        public when_the_hook_has_no_dispatcher_configured(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            _thrown = Catch.Exception(() => _dispatchSchedulerHook.PostCommit(_commit));
        }

        [Fact]
        public void should_not_throw_an_exception()
        {
            _thrown.Should().BeNull();
        }
    }

    public class when_a_commit_is_selected : SpecificationBase<TestFixture>
    {
        private readonly DispatchSchedulerPipelineHook _dispatchSchedulerHook = new DispatchSchedulerPipelineHook();

        private readonly ICommit _commit = new Commit(Bucket.Default, Guid.NewGuid().ToString(), 0, Guid.NewGuid(), 0, DateTime.MinValue, new LongCheckpoint(0).Value, null, null);

        private ICommit _selected;

        public when_a_commit_is_selected(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            _selected = _dispatchSchedulerHook.Select(_commit);
        }

        [Fact]
        public void should_always_return_the_exact_same_commit()
        {
            ReferenceEquals(_selected, _commit).Should().BeTrue();
        }
    }
}

#pragma warning restore 169