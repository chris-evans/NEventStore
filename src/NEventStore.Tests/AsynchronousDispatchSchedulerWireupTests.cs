namespace NEventStore
{
    using System;
    using System.Reactive;
    using System.Threading.Tasks;
    using FluentAssertions;
    using NEventStore.Dispatcher;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using Xunit;

    public class AsynchronousDispatcherSchedulerWireupTests
    {
        public class when_configured_to_auto_start_by_default : SpecificationBase<TestFixture>
        {
            public when_configured_to_auto_start_by_default(TestFixture fixture)
                : base(fixture)
            { }

            private IStoreEvents _eventStore;
            private DummyDispatchCommits _dummyDispatchCommits;

            protected override void Context()
            {
                _dummyDispatchCommits = new DummyDispatchCommits();
                _eventStore = Wireup
                    .Init()
                    .UsingInMemoryPersistence()
                    .UsingAsynchronousDispatchScheduler()
                        .DispatchTo(_dummyDispatchCommits)
                    .Build();
            }

            protected override void Because()
            {
                using (var stream = _eventStore.OpenStream(Guid.NewGuid()))
                {
                    stream.Add(new EventMessage { Body = "Body" });
                    stream.CommitChanges(Guid.NewGuid());
                }
            }

            protected override void Cleanup()
            {
                _eventStore.Dispose();
            }

            [Fact]
            public void should_dispatch_event()
            {
                _dummyDispatchCommits.Dispatched.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            }
        }

        public class when_configured_to_start_explicitly_and_not_started : SpecificationBase<TestFixture>
        {
            private IStoreEvents EventStore
            {
                get
                { return Fixture.Variables[nameof(EventStore)] as IStoreEvents; }
                set
                { Fixture.Variables[nameof(EventStore)] = value; }
            }

            private DummyDispatchCommits DummyDispatchCommits
            {
                get
                { return Fixture.Variables[nameof(DummyDispatchCommits)] as DummyDispatchCommits; }
                set
                { Fixture.Variables[nameof(DummyDispatchCommits)] = value; }
            }

            private Exception Exception
            {
                get
                { return Fixture.Variables[nameof(Exception)] as Exception; }
                set
                { Fixture.Variables[nameof(Exception)] = value; }
            }

            public when_configured_to_start_explicitly_and_not_started(TestFixture fixture)
                            : base(fixture)
            { }

            protected override void Context()
            {
                DummyDispatchCommits = new DummyDispatchCommits();
                EventStore = Wireup
                    .Init()
                    .UsingInMemoryPersistence()
                    .UsingAsynchronousDispatchScheduler()
                        .DispatchTo(DummyDispatchCommits)
                        .Startup(DispatcherSchedulerStartup.Explicit)
                    .Build();
            }

            protected override void Because()
            {
                Exception = Catch.Exception(() =>
                {
                    using (var stream = EventStore.OpenStream(Guid.NewGuid()))
                    {
                        stream.Add(new EventMessage { Body = "Body" });
                        stream.CommitChanges(Guid.NewGuid());
                    }
                });
            }

            protected override void Cleanup()
            {
                EventStore.Dispose();
            }

            [Fact]
            public void should_throw()
            {
                Exception.Should().NotBeNull();
            }

            [Fact]
            public void should_be_invalid_operation()
            {
                Exception.Should().BeOfType<InvalidOperationException>();
            }
        }

        public class when_configured_to_start_explicitly_and_started : SpecificationBase<TestFixture>
        {
            private IStoreEvents _eventStore;
            private DummyDispatchCommits _dummyDispatchCommits;

            public when_configured_to_start_explicitly_and_started(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _dummyDispatchCommits = new DummyDispatchCommits();
                _eventStore = Wireup
                    .Init()
                    .UsingInMemoryPersistence()
                    .UsingAsynchronousDispatchScheduler()
                        .DispatchTo(_dummyDispatchCommits)
                        .Startup(DispatcherSchedulerStartup.Explicit)
                    .Build();
            }

            protected override void Because()
            {
                _eventStore.StartDispatchScheduler();
                using (var stream = _eventStore.OpenStream(Guid.NewGuid()))
                {
                    stream.Add(new EventMessage { Body = "Body" });
                    stream.CommitChanges(Guid.NewGuid());
                }
            }

            protected override void Cleanup()
            {
                _eventStore.Dispose();
            }

            [Fact]
            public void should_dispatch_event()
            {
                _dummyDispatchCommits.Dispatched.Wait(TimeSpan.FromSeconds(1)).Should().BeTrue();
            }
        }

        private class DummyDispatchCommits : IDispatchCommits
        {
            private readonly TaskCompletionSource<Unit> _taskCompletionSource;

            public DummyDispatchCommits()
            {
                _taskCompletionSource = new TaskCompletionSource<Unit>();
            }

            public Task Dispatched
            {
                get { return _taskCompletionSource.Task; }
            }

            public void Dispose()
            { }

            public void Dispatch(ICommit commit)
            {
                _taskCompletionSource.SetResult(Unit.Default);
            }
        }
    }
}