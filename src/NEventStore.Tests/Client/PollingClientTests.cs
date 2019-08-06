namespace NEventStore.Client
{
    using System;
    using System.Reactive.Linq;
    using System.Reactive.Threading.Tasks;
    using System.Threading.Tasks;

    using FakeItEasy;
    using FluentAssertions;
    using NEventStore.Persistence;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using Xunit;

    public class CreatingPollingClientTests
    {
        [Fact]
        public void When_persist_streams_is_null_then_should_throw()
        {
            Catch.Exception(() => new PollingClient(null)).Should().BeOfType<ArgumentNullException>();
        }

        [Fact]
        public void When_interval_less_than_zero_then_should_throw()
        {
            Catch.Exception(() => new PollingClient(A.Fake<IPersistStreams>(), -1)).Should().BeOfType<ArgumentException>();
        }

        [Fact]
        public void When_interval_is_zero_then_should_throw()
        {
            Catch.Exception(() => new PollingClient(A.Fake<IPersistStreams>(), 0)).Should().BeOfType<ArgumentException>();
        }
    }

    public abstract class using_polling_client : SpecificationBase<TestFixture>
    {
        protected const int PollingInterval = 100;

        public using_polling_client(TestFixture fixture)
            : base(fixture)
        { }

        protected PollingClient PollingClient
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(PollingClient), out var pollingClient))
                {
                    pollingClient = new PollingClient(StoreEvents.Advanced, PollingInterval);
                    PollingClient = pollingClient as PollingClient;
                }

                return pollingClient as PollingClient;
            }
            set
            { Fixture.Variables[nameof(PollingClient)] = value; }
        }

        protected IStoreEvents StoreEvents
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(StoreEvents), out var storeEvents))
                {
                    storeEvents = Wireup.Init().UsingInMemoryPersistence().Build();
                    StoreEvents = storeEvents as IStoreEvents;
                }

                return storeEvents as IStoreEvents;
            }
            set
            { Fixture.Variables[nameof(StoreEvents)] = value; }
        }

        protected override void Context()
        { }

        protected override void Cleanup()
        {
            StoreEvents.Dispose();
        }
    }

    public class when_commit_is_comitted_before_subscribing : using_polling_client
    {
        private IObserveCommits ObserveCommits
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(ObserveCommits), out var storeEvents))
                {
                    storeEvents = PollingClient.ObserveFrom();
                    ObserveCommits = storeEvents as IObserveCommits;
                }

                return storeEvents as IObserveCommits;
            }
            set
            { Fixture.Variables[nameof(ObserveCommits)] = value; }
        }

        private Task<ICommit> CommitObserved
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(CommitObserved), out var storeEvents))
                {
                    storeEvents = ObserveCommits.FirstAsync().ToTask();
                    CommitObserved = storeEvents as Task<ICommit>;
                }

                return storeEvents as Task<ICommit>;
            }
            set
            { Fixture.Variables[nameof(CommitObserved)] = value; }
        }

        public when_commit_is_comitted_before_subscribing(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            StoreEvents.Advanced.CommitSingle();
        }

        protected override void Because()
        {
            ObserveCommits.Start();
        }

        protected override void Cleanup()
        {
            ObserveCommits.Dispose();
        }

        [Fact]
        public void should_observe_commit()
        {
            CommitObserved.Wait(PollingInterval * 2).Should().Be(true);
        }
    }

    public class when_commit_is_comitted_before_and_after_subscribing : using_polling_client
    {
        private IObserveCommits _observeCommits;
        private Task<ICommit> _twoCommitsObserved;

        public when_commit_is_comitted_before_and_after_subscribing(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            StoreEvents.Advanced.CommitSingle();
            _observeCommits = PollingClient.ObserveFrom();
            _twoCommitsObserved = _observeCommits.Take(2).ToTask();
        }

        protected override void Because()
        {
            _observeCommits.Start();
            StoreEvents.Advanced.CommitSingle();
        }

        protected override void Cleanup()
        {
            _observeCommits.Dispose();
        }

        [Fact]
        public void should_observe_two_commits()
        {
            _twoCommitsObserved.Wait(PollingInterval * 2).Should().Be(true);
        }
    }

    public class with_two_observers_and_multiple_commits : using_polling_client
    {
        private IObserveCommits ObserveCommits1
        {
            get
            { return Fixture.Variables["observeCommits1"] as IObserveCommits; }
            set
            { Fixture.Variables["observeCommits1"] = value; }
        }

        private IObserveCommits ObserveCommits2
        {
            get
            { return Fixture.Variables["observeCommits2"] as IObserveCommits; }
            set
            { Fixture.Variables["observeCommits2"] = value; }
        }

        private Task<ICommit> ObserveCommits1Complete
        {
            get
            { return Fixture.Variables["observeCommits1Complete"] as Task<ICommit>; }
            set
            { Fixture.Variables["observeCommits1Complete"] = value; }
        }

        private Task<ICommit> ObserveCommits2Complete
        {
            get
            { return Fixture.Variables["observeCommits2Complete"] as Task<ICommit>; }
            set
            { Fixture.Variables["observeCommits2Complete"] = value; }
        }

        public with_two_observers_and_multiple_commits(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            StoreEvents.Advanced.CommitSingle();
            ObserveCommits1 = PollingClient.ObserveFrom();
            ObserveCommits1Complete = ObserveCommits1.Take(5).ToTask();

            ObserveCommits2 = PollingClient.ObserveFrom();
            ObserveCommits2Complete = ObserveCommits1.Take(10).ToTask();
        }

        protected override void Because()
        {
            ObserveCommits1.Start();
            ObserveCommits2.Start();
            Task.Factory.StartNew(() =>
            {
                for (int i = 0; i < 15; i++)
                {
                    StoreEvents.Advanced.CommitSingle();
                }
            });
        }

        protected override void Cleanup()
        {
            ObserveCommits1.Dispose();
            ObserveCommits2.Dispose();
        }

        [Fact]
        public void should_observe_commits_on_first_observer()
        {
            ObserveCommits1Complete.Wait(PollingInterval * 10).Should().Be(true);
        }

        [Fact]
        public void should_observe_commits_on_second_observer()
        {
            ObserveCommits2Complete.Wait(PollingInterval * 10).Should().Be(true);
        }
    }

    public class with_two_subscriptions_on_a_single_observer_and_multiple_commits : using_polling_client
    {
        private IObserveCommits ObserveCommits1
        {
            get
            { return Fixture.Variables["ObserveCommits1"] as IObserveCommits; }
            set
            { Fixture.Variables["ObserveCommits1"] = value; }
        }

        private Task<ICommit> ObserveCommits1Complete
        {
            get
            { return Fixture.Variables["ObserveCommits1Complete"] as Task<ICommit>; }
            set
            { Fixture.Variables["ObserveCommits1Complete"] = value; }
        }

        private Task<ICommit> ObserveCommits2Complete
        {
            get
            { return Fixture.Variables["ObserveCommits2Complete"] as Task<ICommit>; }
            set
            { Fixture.Variables["ObserveCommits2Complete"] = value; }
        }

        public with_two_subscriptions_on_a_single_observer_and_multiple_commits(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            StoreEvents.Advanced.CommitSingle();
            ObserveCommits1 = PollingClient.ObserveFrom();
            ObserveCommits1Complete = ObserveCommits1.Take(5).ToTask();
            ObserveCommits2Complete = ObserveCommits1.Take(10).ToTask();
        }

        protected override void Because()
        {
            ObserveCommits1.Start();
            Task.Factory.StartNew(() =>
            {
                for (int i = 0; i < 15; i++)
                {
                    StoreEvents.Advanced.CommitSingle();
                }
            });
        }

        protected override void Cleanup()
        {
            ObserveCommits1.Dispose();
        }

        [Fact]
        public void should_observe_commits_on_first_observer()
        {
            ObserveCommits1Complete.Wait(PollingInterval * 20).Should().Be(true);
        }

        [Fact]
        public void should_observe_commits_on_second_observer()
        {
            ObserveCommits2Complete.Wait(PollingInterval * 20).Should().Be(true);
        }
    }

    public class with_exception_when_handling_commit : using_polling_client
    {
        private Exception SubscriberException
        {
            get
            { return Fixture.Variables["SubscriberException"] as Exception; }
            set
            { Fixture.Variables["SubscriberException"] = value; }
        }

        private Exception Exception
        {
            get
            { return Fixture.Variables["Exception"] as Exception; }
            set
            { Fixture.Variables["Exception"] = value; }
        }

        private Exception OnErrorException
        {
            get
            { return Fixture.Variables["OnErrorException"] as Exception; }
            set
            { Fixture.Variables["OnErrorException"] = value; }
        }

        private IObserveCommits ObserveCommits
        {
            get
            { return Fixture.Variables["observeCommits"] as IObserveCommits; }
            set
            { Fixture.Variables["observeCommits"] = value; }
        }

        private Task ObservingCommits
        {
            get
            { return Fixture.Variables["ObservingCommits"] as Task; }
            set
            { Fixture.Variables["ObservingCommits"] = value; }
        }

        private IDisposable Subscription
        {
            get
            { return Fixture.Variables["Subscription"] as IDisposable; }
            set
            { Fixture.Variables["Subscription"] = value; }
        }

        public with_exception_when_handling_commit(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            StoreEvents.Advanced.CommitSingle();
            ObserveCommits = PollingClient.ObserveFrom();
            SubscriberException = new Exception();
            Subscription = ObserveCommits.Subscribe(c => { throw SubscriberException; }, ex => OnErrorException = ex);
        }

        protected override void Because()
        {
            ObservingCommits = ObserveCommits.Start();
            StoreEvents.Advanced.CommitSingle();
            Exception = Catch.Exception(() => ObservingCommits.Wait(1000));
        }

        protected override void Cleanup()
        {
            Subscription.Dispose();
            ObserveCommits.Dispose();
        }

        [Fact]
        public void should_observe_exception_from_start_task()
        {
            Exception.InnerException.Should().Be(SubscriberException);
        }

        [Fact]
        public void should_observe_exception_on_subscription()
        {
            OnErrorException.Should().Be(SubscriberException);
        }
    }

    public class when_resuming : using_polling_client
    {
        private IObserveCommits _observeCommits;
        private Task<ICommit> _commitObserved;

        public when_resuming(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            base.Context();
            StoreEvents.Advanced.CommitSingle();
            _observeCommits = PollingClient.ObserveFrom();
            _commitObserved = _observeCommits.FirstAsync().ToTask();
            _observeCommits.Start();
            _commitObserved.Wait(PollingInterval * 2);
            _observeCommits.Dispose();

            StoreEvents.Advanced.CommitSingle();
            string checkpointToken = _commitObserved.Result.CheckpointToken;
            _observeCommits = PollingClient.ObserveFrom(checkpointToken);
        }

        protected override void Because()
        {
            _observeCommits.Start();
            _commitObserved = _observeCommits.FirstAsync().ToTask();
        }

        protected override void Cleanup()
        {
            _observeCommits.Dispose();
        }

        [Fact]
        public void should_observe_commit()
        {
            _commitObserved.Wait(PollingInterval * 2).Should().Be(true);
        }
    }
}