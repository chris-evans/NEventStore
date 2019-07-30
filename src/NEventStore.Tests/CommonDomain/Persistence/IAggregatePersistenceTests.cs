namespace CommonDomain
{
    using FluentAssertions;
    using global::CommonDomain.Core;
    using global::CommonDomain.Persistence.EventStore;
    using NEventStore;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using System;
    using Xunit;

    public class when_an_aggregate_is_persisted : SpecificationBase<TestFixture>
    {
        public when_an_aggregate_is_persisted(TestFixture fixture)
            : base(fixture)
        { }

        private IStoreEvents _store;
        private EventStoreRepository _repository;

        private TestAggregate _testAggregate;
        private Guid _id;

        protected override void Context()
        {
            _store = Wireup.Init().UsingInMemoryPersistence().Build();
            _repository = new EventStoreRepository(_store, new AggregateFactory(), new ConflictDetector());

            _id = Guid.NewGuid();
            _testAggregate = new TestAggregate(_id, "Test");
        }

        protected override void Because()
        {
            _repository.Save(_testAggregate, Guid.NewGuid(), null);
        }

        [Fact]
        public void should_be_returned_when_loaded_by_id()
        {
            _repository.GetById<TestAggregate>(_id).Name.Should().Be(_testAggregate.Name);
        }
    }

    public class when_a_persisted_aggregate_is_updated : SpecificationBase<TestFixture>
    {
        public when_a_persisted_aggregate_is_updated(TestFixture fixture)
            : base(fixture)
        { }

        private IStoreEvents Store
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(Store), out var store))
                {
                    store = Wireup.Init().UsingInMemoryPersistence().Build();
                    Store = store as IStoreEvents;
                }

                return store as IStoreEvents;
            }
            set
            { Fixture.Variables[nameof(Store)] = value; }
        }

        private EventStoreRepository Repository
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(Repository), out var repository))
                {
                    repository = new EventStoreRepository(Store, new AggregateFactory(), new ConflictDetector());
                    Repository = repository as EventStoreRepository;
                }

                return repository as EventStoreRepository;
            }
            set
            { Fixture.Variables[nameof(Repository)] = value; }
        }

        private Guid Id
        {
            get
            {
                if (!Fixture.Variables.TryGetValue(nameof(Id), out var id))
                {
                    id = Guid.NewGuid();
                    Id = (Guid)id;
                }

                return (Guid)id;
            }
            set
            { Fixture.Variables[nameof(Id)] = value; }
        }

        private const string NewName = "UpdatedName";

        protected override void Context()
        { Repository.Save(new TestAggregate(Id, "Test"), Guid.NewGuid(), null); }

        protected override void Because()
        {
            var aggregate = Repository.GetById<TestAggregate>(Id);
            aggregate.ChangeName(NewName);
            Repository.Save(aggregate, Guid.NewGuid(), null);
        }

        [Fact]
        public void should_have_updated_name()
        {
            Repository.GetById<TestAggregate>(Id).Name.Should().Be(NewName);
        }

        [Fact]
        public void should_have_updated_version()
        {
            Repository.GetById<TestAggregate>(Id).Version.Should().Be(2);
        }
    }

    public class when_a_loading_a_specific_aggregate_version : SpecificationBase<TestFixture>
    {
        public when_a_loading_a_specific_aggregate_version(TestFixture fixture)
            : base(fixture)
        { }

        private IStoreEvents _store;
        private EventStoreRepository _repository;

        private Guid _id;

        private const string VersionOneName = "Test";
        private const string NewName = "UpdatedName";

        protected override void Context()
        {
            _store = Wireup.Init().UsingInMemoryPersistence().Build();
            _repository = new EventStoreRepository(_store, new AggregateFactory(), new ConflictDetector());

            _id = Guid.NewGuid();
            _repository.Save(new TestAggregate(_id, VersionOneName), Guid.NewGuid(), null);
        }

        protected override void Because()
        {
            var aggregate = _repository.GetById<TestAggregate>(_id);
            aggregate.ChangeName(NewName);
            _repository.Save(aggregate, Guid.NewGuid(), null);
            _repository.Dispose();
        }

        [Fact]
        public void should_be_able_to_load_initial_version()
        {
            _repository.GetById<TestAggregate>(_id, 1).Name.Should().Be(VersionOneName);
        }
    }

    public class when_an_aggregate_is_persisted_to_specific_bucket : SpecificationBase<TestFixture>
    {
        public when_an_aggregate_is_persisted_to_specific_bucket(TestFixture fixture)
            : base(fixture)
        { }

        private IStoreEvents _store;
        private EventStoreRepository _repository;

        private TestAggregate _testAggregate;

        private Guid _id;

        private string _bucket;

        protected override void Context()
        {
            _store = Wireup.Init().UsingInMemoryPersistence().Build();
            _repository = new EventStoreRepository(_store, new AggregateFactory(), new ConflictDetector());

            _id = Guid.NewGuid();
            _bucket = "TenantB";
            _testAggregate = new TestAggregate(_id, "Test");
        }

        protected override void Because()
        {
            _repository.Save(_bucket, _testAggregate, Guid.NewGuid(), null);
        }

        [Fact]
        public void should_be_returned_when_loaded_by_id()
        {
            _repository.GetById<TestAggregate>(_bucket, _id).Name.Should().Be(_testAggregate.Name);
        }
    }
}