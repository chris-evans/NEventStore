namespace NEventStore
{
    using FluentAssertions;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Xunit;

    public class DefaultSerializationWireupTests
    {
        public class when_building_an_event_store_without_an_explicit_serializer : SpecificationBase<TestFixture>
        {
            private Wireup _wireup;
            private Exception _exception;
            private IStoreEvents _eventStore;

            public when_building_an_event_store_without_an_explicit_serializer(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _wireup = Wireup.Init()
                    .UsingSqlPersistence("fakeConnectionString")
                        .WithDialect(new Persistence.Sql.SqlDialects.MsSqlDialect());
            }

            protected override void Because()
            {
                _exception = Catch.Exception(() => { _eventStore = _wireup.Build(); });
            }

            protected override void Cleanup()
            {
                _eventStore.Dispose();
            }

            [Fact]
            public void should_not_throw_an_argument_null_exception()
            {
                _exception.Should().BeNull();
            }
        }
    }
}
