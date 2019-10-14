namespace NEventStore.Persistence.Sql
{
    using System;
    using System.Runtime.Serialization.Formatters;
    using FluentAssertions;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using NEventStore.Persistence.Sql.SqlDialects;
    using NEventStore.Serialization;
    using Xunit;

    public class when_creating_sql_persistence_factory_with_oracle_native_dialect : SpecificationBase<TestFixture>
    {
        private Exception _exception;

        public when_creating_sql_persistence_factory_with_oracle_native_dialect(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Because()
        {
            _exception = Catch.Exception(() => new SqlPersistenceFactory("Connection",
                new BinarySerializer(FormatterAssemblyStyle.Full),
                new OracleNativeDialect()).Build());
        }

        [Fact]
        public void should_not_throw()
        {
           _exception.Should().BeNull();
        }
    }
}