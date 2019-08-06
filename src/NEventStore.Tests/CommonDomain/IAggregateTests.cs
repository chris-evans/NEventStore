namespace CommonDomain
{
	using System;
    using FluentAssertions;
    using NEventStore.Persistence.AcceptanceTests.BDD;

	using Xunit;

	public class when_an_aggregate_is_created : SpecificationBase<TestFixture>
	{
		private TestAggregate TestAggregate
        {
            get
            { return Fixture.Variables[nameof(TestAggregate)] as TestAggregate; }
            set
            { Fixture.Variables[nameof(TestAggregate)] = value; }
        }

        public when_an_aggregate_is_created(TestFixture fixture)
            : base(fixture)
        { }

		protected override void Because()
		{
			this.TestAggregate = new TestAggregate(Guid.NewGuid(), "Test");
		}

		[Fact]
		public void should_have_name()
		{
			this.TestAggregate.Name.Should().Be("Test");
		}

		[Fact]
		public void aggregate_version_should_be_one()
		{
			this.TestAggregate.Version.Should().Be(1);
		}
	}

	public class when_updating_an_aggregate : SpecificationBase<TestFixture>
	{
		private TestAggregate TestAggregate
        {
            get
            { return Fixture.Variables[nameof(TestAggregate)] as TestAggregate; }
            set
            { Fixture.Variables[nameof(TestAggregate)] = value; }
        }

        public when_updating_an_aggregate(TestFixture fixture)
            : base(fixture)
        { }

		protected override void Context()
		{
			this.TestAggregate = new TestAggregate(Guid.NewGuid(), "Test");
		}

		protected override void Because()
		{
			TestAggregate.ChangeName("UpdatedTest");
		}

		[Fact]
		public void name_change_should_be_applied()
		{
			this.TestAggregate.Name.Should().Be("UpdatedTest");
		}

		[Fact]
		public void applying_events_automatically_increments_version()
		{
			this.TestAggregate.Version.Should().Be(2);
		}
	}
}