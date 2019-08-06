namespace NEventStore.Persistence.AcceptanceTests.BDD
{
    using System;
    using System.Collections.Generic;
    using Xunit;

    public class TestFixture
    {
        public Dictionary<string, object> Variables
        { get; private set; }

        public TestFixture()
        {
            Variables = new Dictionary<string, object>();
            SetupComplete = false;
        }

        public bool SetupComplete
        { get; set; }
    }

    public abstract class SpecificationBase<TFixture> : IClassFixture<TFixture>, IDisposable
        where TFixture : TestFixture
    {
        public TFixture Fixture
        { get; private set; }

        public SpecificationBase(TFixture fixture)
        {
            Fixture = fixture;

            // we only do context and because once
            if (!Fixture.SetupComplete)
            {
                Context();
                Because();
                Fixture.SetupComplete = true;
            }
        }

        protected virtual void Because()
        { }

        protected virtual void Cleanup()
        { }

        protected virtual void Context()
        { }

        public void Dispose()
        { Cleanup(); }
    }
}