namespace NEventStore.ConversionTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using FluentAssertions;
    using NEventStore.Conversion;
    using NEventStore.Persistence;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using Xunit;

    public class when_opening_a_commit_that_does_not_have_convertible_events : using_event_converter
    {
        private ICommit Commit
        {
            get
            { return Fixture.Variables["commit"] as ICommit; }
            set
            { Fixture.Variables["commit"] = value; }
        }

        private ICommit Converted
        {
            get
            { return Fixture.Variables["converted"] as ICommit; }
            set
            { Fixture.Variables["converted"] = value; }
        }

        public when_opening_a_commit_that_does_not_have_convertible_events(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Commit =  CreateCommit(new EventMessage {Body = new NonConvertingEvent()});
        }

        protected override void Because()
        {
            Converted = EventUpconverter.Select(Commit);
        }

        [Fact]
        public void should_not_be_converted()
        {
            Converted.Should().BeSameAs(Commit);
        }

        [Fact]
        public void should_have_the_same_instance_of_the_event()
        {
            Converted.Events.Single().Should().Be(Commit.Events.Single());
        }
    }

    public class when_opening_a_commit_that_has_convertible_events : using_event_converter
    {
        private Guid Id
        {
            get
            { return (Guid)Fixture.Variables["id"]; }
            set
            { Fixture.Variables["id"] = value; }
        }

        private ICommit Commit
        {
            get
            { return Fixture.Variables["commit"] as ICommit; }
            set
            { Fixture.Variables["commit"] = value; }
        }

        private ICommit Converted
        {
            get
            { return Fixture.Variables["converted"] as ICommit; }
            set
            { Fixture.Variables["converted"] = value; }
        }

        public when_opening_a_commit_that_has_convertible_events(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Id = Guid.NewGuid();
            Commit = CreateCommit(new EventMessage {Body = new ConvertingEvent(Id)});
        }

        protected override void Because()
        {
            Converted = EventUpconverter.Select(Commit);
        }

        [Fact]
        public void should_be_of_the_converted_type()
        {
            Converted.Events.Single().Body.GetType().Should().Be(typeof (ConvertingEvent3));
        }

        [Fact]
        public void should_have_the_same_id_of_the_commited_event()
        {
            ((ConvertingEvent3) Converted.Events.Single().Body).Id.Should().Be(Id);
        }
    }

    public class when_an_event_converter_implements_the_IConvertEvents_interface_explicitly : using_event_converter
    {
        private Guid Id
        {
            get
            { return (Guid) Fixture.Variables["id"]; }
            set
            { Fixture.Variables["id"] = value; }
        }

        private ICommit Commit
        {
            get
            { return Fixture.Variables["commit"] as ICommit; }
            set
            { Fixture.Variables["commit"] = value; }
        }

        private ICommit Converted
        {
            get
            { return Fixture.Variables["converted"] as ICommit; }
            set
            { Fixture.Variables["converted"] = value; }
        }

        private EventMessage EventMessage
        {
            get
            { return Fixture.Variables["eventMessage"] as EventMessage; }
            set
            { Fixture.Variables["eventMessage"] = value; }
        }

        public when_an_event_converter_implements_the_IConvertEvents_interface_explicitly(TestFixture fixture)
            : base(fixture)
        { }

        protected override void Context()
        {
            Id = Guid.NewGuid();
            EventMessage = new EventMessage {Body = new ConvertingEvent2(Id, "FooEvent")};

            Commit = CreateCommit(EventMessage);
        }

        protected override void Because()
        {
            Converted = EventUpconverter.Select(Commit);
        }

        [Fact]
        public void should_be_of_the_converted_type()
        {
            Converted.Events.Single().Body.GetType().Should().Be(typeof (ConvertingEvent3));
        }

        [Fact]
        public void should_have_the_same_id_of_the_commited_event()
        {
            ((ConvertingEvent3)Converted.Events.Single().Body).Id.Should().Be(Id);
        }
    }

    public class using_event_converter : SpecificationBase<TestFixture>
    {
        private IEnumerable<Assembly> _assemblies;
        private Dictionary<Type, Func<object, object>> _converters;
        private EventUpconverterPipelineHook _eventUpconverter;

        public using_event_converter(TestFixture fixture)
            : base(fixture)
        { }

        protected EventUpconverterPipelineHook EventUpconverter
        {
            get { return _eventUpconverter ?? (_eventUpconverter = CreateUpConverterHook()); }
        }

        private EventUpconverterPipelineHook CreateUpConverterHook()
        {
            _assemblies = GetAllAssemblies();
            _converters = GetConverters(_assemblies);
            return new EventUpconverterPipelineHook(_converters);
        }

        private Dictionary<Type, Func<object, object>> GetConverters(IEnumerable<Assembly> toScan)
        {
            IEnumerable<KeyValuePair<Type, Func<object, object>>> c = from a in toScan
                from t in a.GetTypes()
                let i = t.GetInterface(typeof (IUpconvertEvents<,>).FullName)
                where i != null
                let sourceType = i.GetGenericArguments().First()
                let convertMethod = i.GetMethods(BindingFlags.Public | BindingFlags.Instance).First()
                let instance = Activator.CreateInstance(t)
                select new KeyValuePair<Type, Func<object, object>>(sourceType, e => convertMethod.Invoke(instance, new[] {e}));
            try
            {
                return c.ToDictionary(x => x.Key, x => x.Value);
            }
            catch (ArgumentException ex)
            {
                throw new MultipleConvertersFoundException(ex.Message, ex);
            }
        }

        private IEnumerable<Assembly> GetAllAssemblies()
        {
            return
                Assembly.GetCallingAssembly().GetReferencedAssemblies().Select(Assembly.Load).Concat(new[] {Assembly.GetCallingAssembly()});
        }

        protected static ICommit CreateCommit(EventMessage eventMessage)
        {
            return new Commit(Bucket.Default,
                Guid.NewGuid().ToString(),
                0,
                Guid.NewGuid(),
                0,
                DateTime.MinValue,
                new LongCheckpoint(0).Value,
                null,
                new[] { eventMessage });
        }
    }

    public class ConvertingEventConverter : IUpconvertEvents<ConvertingEvent, ConvertingEvent2>
    {
        public ConvertingEvent2 Convert(ConvertingEvent sourceEvent)
        {
            return new ConvertingEvent2(sourceEvent.Id, "Temp");
        }
    }

    public class ExplicitConvertingEventConverter : IUpconvertEvents<ConvertingEvent2, ConvertingEvent3>
    {
        ConvertingEvent3 IUpconvertEvents<ConvertingEvent2, ConvertingEvent3>.Convert(ConvertingEvent2 sourceEvent)
        {
            return new ConvertingEvent3(sourceEvent.Id, "Temp", true);
        }
    }

    public class NonConvertingEvent
    {}

    public class ConvertingEvent
    {
        public ConvertingEvent(Guid id)
        {
            Id = id;
        }

        public Guid Id { get; set; }
    }

    public class ConvertingEvent2
    {
        public ConvertingEvent2(Guid id, string name)
        {
            Id = id;
            Name = name;
        }

        public Guid Id { get; set; }
        public string Name { get; set; }
    }

    public class ConvertingEvent3
    {
        public ConvertingEvent3(Guid id, string name, bool imExplicit)
        {
            Id = id;
            Name = name;
            ImExplicit = imExplicit;
        }

        public Guid Id { get; set; }
        public string Name { get; set; }
        public bool ImExplicit { get; set; }
    }
}

// ReSharper enable InconsistentNaming
#pragma warning restore 169