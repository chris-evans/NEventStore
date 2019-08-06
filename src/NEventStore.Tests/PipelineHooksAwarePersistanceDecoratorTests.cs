
#pragma warning disable 169
// ReSharper disable InconsistentNaming

namespace NEventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FakeItEasy;
    using NEventStore.Persistence;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using Xunit;

    public class PipelineHooksAwarePersistenceDecoratorTests
    {
        public class when_disposing_the_decorator : using_underlying_persistence
        {
            public when_disposing_the_decorator(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Because()
            {
                Decorator.Dispose();
            }

            [Fact]
            public void should_dispose_the_underlying_persistence()
            {
                A.CallTo(() => Persistence.Dispose()).MustHaveHappenedOnceExactly();
            }
        }

        public class when_reading_the_all_events_from_date : using_underlying_persistence
        {
            private ICommit Commit
            {
                get
                { return Fixture.Variables[nameof(Commit)] as ICommit; }
                set
                { Fixture.Variables[nameof(Commit)] = value; }
            }

            private DateTime Date
            {
                get
                { return (DateTime)Fixture.Variables[nameof(Date)]; }
                set
                { Fixture.Variables[nameof(Date)] = value; }
            }

            private IPipelineHook Hook1
            {
                get
                { return Fixture.Variables[nameof(Hook1)] as IPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook1)] = value; }
            }

            private IPipelineHook Hook2
            {
                get
                { return Fixture.Variables[nameof(Hook2)] as IPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook2)] = value; }
            }

            public when_reading_the_all_events_from_date(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                base.Context();

                Date = DateTime.Now;
                Commit = new Commit(Bucket.Default, streamId, 1, Guid.NewGuid(), 1, DateTime.Now, new LongCheckpoint(0).Value, null, null);

                Hook1 = A.Fake<IPipelineHook>();
                A.CallTo(() => Hook1.Select(Commit)).Returns(Commit);
                PipelineHooks.Add(Hook1);

                Hook2 = A.Fake<IPipelineHook>();
                A.CallTo(() => Hook2.Select(Commit)).Returns(Commit);
                PipelineHooks.Add(Hook2);

                A.CallTo(() => Persistence.GetFrom(Bucket.Default, Date)).Returns(new List<ICommit> { Commit });
            }

            protected override void Because()
            {
                // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                // Forces enumeration of commits.
                Decorator.GetFrom(Date).ToList();
            }

            [Fact]
            public void should_call_the_underlying_persistence_to_get_events()
            {
                A.CallTo(() => Persistence.GetFrom(Bucket.Default, Date)).MustHaveHappenedOnceExactly();
            }

            [Fact]
            public void should_pass_all_events_through_the_pipeline_hooks()
            {
                A.CallTo(() => Hook1.Select(Commit)).MustHaveHappenedOnceExactly();
                A.CallTo(() => Hook2.Select(Commit)).MustHaveHappenedOnceExactly();
            }
        }

        public class when_reading_the_all_events_to_date : using_underlying_persistence
        {
            private ICommit Commit
            {
                get
                { return Fixture.Variables[nameof(Commit)] as ICommit; }
                set
                { Fixture.Variables[nameof(Commit)] = value; }
            }

            private DateTime End
            {
                get
                { return (DateTime)Fixture.Variables[nameof(End)]; }
                set
                { Fixture.Variables[nameof(End)] = value; }
            }

            private IPipelineHook Hook1
            {
                get
                { return Fixture.Variables[nameof(Hook1)] as IPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook1)] = value; }
            }

            private IPipelineHook Hook2
            {
                get
                { return Fixture.Variables[nameof(Hook2)] as IPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook2)] = value; }
            }

            private DateTime Start
            {
                get
                { return (DateTime)Fixture.Variables[nameof(Start)]; }
                set
                { Fixture.Variables[nameof(Start)] = value; }
            }

            public when_reading_the_all_events_to_date(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                base.Context();

                Start = DateTime.Now;
                End = DateTime.Now;
                Commit = new Commit(Bucket.Default, streamId, 1, Guid.NewGuid(), 1, DateTime.Now, new LongCheckpoint(0).Value, null, null);

                Hook1 = A.Fake<IPipelineHook>();
                A.CallTo(() => Hook1.Select(Commit)).Returns(Commit);
                PipelineHooks.Add(Hook1);

                Hook2 = A.Fake<IPipelineHook>();
                A.CallTo(() => Hook2.Select(Commit)).Returns(Commit);
                PipelineHooks.Add(Hook2);

                A.CallTo(() => Persistence.GetFromTo(Bucket.Default, Start, End)).Returns(new List<ICommit> { Commit });
            }

            protected override void Because()
            {
                // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                // Forces enumeration of commits
                Decorator.GetFromTo(Start, End).ToList();
            }

            [Fact]
            public void should_call_the_underlying_persistence_to_get_events()
            {
                A.CallTo(() => Persistence.GetFromTo(Bucket.Default, Start, End)).MustHaveHappenedOnceExactly();
            }

            [Fact]
            public void should_pass_all_events_through_the_pipeline_hooks()
            {
                A.CallTo(() => Hook1.Select(Commit)).MustHaveHappenedOnceExactly();
                A.CallTo(() => Hook2.Select(Commit)).MustHaveHappenedOnceExactly();
            }
        }

        public class when_committing : using_underlying_persistence
        {
            private CommitAttempt _attempt;

            public when_committing(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _attempt = new CommitAttempt(streamId, 1, Guid.NewGuid(), 1, DateTime.Now, null, new List<EventMessage> { new EventMessage() });
            }

            protected override void Because()
            {
                Decorator.Commit(_attempt);
            }

            [Fact]
            public void should_dispose_the_underlying_persistence()
            {
                A.CallTo(() => Persistence.Commit(_attempt)).MustHaveHappenedOnceExactly();
            }
        }

        public class when_reading_the_all_events_from_checkpoint : using_underlying_persistence
        {
            private ICommit Commit
            {
                get
                { return Fixture.Variables[nameof(Commit)] as ICommit; }
                set
                { Fixture.Variables[nameof(Commit)] = value; }
            }

            private IPipelineHook Hook1
            {
                get
                { return Fixture.Variables[nameof(Hook1)] as IPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook1)] = value; }
            }

            private IPipelineHook Hook2
            {
                get
                { return Fixture.Variables[nameof(Hook2)] as IPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook2)] = value; }
            }

            public when_reading_the_all_events_from_checkpoint(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                base.Context();
                Commit = new Commit(Bucket.Default, streamId, 1, Guid.NewGuid(), 1, DateTime.Now, new LongCheckpoint(0).Value, null, null);

                Hook1 = A.Fake<IPipelineHook>();
                A.CallTo(() => Hook1.Select(Commit)).Returns(Commit);
                PipelineHooks.Add(Hook1);

                Hook2 = A.Fake<IPipelineHook>();
                A.CallTo(() => Hook2.Select(Commit)).Returns(Commit);
                PipelineHooks.Add(Hook2);

                A.CallTo(() => Persistence.GetFrom(null)).Returns(new List<ICommit> { Commit });
            }

            protected override void Because()
            {
                Decorator.GetFrom(null).ToList();
            }

            [Fact]
            public void should_call_the_underlying_persistence_to_get_events()
            {
                A.CallTo(() => Persistence.GetFrom(null)).MustHaveHappenedOnceExactly();
            }

            [Fact]
            public void should_pass_all_events_through_the_pipeline_hooks()
            {
                A.CallTo(() => Hook1.Select(Commit)).MustHaveHappenedOnceExactly();
                A.CallTo(() => Hook2.Select(Commit)).MustHaveHappenedOnceExactly();
            }
        }

        public class when_reading_the_all_events_get_undispatched : using_underlying_persistence
        {
            private ICommit Commit
            {
                get
                { return Fixture.Variables[nameof(Commit)] as ICommit; }
                set
                { Fixture.Variables[nameof(Commit)] = value; }
            }

            private IPipelineHook Hook1
            {
                get
                { return Fixture.Variables[nameof(Hook1)] as IPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook1)] = value; }
            }

            private IPipelineHook Hook2
            {
                get
                { return Fixture.Variables[nameof(Hook2)] as IPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook2)] = value; }
            }

            public when_reading_the_all_events_get_undispatched(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                base.Context();

                Commit = new Commit(Bucket.Default, streamId, 1, Guid.NewGuid(), 1, DateTime.Now, new LongCheckpoint(0).Value, null, null);

                Hook1 = A.Fake<IPipelineHook>();
                A.CallTo(() => Hook1.Select(Commit)).Returns(Commit);
                PipelineHooks.Add(Hook1);

                Hook2 = A.Fake<IPipelineHook>();
                A.CallTo(() => Hook2.Select(Commit)).Returns(Commit);
                PipelineHooks.Add(Hook2);

                A.CallTo(() => Persistence.GetUndispatchedCommits()).Returns(new List<ICommit> { Commit });
            }

            protected override void Because()
            {
                Decorator.GetUndispatchedCommits().ToList();
            }

            [Fact]
            public void should_call_the_underlying_persistence_to_get_events()
            {
                A.CallTo(() => Persistence.GetUndispatchedCommits()).MustHaveHappenedOnceExactly();
            }

            [Fact]
            public void should_pass_all_events_through_the_pipeline_hooks()
            {
                A.CallTo(() => Hook1.Select(Commit)).MustHaveHappenedOnceExactly();
                A.CallTo(() => Hook2.Select(Commit)).MustHaveHappenedOnceExactly();
            }
        }

        public class when_purging : using_underlying_persistence
        {
            private IPipelineHook _hook;

            public when_purging(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _hook = A.Fake<IPipelineHook>();
                PipelineHooks.Add(_hook);
            }

            protected override void Because()
            {
                Decorator.Purge();
            }

            [Fact]
            public void should_call_the_pipeline_hook_purge()
            {
                A.CallTo(() => _hook.OnPurge(null)).MustHaveHappenedOnceExactly();
            }
        }

        public class when_purging_a_bucket : using_underlying_persistence
        {
            private IPipelineHook _hook;
            private const string _bucketId = "Bucket";

            public when_purging_a_bucket(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                _hook = A.Fake<IPipelineHook>();
                PipelineHooks.Add(_hook);
            }

            protected override void Because()
            {
                Decorator.Purge(_bucketId);
            }

            [Fact]
            public void should_call_the_pipeline_hook_purge()
            {
                A.CallTo(() => _hook.OnPurge(_bucketId)).MustHaveHappenedOnceExactly();
            }
        }

        public class when_deleting_a_stream : using_underlying_persistence
        {
            private IPipelineHook Hook
            {
                get
                { return Fixture.Variables[nameof(Hook)] as IPipelineHook; }
                set
                { Fixture.Variables[nameof(Hook)] = value; }
            }

            private const string _bucketId = "Bucket";
            private const string _streamId = "Stream";

            public when_deleting_a_stream(TestFixture fixture)
                : base(fixture)
            { }

            protected override void Context()
            {
                base.Context();
                Hook = A.Fake<IPipelineHook>();
                PipelineHooks.Add(Hook);
            }

            protected override void Because()
            {
                Decorator.DeleteStream(_bucketId, _streamId);
            }

            [Fact]
            public void should_call_the_pipeline_hook_purge()
            {
                A.CallTo(() => Hook.OnDeleteStream(_bucketId, _streamId)).MustHaveHappenedOnceExactly();
            }
        }

        public abstract class using_underlying_persistence : SpecificationBase<TestFixture>
        {
            protected readonly string streamId = Guid.NewGuid().ToString();

            public using_underlying_persistence(TestFixture fixture)
                : base(fixture)
            { }

            protected List<IPipelineHook> PipelineHooks
            {
                get
                {
                    if (!Fixture.Variables.TryGetValue(nameof(PipelineHooks), out var pipelineHooks))
                    {
                        pipelineHooks = new List<IPipelineHook>();
                        PipelineHooks = pipelineHooks as List<IPipelineHook>;
                    }

                    return pipelineHooks as List<IPipelineHook>;
                }
                set
                { Fixture.Variables[nameof(PipelineHooks)] = value; }
            }

            protected IPersistStreams Persistence
            {
                get
                {
                    if (!Fixture.Variables.TryGetValue(nameof(Persistence), out var persistence))
                    {
                        persistence = A.Fake<IPersistStreams>();
                        Persistence = persistence as IPersistStreams;
                    }

                    return persistence as IPersistStreams;
                }
                set
                { Fixture.Variables[nameof(Persistence)] = value; }
            }

            protected PipelineHooksAwarePersistanceDecorator Decorator
            {
                get
                {
                    if (!Fixture.Variables.TryGetValue(nameof(Decorator), out var decorator))
                    {
                        decorator = new PipelineHooksAwarePersistanceDecorator(Persistence, PipelineHooks.Select(x => x));
                        Decorator = decorator as PipelineHooksAwarePersistanceDecorator;
                    }

                    return decorator as PipelineHooksAwarePersistanceDecorator;
                }
                set
                { Fixture.Variables[nameof(Decorator)] = value; }
            }
        }
    }
}

// ReSharper enable InconsistentNaming
#pragma warning restore 169