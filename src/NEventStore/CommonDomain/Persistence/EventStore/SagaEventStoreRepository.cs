namespace CommonDomain.Persistence.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using NEventStore;
    using NEventStore.Persistence;

    public class SagaEventStoreRepository : ISagaRepository, IDisposable
    {
        private const string SagaTypeHeader = "SagaType";

        private const string UndispatchedMessageHeader = "UndispatchedMessage.";

        private readonly IStoreEvents eventStore;

        private readonly IConstructSagas factory;

        private readonly IDictionary<string, IEventStream> streams = new Dictionary<string, IEventStream>();

        public SagaEventStoreRepository(IStoreEvents eventStore, IConstructSagas factory)
        {
            Guard.NotNull(() => eventStore, eventStore);
            Guard.NotNull(() => factory, factory);
            this.eventStore = eventStore;
            this.factory = factory;
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        public TSaga GetById<TSaga>(Guid sagaId) where TSaga : class, ISaga, new()
        {
            return GetById<TSaga>(Bucket.Default, sagaId);
        }

        public TSaga GetById<TSaga>(string bucketId, Guid sagaId) where TSaga : class, ISaga, new()
        {
            ISaga saga = this.GetSaga<TSaga>();
            ApplyEventsToSaga(this.OpenStream(bucketId, sagaId), saga);
            return saga as TSaga;
        }

        public void Save(ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders)
        {
            Save(Bucket.Default, saga, commitId, updateHeaders);
        }

        public void Save(string bucketId, ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders)
        {
            if (saga == null)
            {
                throw new ArgumentNullException("saga", ExceptionMessages.NullArgument);
            }

            Dictionary<string, object> headers = PrepareHeaders(saga, updateHeaders);
            IEventStream stream = this.PrepareStream(bucketId, saga, headers);

            Persist(stream, commitId);

            saga.ClearUncommittedEvents();
            saga.ClearUndispatchedMessages();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            lock (this.streams)
            {
                foreach (var stream in this.streams)
                {
                    stream.Value.Dispose();
                }

                this.streams.Clear();
            }
        }

        private IEventStream OpenStream(string bucketId, Guid sagaId)
        {
            IEventStream stream;
            string streamsId = bucketId + sagaId;

            if (this.streams.TryGetValue(streamsId, out stream))
            { return stream; }

            try
            {
                stream = this.eventStore.OpenStream(bucketId, sagaId, 0, int.MaxValue);
            }
            catch (StreamNotFoundException)
            {
                stream = this.eventStore.CreateStream(sagaId);
            }

            return this.streams[streamsId] = stream;
        }

        private ISaga GetSaga<TSaga>() where TSaga : class, ISaga, new()
        {
            return this.factory.Build(typeof(TSaga));
        }

        private static void ApplyEventsToSaga(IEventStream stream, ISaga saga)
        {
            foreach (var @event in stream.CommittedEvents.Select(x => x.Body))
            {
                saga.Transition(@event);
            }

            saga.ClearUncommittedEvents();
            saga.ClearUndispatchedMessages();
        }

        private static Dictionary<string, object> PrepareHeaders(
            ISaga saga, Action<IDictionary<string, object>> updateHeaders)
        {
            var headers = new Dictionary<string, object>();

            headers[SagaTypeHeader] = saga.GetType().FullName;
            if (updateHeaders != null)
            {
                updateHeaders(headers);
            }

            int i = 0;
            foreach (var command in saga.GetUndispatchedMessages())
            {
                headers[UndispatchedMessageHeader + i++] = command;
            }

            return headers;
        }

        private IEventStream PrepareStream(string bucketId, ISaga saga, Dictionary<string, object> headers)
        {
            IEventStream stream;
            string streamsId = bucketId + saga.Id;

            if (!this.streams.TryGetValue(streamsId, out stream))
            {
                this.streams[streamsId] = stream = this.eventStore.CreateStream(saga.Id);
            }

            foreach (var item in headers)
            {
                stream.UncommittedHeaders[item.Key] = item.Value;
            }

            saga.GetUncommittedEvents().Cast<object>().Select(x => new EventMessage { Body = x }).ToList().ForEach(stream.Add);

            return stream;
        }

        private static void Persist(IEventStream stream, Guid commitId)
        {
            try
            {
                stream.CommitChanges(commitId);
            }
            catch (DuplicateCommitException)
            {
                stream.ClearChanges();
            }
            catch (StorageException e)
            {
                throw new PersistenceException(e.Message, e);
            }
        }
    }
}