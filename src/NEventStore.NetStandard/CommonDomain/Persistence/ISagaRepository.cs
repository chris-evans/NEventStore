namespace CommonDomain.Persistence
{
	using System;
	using System.Collections.Generic;

	public interface ISagaRepository
	{
		TSaga GetById<TSaga>(Guid sagaId) where TSaga : class, ISaga, new();

		TSaga GetById<TSaga>(string bucketId, Guid sagaId) where TSaga : class, ISaga, new();

		void Save(ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders);

		void Save(string bucketId, ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders);
	}
}