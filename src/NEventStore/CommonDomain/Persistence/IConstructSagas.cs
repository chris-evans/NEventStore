using System;

namespace CommonDomain.Persistence
{
    /// <summary>
    /// Interface for factories that construct Sagas.
    /// This follows the same pattern as the Aggregate constructor.
    /// </summary>
    public interface IConstructSagas
    {
        /// <summary>
        /// Builds the saga.
        /// </summary>
        ISaga Build(Type type);
    }
}
