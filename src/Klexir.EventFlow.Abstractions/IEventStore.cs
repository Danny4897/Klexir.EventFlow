namespace Klexir.EventFlow.Abstractions;

/// <summary>Append-only log of events grouped by stream id, readable from an arbitrary version.</summary>
public interface IEventStore
{
    ValueTask AppendAsync<TEvent>(string streamId, TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : IEvent;

    /// <summary>Reads a stream's events starting at <paramref name="fromVersion"/> (0-based, inclusive). Unknown streams read as empty.</summary>
    ValueTask<IReadOnlyList<IEvent>> ReadStreamAsync(string streamId, long fromVersion = 0, CancellationToken cancellationToken = default);
}
