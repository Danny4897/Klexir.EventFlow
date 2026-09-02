using System.Collections.Concurrent;
using Klexir.EventFlow.Abstractions;

namespace Klexir.EventFlow;

/// <summary>Process-local append-only event store. Each stream is serialized by its own lock; streams never block each other.</summary>
public sealed class InMemoryEventStore : IEventStore
{
    private readonly ConcurrentDictionary<string, StreamLog> _streams = new();

    public ValueTask AppendAsync<TEvent>(string streamId, TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : IEvent
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);
        ArgumentNullException.ThrowIfNull(@event);

        var log = _streams.GetOrAdd(streamId, static _ => new StreamLog());
        lock (log.Gate)
        {
            log.Events.Add(@event);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<IEvent>> ReadStreamAsync(string streamId, long fromVersion = 0, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);
        ArgumentOutOfRangeException.ThrowIfNegative(fromVersion);

        if (!_streams.TryGetValue(streamId, out var log))
        {
            return ValueTask.FromResult<IReadOnlyList<IEvent>>(Array.Empty<IEvent>());
        }

        lock (log.Gate)
        {
            IReadOnlyList<IEvent> slice = fromVersion >= log.Events.Count
                ? Array.Empty<IEvent>()
                : log.Events.Skip((int)fromVersion).ToArray();

            return ValueTask.FromResult(slice);
        }
    }

    private sealed class StreamLog
    {
        public object Gate { get; } = new();

        public List<IEvent> Events { get; } = [];
    }
}
