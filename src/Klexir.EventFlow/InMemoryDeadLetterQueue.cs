using System.Collections.Concurrent;
using Klexir.EventFlow.Abstractions;

namespace Klexir.EventFlow;

/// <summary>Process-local, unbounded dead-letter sink. Entries are kept until read via <see cref="Snapshot"/>.</summary>
public sealed class InMemoryDeadLetterQueue : IDeadLetterQueue
{
    private readonly ConcurrentQueue<DeadLetterEnvelope> _entries = new();

    public ValueTask EnqueueAsync(DeadLetterEnvelope envelope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        _entries.Enqueue(envelope);
        return ValueTask.CompletedTask;
    }

    public IReadOnlyList<DeadLetterEnvelope> Snapshot() => _entries.ToArray();
}
