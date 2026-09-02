using System.Collections.Concurrent;
using Klexir.EventFlow.Abstractions;

namespace Klexir.EventFlow;

/// <summary>Process-local idempotency guard backed by a concurrent set. Keys are never evicted.</summary>
public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    private readonly ConcurrentDictionary<string, byte> _seenKeys = new();

    public ValueTask<bool> TryMarkProcessedAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(idempotencyKey);
        return ValueTask.FromResult(_seenKeys.TryAdd(idempotencyKey, 0));
    }
}
