namespace Klexir.EventFlow.Abstractions;

/// <summary>Deduplication guard keyed by an idempotency key (typically event id + handler).</summary>
public interface IIdempotencyStore
{
    /// <summary>
    /// Atomically claims <paramref name="idempotencyKey"/>. Returns <see langword="true"/> the first time a key is
    /// claimed, <see langword="false"/> on every subsequent call for the same key.
    /// </summary>
    ValueTask<bool> TryMarkProcessedAsync(string idempotencyKey, CancellationToken cancellationToken = default);
}
