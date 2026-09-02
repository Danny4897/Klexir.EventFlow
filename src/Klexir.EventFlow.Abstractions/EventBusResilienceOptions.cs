namespace Klexir.EventFlow.Abstractions;

/// <summary>
/// Per-handler resilience policy applied by an <see cref="IEventBus"/> implementation.
/// Defaults preserve the non-resilient baseline: one attempt, no timeout, no concurrency cap.
/// </summary>
public sealed record EventBusResilienceOptions
{
    /// <summary>Maximum wall-clock time a single handler invocation may take before it is treated as failed.</summary>
    public TimeSpan? HandlerTimeout { get; init; }

    /// <summary>Total invocation attempts for a handler, including the first. Must be at least 1.</summary>
    public int MaxAttempts { get; init; } = 1;

    /// <summary>Delay awaited between retry attempts.</summary>
    public TimeSpan RetryDelay { get; init; } = TimeSpan.Zero;

    /// <summary>Upper bound on handler invocations running concurrently across the whole bus. Null means unbounded.</summary>
    public int? MaxConcurrentDispatches { get; init; }
}
