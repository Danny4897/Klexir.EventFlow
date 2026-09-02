namespace Klexir.EventFlow.Abstractions;

/// <summary>Sink for handler invocations that failed after exhausting their retry budget.</summary>
public interface IDeadLetterQueue
{
    ValueTask EnqueueAsync(DeadLetterEnvelope envelope, CancellationToken cancellationToken = default);
}
