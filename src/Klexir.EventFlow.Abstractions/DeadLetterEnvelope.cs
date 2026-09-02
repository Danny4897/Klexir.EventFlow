namespace Klexir.EventFlow.Abstractions;

/// <summary>A handler invocation that exhausted its retry budget, captured instead of throwing.</summary>
public sealed record DeadLetterEnvelope(
    IEvent Event,
    string HandlerTypeName,
    string FailureReason,
    int AttemptCount,
    DateTimeOffset FailedAt);
