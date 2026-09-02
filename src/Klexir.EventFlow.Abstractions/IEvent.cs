namespace Klexir.EventFlow.Abstractions;

/// <summary>Immutable fact published by the event bus.</summary>
public interface IEvent
{
    Guid EventId { get; }

    DateTimeOffset OccurredAt { get; }
}
