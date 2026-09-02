namespace Klexir.EventFlow.Abstractions;

/// <summary>Consumes one concrete event type.</summary>
/// <typeparam name="TEvent">The event accepted by the handler.</typeparam>
public interface IEventHandler<in TEvent> where TEvent : IEvent
{
    ValueTask HandleAsync(TEvent @event, CancellationToken cancellationToken);
}
