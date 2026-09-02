namespace Klexir.EventFlow.Abstractions;

/// <summary>Publishes events to their registered handlers in registration order.</summary>
public interface IEventBus
{
    ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : IEvent;
}
