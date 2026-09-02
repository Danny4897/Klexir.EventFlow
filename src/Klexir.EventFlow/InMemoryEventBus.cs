using System.Collections.Concurrent;
using Klexir.EventFlow.Abstractions;

namespace Klexir.EventFlow;

/// <summary>
/// Thread-safe registration with deterministic, sequential handler invocation per publication.
/// This is the Stage 1 baseline; asynchronous concurrency and resilience policies are separate increments.
/// </summary>
public sealed class InMemoryEventBus : IEventBus
{
    private readonly ConcurrentDictionary<Type, IHandlerInvoker[]> _handlers = new();

    public void Register<TEvent>(IEventHandler<TEvent> handler) where TEvent : IEvent
    {
        ArgumentNullException.ThrowIfNull(handler);

        _handlers.AddOrUpdate(
            typeof(TEvent),
            _ => [new HandlerInvoker<TEvent>(handler)],
            (_, handlers) => [.. handlers, new HandlerInvoker<TEvent>(handler)]);
    }

    public async ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : IEvent
    {
        ArgumentNullException.ThrowIfNull(@event);

        if (!_handlers.TryGetValue(typeof(TEvent), out var handlers))
        {
            return;
        }

        foreach (var handler in handlers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await handler.HandleAsync(@event, cancellationToken).ConfigureAwait(false);
        }
    }

    private interface IHandlerInvoker
    {
        ValueTask HandleAsync(IEvent @event, CancellationToken cancellationToken);
    }

    private sealed class HandlerInvoker<TEvent>(IEventHandler<TEvent> handler) : IHandlerInvoker where TEvent : IEvent
    {
        public ValueTask HandleAsync(IEvent @event, CancellationToken cancellationToken) =>
            handler.HandleAsync((TEvent)@event, cancellationToken);
    }
}
