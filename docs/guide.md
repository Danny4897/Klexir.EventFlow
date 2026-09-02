# Quick example

```csharp
public sealed record OrderPlaced(Guid EventId, DateTimeOffset OccurredAt, decimal Total) : IEvent;

public sealed class SendReceiptHandler : IEventHandler<OrderPlaced>
{
    public async ValueTask HandleAsync(OrderPlaced @event, CancellationToken ct) =>
        await _email.SendReceiptAsync(@event.Total, ct);
}

var bus = new InMemoryEventBus(
    dispatchMode: EventDispatchMode.Parallel,
    deadLetterQueue: new InMemoryDeadLetterQueue(),
    resilience: new EventBusResilienceOptions
    {
        MaxAttempts = 3,
        RetryDelay = TimeSpan.FromSeconds(1),
        HandlerTimeout = TimeSpan.FromSeconds(5),
        MaxConcurrentDispatches = 8,
    },
    idempotencyStore: new InMemoryIdempotencyStore());

bus.Register(new SendReceiptHandler());
await bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow, 42.50m));
```

A handler that keeps failing after 3 attempts lands in the dead-letter queue instead of crashing the publisher. Republishing the same `EventId` is a no-op for a handler that already saw it. Nothing here throws on a handler failure — check `deadLetterQueue` for what didn't make it.

See the [full README](https://github.com/Danny4897/Klexir.EventFlow#readme) on GitHub for the complete feature table and current gaps.
