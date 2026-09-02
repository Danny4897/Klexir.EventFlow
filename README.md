# Klexir.EventFlow

[![CI](https://github.com/Danny4897/Klexir.EventFlow/actions/workflows/ci.yml/badge.svg)](https://github.com/Danny4897/Klexir.EventFlow/actions/workflows/ci.yml)
[![.NET](https://img.shields.io/badge/.NET-8.0-purple.svg)](https://dotnet.microsoft.com/)
[![Docs](https://img.shields.io/badge/docs-vitepress-7c3aed.svg)](https://danny4897.github.io/Klexir.EventFlow/)

Strongly typed, in-process event bus for the Klexir ecosystem — resilience, idempotency, event sourcing and tracing built in, no message broker required.

> **Status: public research repo, not yet published to NuGet.** Reference the project directly (`ProjectReference` to `src/Klexir.EventFlow.Abstractions` and `src/Klexir.EventFlow`) until/unless it's published.

---

## Quick example

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

---

## What's in the box

| Capability | API | Notes |
|---|---|---|
| Typed pub/sub | `IEventBus`, `IEventHandler<T>`, `InMemoryEventBus` | Sequential or parallel dispatch, registration order preserved |
| Resilience | `EventBusResilienceOptions` | Per-handler timeout, retry with fixed delay, backpressure via a bounded `Channel<T>`-backed gate |
| Dead-lettering | `IDeadLetterQueue`, `InMemoryDeadLetterQueue` | A handler that exhausts retries is captured, not thrown |
| Idempotency | `IIdempotencyStore`, `InMemoryIdempotencyStore` | Dedup key = event id + per-registration handler id |
| Event sourcing | `IEventStore`, `InMemoryEventStore` | Append-only per-stream log, replay from any version |
| Tracing | `System.Diagnostics.ActivitySource` (`InMemoryEventBus.ActivitySourceName`) | No OpenTelemetry SDK needed to instrument — only to export |

Every option above defaults to **off**: a bare `new InMemoryEventBus()` behaves exactly like the original sequential, non-resilient bus.

## Replay a stream

```csharp
var store = new InMemoryEventStore();
await store.AppendAsync("order-123", new OrderPlaced(...));

IReadOnlyList<IEvent> history = (await store.ReadStreamAsync("order-123", fromVersion: 0)).Value;
```

## Not there yet

- Distributed adapters (RabbitMQ/Kafka) — needs a real broker to test against; no infrastructure decision made
- Metrics (only tracing spans exist today, no `Meter`/counters)

## Requirements

.NET 8 SDK. No external dependencies — this repo intentionally stays plain-BCL (it predates the ecosystem's later decision to adopt [MonadicSharp](https://www.nuget.org/packages/MonadicSharp/) `Result<T>` for new repos; see `Klexir.Engine`/`Klexir.Runtime`/`Klexir.Lang`/`Klexir.Workflow` for that style).
