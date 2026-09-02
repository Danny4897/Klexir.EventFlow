# Klexir.EventFlow

Strongly typed event bus for the Klexir ecosystem.

Only `Klexir.EventFlow.Abstractions` is a public NuGet package. The in-memory bus supports sequential or parallel dispatch, opt-in resilience (per-handler timeout, retry with fixed delay, backpressure via a bounded `Channel<T>`-based concurrency gate, dead-lettering instead of a propagated exception), and opt-in idempotency: an `IIdempotencyStore` dedups by event id + handler instance, so republishing the same event id is a no-op for handlers that already saw it. `IEventStore`/`InMemoryEventStore` add an append-only per-stream log: `AppendAsync` writes, `ReadStreamAsync(streamId, fromVersion)` replays from a given version; streams are independent and each serializes its own writers.

Observability uses the BCL's own `System.Diagnostics.ActivitySource`/`Activity` (no OpenTelemetry SDK dependency — the instrumentation API is runtime-included; add the SDK only when you want to export) under the source name `InMemoryEventBus.ActivitySourceName` ("Klexir.EventFlow"): `PublishAsync` starts an `EventFlow.Publish` activity tagged with event type/id and handler count, and each handler dispatch starts a child `EventFlow.Handle` activity tagged with the handler type, marked `Error` (with the failure message) when a handler is dead-lettered or its retries are exhausted without a dead-letter queue to absorb it.

Distributed adapters follow in a separate milestone.
