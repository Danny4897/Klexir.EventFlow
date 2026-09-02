# Klexir.EventFlow

Strongly typed event bus for the Klexir ecosystem.

Only `Klexir.EventFlow.Abstractions` is a public NuGet package. The in-memory bus supports sequential or parallel dispatch, opt-in resilience (per-handler timeout, retry with fixed delay, backpressure via a bounded `Channel<T>`-based concurrency gate, dead-lettering instead of a propagated exception), and opt-in idempotency: an `IIdempotencyStore` dedups by event id + handler instance, so republishing the same event id is a no-op for handlers that already saw it. Persistence and distributed adapters follow in separate milestones.
