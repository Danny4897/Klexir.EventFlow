# Klexir.EventFlow

Strongly typed event bus for the Klexir ecosystem.

Only `Klexir.EventFlow.Abstractions` is a public NuGet package. The in-memory bus supports sequential or parallel dispatch, and opt-in resilience: per-handler timeout, retry with fixed delay, backpressure (bounded `Channel<T>`-based concurrency gate), and dead-lettering instead of a propagated exception. Persistence, idempotency and distributed adapters follow in separate milestones.
