---
layout: home

hero:
  name: "Klexir.EventFlow"
  text: "Strongly typed event bus"
  tagline: In-process events for the Klexir ecosystem — resilience, idempotency, dead-lettering and tracing built in, no message broker required.
  actions:
    - theme: brand
      text: Quick example
      link: /guide
    - theme: alt
      text: Full README on GitHub
      link: https://github.com/Danny4897/Klexir.EventFlow
    - theme: alt
      text: Klexir Ecosystem
      link: https://danny4897.github.io/MonadicSharp/ecosystem

features:
  - title: Resilient by default
    details: Retry with backoff, per-handler timeout, and a dead-letter queue for a handler that keeps failing — nothing crashes the publisher.
  - title: Idempotent delivery
    details: Republishing the same EventId is a no-op for a handler that already saw it, opt-in via IIdempotencyStore.
  - title: Part of the Klexir Ecosystem
    details: One of 7 experimental .NET repos exploring systems-programming concepts — see the full ecosystem on MonadicSharp's docs.
---
