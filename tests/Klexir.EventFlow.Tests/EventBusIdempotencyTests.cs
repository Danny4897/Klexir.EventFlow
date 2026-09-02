using FluentAssertions;
using Klexir.EventFlow.Abstractions;
using Xunit;

namespace Klexir.EventFlow.Tests;

public sealed class EventBusIdempotencyTests
{
    [Fact]
    public async Task PublishAsync_skips_a_handler_for_an_event_id_it_has_already_processed()
    {
        var handler = new CountingHandler();
        var bus = new InMemoryEventBus(idempotencyStore: new InMemoryIdempotencyStore());
        bus.Register(handler);
        var eventId = Guid.NewGuid();

        await bus.PublishAsync(new OrderPlaced(eventId, DateTimeOffset.UtcNow));
        await bus.PublishAsync(new OrderPlaced(eventId, DateTimeOffset.UtcNow));

        handler.CallCount.Should().Be(1);
    }

    [Fact]
    public async Task PublishAsync_processes_a_new_event_id_normally()
    {
        var handler = new CountingHandler();
        var bus = new InMemoryEventBus(idempotencyStore: new InMemoryIdempotencyStore());
        bus.Register(handler);

        await bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow));
        await bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow));

        handler.CallCount.Should().Be(2);
    }

    [Fact]
    public async Task PublishAsync_tracks_idempotency_independently_per_handler()
    {
        var first = new CountingHandler();
        var second = new CountingHandler();
        var bus = new InMemoryEventBus(idempotencyStore: new InMemoryIdempotencyStore());
        bus.Register(first);
        bus.Register(second);
        var eventId = Guid.NewGuid();

        await bus.PublishAsync(new OrderPlaced(eventId, DateTimeOffset.UtcNow));
        await bus.PublishAsync(new OrderPlaced(eventId, DateTimeOffset.UtcNow));

        first.CallCount.Should().Be(1);
        second.CallCount.Should().Be(1);
    }

    [Fact]
    public async Task PublishAsync_without_an_idempotency_store_processes_every_publication()
    {
        var handler = new CountingHandler();
        var bus = new InMemoryEventBus();
        bus.Register(handler);
        var eventId = Guid.NewGuid();

        await bus.PublishAsync(new OrderPlaced(eventId, DateTimeOffset.UtcNow));
        await bus.PublishAsync(new OrderPlaced(eventId, DateTimeOffset.UtcNow));

        handler.CallCount.Should().Be(2);
    }

    private sealed record OrderPlaced(Guid EventId, DateTimeOffset OccurredAt) : IEvent;

    private sealed class CountingHandler : IEventHandler<OrderPlaced>
    {
        public int CallCount { get; private set; }

        public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
        {
            CallCount++;
            return ValueTask.CompletedTask;
        }
    }
}
