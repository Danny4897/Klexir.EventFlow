using FluentAssertions;
using Klexir.EventFlow.Abstractions;
using Xunit;

namespace Klexir.EventFlow.Tests;

public sealed class InMemoryEventBusTests
{
    [Fact]
    public async Task PublishAsync_invokes_handlers_in_registration_order()
    {
        var calls = new List<string>();
        var bus = new InMemoryEventBus();
        bus.Register(new RecordingHandler("first", calls));
        bus.Register(new RecordingHandler("second", calls));

        await bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow));

        calls.Should().Equal("first", "second");
    }

    [Fact]
    public async Task PublishAsync_does_not_invoke_handlers_for_a_different_event_type()
    {
        var calls = new List<string>();
        var bus = new InMemoryEventBus();
        bus.Register(new RecordingHandler("order", calls));

        await bus.PublishAsync(new InvoiceIssued(Guid.NewGuid(), DateTimeOffset.UtcNow));

        calls.Should().BeEmpty();
    }

    private sealed record OrderPlaced(Guid EventId, DateTimeOffset OccurredAt) : IEvent;

    private sealed record InvoiceIssued(Guid EventId, DateTimeOffset OccurredAt) : IEvent;

    private sealed class RecordingHandler(string name, ICollection<string> calls) : IEventHandler<OrderPlaced>
    {
        public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
        {
            calls.Add(name);
            return ValueTask.CompletedTask;
        }
    }
}
