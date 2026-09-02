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

    [Fact]
    public async Task PublishAsync_with_parallel_mode_starts_all_handlers_before_completion()
    {
        var started = new CountdownEvent(2);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var bus = new InMemoryEventBus(EventDispatchMode.Parallel);
        bus.Register(new BlockingHandler(started, release));
        bus.Register(new BlockingHandler(started, release));

        var publishing = bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow)).AsTask();

        started.Wait(TimeSpan.FromSeconds(1)).Should().BeTrue();
        release.SetResult();
        await publishing;
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

    private sealed class BlockingHandler(CountdownEvent started, TaskCompletionSource release) : IEventHandler<OrderPlaced>
    {
        public async ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
        {
            started.Signal();
            await release.Task.WaitAsync(cancellationToken);
        }
    }
}
