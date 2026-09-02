using FluentAssertions;
using Klexir.EventFlow.Abstractions;
using Xunit;

namespace Klexir.EventFlow.Tests;

public sealed class EventBusResilienceTests
{
    [Fact]
    public async Task PublishAsync_routes_handler_to_dead_letter_queue_after_max_attempts_exhausted()
    {
        var deadLetterQueue = new InMemoryDeadLetterQueue();
        var bus = new InMemoryEventBus(
            deadLetterQueue: deadLetterQueue,
            resilience: new EventBusResilienceOptions { MaxAttempts = 2 });
        bus.Register(new AlwaysFailingHandler());

        await bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow));

        var entry = deadLetterQueue.Snapshot().Should().ContainSingle().Which;
        entry.FailureReason.Should().Be("boom");
        entry.AttemptCount.Should().Be(2);
    }

    [Fact]
    public async Task PublishAsync_retries_handler_until_it_succeeds_within_max_attempts()
    {
        var deadLetterQueue = new InMemoryDeadLetterQueue();
        var handler = new FlakyHandler(failuresBeforeSuccess: 2);
        var bus = new InMemoryEventBus(
            deadLetterQueue: deadLetterQueue,
            resilience: new EventBusResilienceOptions { MaxAttempts = 3 });
        bus.Register(handler);

        await bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow));

        handler.AttemptCount.Should().Be(3);
        deadLetterQueue.Snapshot().Should().BeEmpty();
    }

    [Fact]
    public async Task PublishAsync_dead_letters_a_handler_that_exceeds_the_configured_timeout()
    {
        var deadLetterQueue = new InMemoryDeadLetterQueue();
        var bus = new InMemoryEventBus(
            deadLetterQueue: deadLetterQueue,
            resilience: new EventBusResilienceOptions
            {
                MaxAttempts = 1,
                HandlerTimeout = TimeSpan.FromMilliseconds(50),
            });
        bus.Register(new NeverCompletingHandler());

        await bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow));

        deadLetterQueue.Snapshot().Should().ContainSingle();
    }

    [Fact]
    public async Task PublishAsync_in_parallel_mode_bounds_concurrent_handler_execution_via_backpressure()
    {
        var firstStarted = new ManualResetEventSlim(false);
        var secondStarted = new ManualResetEventSlim(false);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var bus = new InMemoryEventBus(
            EventDispatchMode.Parallel,
            resilience: new EventBusResilienceOptions { MaxConcurrentDispatches = 1 });

        bus.Register(new TrackingHandler(async () =>
        {
            firstStarted.Set();
            await releaseFirst.Task;
        }));
        bus.Register(new TrackingHandler(() =>
        {
            secondStarted.Set();
            return Task.CompletedTask;
        }));

        var publishing = bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow)).AsTask();

        firstStarted.Wait(TimeSpan.FromSeconds(1)).Should().BeTrue();
        secondStarted.Wait(TimeSpan.FromMilliseconds(100)).Should()
            .BeFalse("the backpressure gate should hold the second handler back while the first is still running");

        releaseFirst.SetResult();
        await publishing;

        secondStarted.Wait(TimeSpan.FromSeconds(1)).Should().BeTrue();
    }

    private sealed record OrderPlaced(Guid EventId, DateTimeOffset OccurredAt) : IEvent;

    private sealed class AlwaysFailingHandler : IEventHandler<OrderPlaced>
    {
        public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("boom");
    }

    private sealed class FlakyHandler(int failuresBeforeSuccess) : IEventHandler<OrderPlaced>
    {
        public int AttemptCount { get; private set; }

        public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
        {
            AttemptCount++;
            if (AttemptCount <= failuresBeforeSuccess)
            {
                throw new InvalidOperationException("not yet");
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class NeverCompletingHandler : IEventHandler<OrderPlaced>
    {
        public async ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }
    }

    private sealed class TrackingHandler(Func<Task> onHandle) : IEventHandler<OrderPlaced>
    {
        public async ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken) =>
            await onHandle();
    }
}
