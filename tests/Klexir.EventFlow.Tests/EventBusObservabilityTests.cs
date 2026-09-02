using System.Diagnostics;
using FluentAssertions;
using Klexir.EventFlow.Abstractions;
using Xunit;

namespace Klexir.EventFlow.Tests;

public sealed class EventBusObservabilityTests
{
    [Fact]
    public async Task PublishAsync_starts_a_publish_activity_with_event_and_handler_count_tags()
    {
        var eventId = Guid.NewGuid();
        using var captured = CaptureActivities();

        var bus = new InMemoryEventBus();
        bus.Register(new NoOpHandler());
        await bus.PublishAsync(new OrderPlaced(eventId, DateTimeOffset.UtcNow));

        var publishActivity = captured.Activities.Single(a => a.OperationName == "EventFlow.Publish" && HasEventId(a, eventId));

        publishActivity.GetTagItem("klexir.event.type").Should().Be(nameof(OrderPlaced));
        publishActivity.GetTagItem("klexir.handler.count").Should().Be(1);
    }

    [Fact]
    public async Task PublishAsync_starts_a_child_handle_activity_per_handler()
    {
        var eventId = Guid.NewGuid();
        using var captured = CaptureActivities();

        var bus = new InMemoryEventBus();
        bus.Register(new NoOpHandler());
        await bus.PublishAsync(new OrderPlaced(eventId, DateTimeOffset.UtcNow));

        var publishActivity = captured.Activities.Single(a => a.OperationName == "EventFlow.Publish" && HasEventId(a, eventId));
        var handleActivity = captured.Activities.Single(a => a.OperationName == "EventFlow.Handle" && HasEventId(a, eventId));

        handleActivity.ParentId.Should().Be(publishActivity.Id);
        handleActivity.GetTagItem("klexir.handler.type").Should().Be(nameof(NoOpHandler));
        handleActivity.Status.Should().Be(ActivityStatusCode.Ok);
    }

    [Fact]
    public async Task A_dead_lettered_handler_marks_its_handle_activity_as_an_error()
    {
        var eventId = Guid.NewGuid();
        using var captured = CaptureActivities();

        var bus = new InMemoryEventBus(deadLetterQueue: new InMemoryDeadLetterQueue());
        bus.Register(new ThrowingHandler());
        await bus.PublishAsync(new OrderPlaced(eventId, DateTimeOffset.UtcNow));

        var handleActivity = captured.Activities.Single(a => a.OperationName == "EventFlow.Handle" && HasEventId(a, eventId));

        handleActivity.Status.Should().Be(ActivityStatusCode.Error);
    }

    private static bool HasEventId(Activity activity, Guid eventId) =>
        activity.GetTagItem("klexir.event.id") is string tag && tag == eventId.ToString();

    private static CapturedActivities CaptureActivities()
    {
        var activities = new List<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == InMemoryEventBus.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
            {
                lock (activities)
                {
                    activities.Add(activity);
                }
            },
        };
        ActivitySource.AddActivityListener(listener);
        return new CapturedActivities(listener, activities);
    }

    private sealed class CapturedActivities(ActivityListener listener, List<Activity> activities) : IDisposable
    {
        // Other test classes running concurrently share this process-wide ActivitySource, so this list is written
        // from multiple threads; snapshot under lock rather than exposing the live list to the caller's LINQ query.
        public IReadOnlyList<Activity> Activities
        {
            get
            {
                lock (activities)
                {
                    return [.. activities];
                }
            }
        }

        public void Dispose() => listener.Dispose();
    }

    private sealed record OrderPlaced(Guid EventId, DateTimeOffset OccurredAt) : IEvent;

    private sealed class NoOpHandler : IEventHandler<OrderPlaced>
    {
        public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class ThrowingHandler : IEventHandler<OrderPlaced>
    {
        public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("boom");
    }
}
