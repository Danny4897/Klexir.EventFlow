using FluentAssertions;
using Klexir.EventFlow.Abstractions;
using Xunit;

namespace Klexir.EventFlow.Tests;

public sealed class InMemoryEventStoreTests
{
    [Fact]
    public async Task ReadStreamAsync_returns_events_in_the_order_they_were_appended()
    {
        var store = new InMemoryEventStore();
        var streamId = Guid.NewGuid().ToString("N");
        await store.AppendAsync(streamId, new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow, 1));
        await store.AppendAsync(streamId, new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow, 2));

        var events = await store.ReadStreamAsync(streamId);

        events.Should().HaveCount(2);
        ((OrderPlaced)events[0]).Sequence.Should().Be(1);
        ((OrderPlaced)events[1]).Sequence.Should().Be(2);
    }

    [Fact]
    public async Task ReadStreamAsync_with_fromVersion_skips_earlier_events()
    {
        var store = new InMemoryEventStore();
        var streamId = Guid.NewGuid().ToString("N");
        await store.AppendAsync(streamId, new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow, 1));
        await store.AppendAsync(streamId, new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow, 2));
        await store.AppendAsync(streamId, new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow, 3));

        var events = await store.ReadStreamAsync(streamId, fromVersion: 1);

        events.Should().HaveCount(2);
        ((OrderPlaced)events[0]).Sequence.Should().Be(2);
        ((OrderPlaced)events[1]).Sequence.Should().Be(3);
    }

    [Fact]
    public async Task ReadStreamAsync_for_an_unknown_stream_returns_an_empty_list()
    {
        var store = new InMemoryEventStore();

        var events = await store.ReadStreamAsync("missing-stream");

        events.Should().BeEmpty();
    }

    [Fact]
    public async Task AppendAsync_keeps_different_streams_independent()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("stream-a", new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow, 1));
        await store.AppendAsync("stream-b", new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow, 2));

        var streamA = await store.ReadStreamAsync("stream-a");
        var streamB = await store.ReadStreamAsync("stream-b");

        ((OrderPlaced)streamA.Should().ContainSingle().Subject).Sequence.Should().Be(1);
        ((OrderPlaced)streamB.Should().ContainSingle().Subject).Sequence.Should().Be(2);
    }

    [Fact]
    public async Task AppendAsync_does_not_lose_events_under_concurrent_writers()
    {
        var store = new InMemoryEventStore();
        var streamId = Guid.NewGuid().ToString("N");

        await Task.WhenAll(Enumerable.Range(0, 200)
            .Select(i => store.AppendAsync(streamId, new OrderPlaced(Guid.NewGuid(), DateTimeOffset.UtcNow, i)).AsTask()));

        var events = await store.ReadStreamAsync(streamId);

        events.Should().HaveCount(200);
    }

    private sealed record OrderPlaced(Guid EventId, DateTimeOffset OccurredAt, int Sequence) : IEvent;
}
