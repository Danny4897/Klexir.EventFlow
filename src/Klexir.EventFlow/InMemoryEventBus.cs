using System.Collections.Concurrent;
using Klexir.EventFlow.Abstractions;

namespace Klexir.EventFlow;

/// <summary>
/// Thread-safe registration with deterministic handler invocation per publication.
/// Resilience (timeout, retry, backpressure, dead-lettering) is opt-in via <see cref="EventBusResilienceOptions"/>;
/// with defaults, behavior matches the Stage 1 baseline exactly.
/// </summary>
public sealed class InMemoryEventBus : IEventBus
{
    private readonly ConcurrentDictionary<Type, IHandlerInvoker[]> _handlers = new();
    private readonly EventDispatchMode _dispatchMode;
    private readonly IDeadLetterQueue? _deadLetterQueue;
    private readonly IIdempotencyStore? _idempotencyStore;
    private readonly EventBusResilienceOptions _resilience;
    private readonly ChannelBackpressureGate? _backpressureGate;

    public InMemoryEventBus(
        EventDispatchMode dispatchMode = EventDispatchMode.Sequential,
        IDeadLetterQueue? deadLetterQueue = null,
        EventBusResilienceOptions? resilience = null,
        IIdempotencyStore? idempotencyStore = null)
    {
        _dispatchMode = dispatchMode;
        _deadLetterQueue = deadLetterQueue;
        _idempotencyStore = idempotencyStore;
        _resilience = resilience ?? new EventBusResilienceOptions();

        if (_resilience.MaxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(resilience), _resilience.MaxAttempts, "MaxAttempts must be at least 1.");
        }

        _backpressureGate = _resilience.MaxConcurrentDispatches is { } maxConcurrency
            ? new ChannelBackpressureGate(maxConcurrency)
            : null;
    }

    public void Register<TEvent>(IEventHandler<TEvent> handler) where TEvent : IEvent
    {
        ArgumentNullException.ThrowIfNull(handler);

        _handlers.AddOrUpdate(
            typeof(TEvent),
            _ => [new HandlerInvoker<TEvent>(handler)],
            (_, handlers) => [.. handlers, new HandlerInvoker<TEvent>(handler)]);
    }

    public async ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : IEvent
    {
        ArgumentNullException.ThrowIfNull(@event);

        if (!_handlers.TryGetValue(typeof(TEvent), out var handlers))
        {
            return;
        }

        if (_dispatchMode is EventDispatchMode.Parallel)
        {
            await Task.WhenAll(handlers.Select(handler => DispatchWithResilienceAsync(handler, @event, cancellationToken).AsTask()))
                .ConfigureAwait(false);
            return;
        }

        foreach (var handler in handlers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await DispatchWithResilienceAsync(handler, @event, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask DispatchWithResilienceAsync(IHandlerInvoker handler, IEvent @event, CancellationToken cancellationToken)
    {
        if (_idempotencyStore is not null)
        {
            var idempotencyKey = $"{@event.EventId:N}:{handler.HandlerId}";
            var firstDelivery = await _idempotencyStore.TryMarkProcessedAsync(idempotencyKey, cancellationToken).ConfigureAwait(false);
            if (!firstDelivery)
            {
                return;
            }
        }

        if (_backpressureGate is not null)
        {
            await _backpressureGate.AcquireAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            var attempt = 0;
            while (true)
            {
                attempt++;
                try
                {
                    await InvokeWithTimeoutAsync(handler, @event, cancellationToken).ConfigureAwait(false);
                    return;
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                {
                    if (attempt < _resilience.MaxAttempts)
                    {
                        if (_resilience.RetryDelay > TimeSpan.Zero)
                        {
                            await Task.Delay(_resilience.RetryDelay, cancellationToken).ConfigureAwait(false);
                        }

                        continue;
                    }

                    if (_deadLetterQueue is null)
                    {
                        throw;
                    }

                    await _deadLetterQueue.EnqueueAsync(
                        new DeadLetterEnvelope(@event, handler.HandlerTypeName, ex.Message, attempt, DateTimeOffset.UtcNow),
                        cancellationToken).ConfigureAwait(false);
                    return;
                }
            }
        }
        finally
        {
            _backpressureGate?.Release();
        }
    }

    private async Task InvokeWithTimeoutAsync(IHandlerInvoker handler, IEvent @event, CancellationToken cancellationToken)
    {
        if (_resilience.HandlerTimeout is not { } timeout)
        {
            await handler.HandleAsync(@event, cancellationToken).AsTask().ConfigureAwait(false);
            return;
        }

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);

        try
        {
            await handler.HandleAsync(@event, timeoutCts.Token).AsTask().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Handler '{handler.HandlerTypeName}' timed out after {timeout}.");
        }
    }

    private interface IHandlerInvoker
    {
        string HandlerTypeName { get; }

        /// <summary>Unique per registered handler instance; two handlers of the same type get distinct ids.</summary>
        string HandlerId { get; }

        ValueTask HandleAsync(IEvent @event, CancellationToken cancellationToken);
    }

    private sealed class HandlerInvoker<TEvent>(IEventHandler<TEvent> handler) : IHandlerInvoker where TEvent : IEvent
    {
        private readonly string _handlerId = Guid.NewGuid().ToString("N");

        public string HandlerTypeName => handler.GetType().Name;

        public string HandlerId => _handlerId;

        public ValueTask HandleAsync(IEvent @event, CancellationToken cancellationToken) =>
            handler.HandleAsync((TEvent)@event, cancellationToken);
    }
}
