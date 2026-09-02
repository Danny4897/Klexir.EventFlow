using System.Threading.Channels;

namespace Klexir.EventFlow;

/// <summary>
/// Bounds concurrent handler execution using a pre-filled bounded <see cref="Channel{T}"/> as a permit pool:
/// acquiring reads a permit (blocking when none are available), releasing writes one back.
/// </summary>
internal sealed class ChannelBackpressureGate
{
    private readonly Channel<byte> _permits;

    public ChannelBackpressureGate(int maxConcurrency)
    {
        if (maxConcurrency <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency), maxConcurrency, "Must be at least 1.");
        }

        _permits = Channel.CreateBounded<byte>(maxConcurrency);
        for (var i = 0; i < maxConcurrency; i++)
        {
            _permits.Writer.TryWrite(0);
        }
    }

    public async ValueTask AcquireAsync(CancellationToken cancellationToken) =>
        await _permits.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);

    public void Release() => _permits.Writer.TryWrite(0);
}
