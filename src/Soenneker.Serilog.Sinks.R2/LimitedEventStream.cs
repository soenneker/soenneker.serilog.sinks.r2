using System;
using System.IO;

namespace Soenneker.Serilog.Sinks.R2;

// StreamWriter encodes incrementally. Once the limit is reached, discard the rest of this event.
internal sealed class LimitedEventStream(int limit) : MemoryStream
{
    public int EventStart { get; private set; }
    public bool Oversized { get; private set; }

    public override void Write(byte[] buffer, int offset, int count)
    {
        if (Accept(count))
            base.Write(buffer, offset, count);
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        if (Accept(buffer.Length))
            base.Write(buffer);
    }

    private bool Accept(int count)
    {
        Oversized |= count > limit - (Length - EventStart);
        return !Oversized;
    }

    public void BeginEvent()
    {
        EventStart = checked((int)Length);
        Oversized = false;
    }
}
