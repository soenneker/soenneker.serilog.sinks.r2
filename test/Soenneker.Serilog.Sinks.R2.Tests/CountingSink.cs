using System;
using Serilog.Core;
using Serilog.Events;

namespace Soenneker.Serilog.Sinks.R2.Tests;

internal sealed class CountingSink(Action count) : ILogEventSink
{
    public void Emit(LogEvent logEvent) => count();
}
