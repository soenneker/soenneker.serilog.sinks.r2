using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;

using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Formatting.Json;
using Soenneker.Cloudflare.R2.Abstract;

namespace Soenneker.Serilog.Sinks.R2;

internal sealed class R2Sink(Func<ICloudflareR2Util> getR2, R2Destination destination, int maxObjectBytes, TimeSpan timeout) : IBatchedLogEventSink
{
    private static readonly AsyncLocal<bool> Uploading = new();
    private static readonly JsonFormatter Formatter = new(closingDelimiter: "\n");
    private static readonly Encoding Utf8 = new UTF8Encoding(false);
    private static readonly Meter Meter = new("Soenneker.Serilog.Sinks.R2");
    private static readonly Counter<long> Uploaded = Meter.CreateCounter<long>("r2.objects.uploaded");
    private static readonly Counter<long> Bytes = Meter.CreateCounter<long>("r2.bytes.uploaded", "By");
    private static readonly Counter<long> Failures = Meter.CreateCounter<long>("r2.upload.failures");
    private static readonly Counter<long> Oversized = Meter.CreateCounter<long>("r2.events.oversized");

    // Serilog delivers serially. A retry may contain new events after the original batch.
    private readonly LimitedEventStream _buffer = new(maxObjectBytes);
    private StreamWriter? _writer;
    private LogEvent? _first;
    private int _nextIndex;
    private string? _pendingKey;
    private int _pendingBytes;

    internal static bool UploadInProgress => Uploading.Value;

    public async Task EmitBatchAsync(IReadOnlyCollection<LogEvent> batch)
    {
        if (batch.Count == 0)
            return;

        using IEnumerator<LogEvent> entries = batch.GetEnumerator();
        if (!entries.MoveNext())
            return;
        if (!ReferenceEquals(entries.Current, _first))
        {
            Reset();
            _first = entries.Current;
        }

        Uploading.Value = true;
        try
        {
            if (_pendingKey != null)
                await UploadPending().ConfigureAwait(false);

            _buffer.SetLength(0);
            _writer ??= new StreamWriter(_buffer, Utf8, leaveOpen: true);
            DateTimeOffset timestamp = default;
            int index = 0;
            do
            {
                int eventIndex = index++;
                if (eventIndex < _nextIndex)
                    continue;

                LogEvent entry = entries.Current;
                _buffer.BeginEvent();
                Formatter.Format(entry, _writer);
                _writer.Flush();
                int start = _buffer.EventStart;
                if (_buffer.Oversized)
                {
                    _buffer.SetLength(start);
                    Oversized.Add(1);
                    SelfLog.WriteLine("R2 skipped an event at {0:o} exceeding the {1}-byte object limit.", entry.Timestamp, maxObjectBytes);
                    continue;
                }

                if (_buffer.Length > maxObjectBytes)
                {
                    // The last complete line belongs to the next object. Move it only after the prefix is acknowledged.
                    await Flush(start, timestamp, eventIndex).ConfigureAwait(false);
                    byte[] bytes = _buffer.GetBuffer();
                    int tail = checked((int)_buffer.Length) - start;
                    bytes.AsSpan(start, tail).CopyTo(bytes);
                    _buffer.SetLength(tail);
                    start = 0;
                }
                if (start == 0)
                    timestamp = entry.Timestamp;
            } while (entries.MoveNext());

            if (_buffer.Length > 0)
                await Flush(checked((int)_buffer.Length), timestamp, index).ConfigureAwait(false);
            Reset();
            // Allow a full default-size object plus its boundary event to reuse the buffer.
            ReleaseExcessCapacity(16 * 1024 * 1024);
        }
        catch
        {
            // Formatting can throw with partially buffered text. Start the next attempt with a clean encoder.
            _writer = null;
            throw;
        }
        finally
        {
            Uploading.Value = false;
        }
    }

    private Task Flush(int count, DateTimeOffset timestamp, int nextIndex)
    {
        _pendingKey = destination.NewKey(timestamp);
        _pendingBytes = count;
        _nextIndex = nextIndex;
        return UploadPending();
    }

    private void ReleaseExcessCapacity(int retainedBytes)
    {
        _buffer.SetLength(0);
        if (_buffer.Capacity > retainedBytes)
            _buffer.Capacity = 0;
    }
    private async Task UploadPending()
    {
        using var deadline = new CancellationTokenSource(timeout);
        try
        {
            using var content = new MemoryStream(_buffer.GetBuffer(), 0, _pendingBytes, writable: false);
            var response = await getR2().PutObject(destination.AccountId, destination.BucketName, _pendingKey!, content,
                "application/x-ndjson", cancellationToken: deadline.Token).ConfigureAwait(false);
            if (response?.Success != true)
                throw new InvalidDataException("Cloudflare did not acknowledge the R2 upload.");

            Uploaded.Add(1);
            Bytes.Add(_pendingBytes);
            _pendingKey = null;
            _pendingBytes = 0;
        }
        catch
        {
            Failures.Add(1);
            throw;
        }
    }

    private void Reset()
    {
        _first = null;
        _nextIndex = 0;
        _pendingKey = null;
        _pendingBytes = 0;
    }

    public Task OnEmptyBatchAsync()
    {
        Reset();
        ReleaseExcessCapacity(64 * 1024);
        return Task.CompletedTask;
    }
}
