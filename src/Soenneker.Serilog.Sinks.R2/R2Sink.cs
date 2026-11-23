using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Formatting.Json;
using Soenneker.Cloudflare.HttpClient.Abstract;

namespace Soenneker.Serilog.Sinks.R2;

internal sealed class R2Sink(Func<ICloudflareHttpClient> getClient, R2Destination destination, int maxObjectBytes, TimeSpan timeout) : IBatchedLogEventSink
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
    private LogEvent? _first;
    private int _nextIndex;
    private string? _pendingPath;
    private ArraySegment<byte> _pendingBody;

    internal static bool UploadInProgress => Uploading.Value;

    public async Task EmitBatchAsync(IReadOnlyCollection<LogEvent> batch)
    {
        if (batch.Count == 0)
            return;

        LogEvent first = batch.First();
        if (!ReferenceEquals(first, _first))
        {
            Reset();
            _first = first;
        }

        Uploading.Value = true;
        try
        {
            if (_pendingPath != null)
                await UploadPending().ConfigureAwait(false);

            using var line = new LimitedEventStream(maxObjectBytes);
            using var writer = new StreamWriter(line, Utf8, leaveOpen: true);
            using var chunk = new MemoryStream();
            DateTimeOffset timestamp = default;
            int index = _nextIndex;
            foreach (LogEvent entry in batch.Skip(_nextIndex))
            {
                line.Reset();
                Formatter.Format(entry, writer);
                writer.Flush();
                if (line.Oversized)
                {
                    Oversized.Add(1);
                    SelfLog.WriteLine("R2 skipped an event at {0:o} exceeding the {1}-byte object limit.", entry.Timestamp, maxObjectBytes);
                    index++;
                    continue;
                }

                if (chunk.Length + line.Length > maxObjectBytes)
                    await Flush(chunk, timestamp, index).ConfigureAwait(false);

                if (chunk.Length == 0)
                    timestamp = entry.Timestamp;
                line.WriteTo(chunk);
                index++;
            }

            if (chunk.Length > 0)
                await Flush(chunk, timestamp, index).ConfigureAwait(false);
            Reset();
        }
        finally
        {
            Uploading.Value = false;
        }
    }

    private async Task Flush(MemoryStream chunk, DateTimeOffset timestamp, int nextIndex)
    {
        _pendingPath = destination.NewPath(timestamp);
        _pendingBody = new ArraySegment<byte>(chunk.GetBuffer(), 0, checked((int)chunk.Length));
        _nextIndex = nextIndex;
        await UploadPending().ConfigureAwait(false);
        chunk.SetLength(0);
    }

    private async Task UploadPending()
    {
        using var deadline = new CancellationTokenSource(timeout);
        try
        {
            HttpClient client = await getClient().Get(deadline.Token).ConfigureAwait(false);
            using var body = new ByteArrayContent(_pendingBody.Array!, _pendingBody.Offset, _pendingBody.Count);
            body.Headers.ContentType = new MediaTypeHeaderValue("application/x-ndjson");
            using var request = new HttpRequestMessage(HttpMethod.Put, _pendingPath) { Content = body };
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, deadline.Token).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            await response.Content.LoadIntoBufferAsync(64 * 1024, deadline.Token).ConfigureAwait(false);
            using var envelope = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(deadline.Token).ConfigureAwait(false));
            if (!envelope.RootElement.TryGetProperty("success", out JsonElement success) || success.ValueKind != JsonValueKind.True)
                throw new InvalidDataException("Cloudflare did not acknowledge the R2 upload.");

            Uploaded.Add(1);
            Bytes.Add(_pendingBody.Count);
            _pendingPath = null;
            _pendingBody = default;
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
        _pendingPath = null;
        _pendingBody = default;
    }

    public Task OnEmptyBatchAsync()
    {
        Reset();
        return Task.CompletedTask;
    }
}

