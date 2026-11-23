using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Soenneker.Cloudflare.HttpClient.Abstract;

namespace Soenneker.Serilog.Sinks.R2.Tests;

public sealed class R2SinkTests
{
    [Test, Arguments(false), Arguments(true)]
    public async Task FlushUploadsStructuredEventsThroughCloudflare(bool retry)
    {
        var requests = new ConcurrentQueue<(Uri Uri, string Body, string? ContentType)>();
        Logger? logger = null;
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var handler = new RecordingHandler(async (request, _) =>
        {
            requests.Enqueue((request.RequestUri!, await request.Content!.ReadAsStringAsync(), request.Content.Headers.ContentType?.MediaType));
            logger!.Information("Transport diagnostic");
            if (retry && requests.Count == 1)
                throw new HttpRequestException("Lost upload response");

            delivered.TrySetResult();
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""{"success":true,"result":{}}""", Encoding.UTF8, "application/json")
            };
        });
        using var http = new HttpClient(handler) { BaseAddress = new Uri("https://api.cloudflare.com/client/v4/") };
        var transport = Substitute.For<ICloudflareHttpClient>();
        transport.Get(Arg.Any<CancellationToken>()).Returns(new ValueTask<HttpClient>(http));

        int observed = 0;
        logger = new LoggerConfiguration().MinimumLevel.Debug()
            .WriteTo.Sink(new CountingSink(() => Interlocked.Increment(ref observed)))
            .WriteTo.R2(() => transport, "account", "bucket", "/apps/service/",
                new BatchingOptions { BatchSizeLimit = retry ? 2 : 100, EagerlyEmitFirstEvent = false, BufferingTimeLimit = TimeSpan.FromMilliseconds(retry ? 50 : 60000) },
                restrictedToMinimumLevel: LogEventLevel.Information)
            .CreateLogger();

        var timestamp = new DateTimeOffset(2026, 11, 23, 23, 30, 0, TimeSpan.FromHours(-5));
        logger.Debug("Below threshold");
        logger.Write(new LogEvent(timestamp, LogEventLevel.Information, null,
            new global::Serilog.Parsing.MessageTemplateParser().Parse("Received {Value}"),
            [new LogEventProperty("Value", new ScalarValue("café\nline"))]));
        logger.Error(new InvalidOperationException("failure"), "Second event");
        try
        {
            if (retry)
                await delivered.Task.WaitAsync(TimeSpan.FromSeconds(20));
        }
        finally
        {
            await logger.DisposeAsync();
        }

        await Assert.That(requests.Count).IsEqualTo(retry ? 2 : 1);
        var upload = requests.Last();
        await Assert.That(requests.First()).IsEqualTo(upload);
        string path = upload.Uri.AbsolutePath;
        await Assert.That(path).Contains("/accounts/account/r2/buckets/bucket/objects/apps/service/2026/11/24/");
        await Assert.That(path).EndsWith(".ndjson");
        await Assert.That(upload.ContentType).IsEqualTo("application/x-ndjson");
        string[] lines = upload.Body.Split('\n');
        await Assert.That(lines.Length).IsEqualTo(3);
        await Assert.That(lines[2]).IsEqualTo("");
        using var first = JsonDocument.Parse(lines[0]);
        using var second = JsonDocument.Parse(lines[1]);
        await Assert.That(first.RootElement.GetProperty("Properties").GetProperty("Value").GetString()).IsEqualTo("café\nline");
        await Assert.That(second.RootElement.GetProperty("Exception").GetString()).Contains("failure");
        await Assert.That(observed).IsEqualTo(retry ? 5 : 4);
    }
}





