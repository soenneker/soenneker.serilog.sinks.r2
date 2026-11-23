using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Soenneker.Cloudflare.HttpClient.Registrars;
using NSubstitute;
using Serilog;
using Serilog.Events;
using Serilog.Parsing;
using Soenneker.Cloudflare.HttpClient.Abstract;

namespace Soenneker.Serilog.Sinks.R2.Tests;

public sealed class R2DeliveryTests
{
    private static LogEvent Event(string text) => new(DateTimeOffset.UtcNow, LogEventLevel.Information, null,
        new MessageTemplateParser().Parse("{Text}"), [new LogEventProperty("Text", new ScalarValue(text))]);

    private static R2Sink Sink(HttpClient http, int limit = 512, TimeSpan? timeout = null)
    {
        var provider = Substitute.For<ICloudflareHttpClient>();
        provider.Get(Arg.Any<CancellationToken>()).Returns(new ValueTask<HttpClient>(http));
        return new R2Sink(() => provider, new R2Destination("account", "bucket", "prod #1/%"), limit, timeout ?? TimeSpan.FromSeconds(10));
    }

    private static HttpResponseMessage Success() => new(HttpStatusCode.OK) { Content = new StringContent("""{"success":true}""") };

    [Test]
    public async Task RegisteredClientTargetsCloudflareWithConfiguredToken()
    {
        using var config = new ConfigurationManager();
        config["Cloudflare:ApiKey"] = "test-token";
        config["Cloudflare:RequestResponseLogging"] = "true";
        await using var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(config)
            .AddLogging()
            .AddCloudflareHttpClientAsSingleton()
            .BuildServiceProvider();

        HttpClient http = await services.GetRequiredService<ICloudflareHttpClient>().Get();
        await Assert.That(http.BaseAddress?.AbsoluteUri).IsEqualTo("https://api.cloudflare.com/client/v4/");
        await Assert.That(http.DefaultRequestHeaders.Authorization?.ToString()).IsEqualTo("Bearer test-token");
    }
    [Test]
    public async Task PartialBatchRetryPreservesPayloadAndAcceptsLaterEvents()
    {
        var writes = new List<(string Path, string Payload)>();
        using var http = new HttpClient(new RecordingHandler(async (request, token) =>
        {
            writes.Add((request.RequestUri!.AbsolutePath, await request.Content!.ReadAsStringAsync(token)));
            return writes.Count == 2 ? new HttpResponseMessage(HttpStatusCode.TooManyRequests) : Success();
        })) { BaseAddress = new Uri("https://api.cloudflare.com/client/v4/") };

        var sink = Sink(http);
        // Each normal event fits on its own. The oversized event between them cannot poison its neighbors.
        LogEvent[] original = [Event(new string('a', 250)), Event(new string('界', 700)), Event(new string('b', 250))];
        await Assert.That(async () => await sink.EmitBatchAsync(original)).Throws<HttpRequestException>();
        await sink.EmitBatchAsync([.. original, Event(new string('c', 250))]);

        await Assert.That(writes.Count).IsEqualTo(4);
        await Assert.That(writes[1]).IsEqualTo(writes[2]);
        await Assert.That(writes.Select(w => w.Path).Distinct().Count()).IsEqualTo(3);
        string letters = "";
        foreach (var write in writes)
        {
            await Assert.That(write.Path).Contains("/objects/prod%20%231/%25/");
            await Assert.That(Encoding.UTF8.GetByteCount(write.Payload)).IsLessThanOrEqualTo(512);
            using var line = JsonDocument.Parse(write.Payload);
            letters += line.RootElement.GetProperty("Properties").GetProperty("Text").GetString()![0];
        }
        await Assert.That(letters).IsEqualTo("abbc");
    }

    [Test, Arguments("false"), Arguments("missing"), Arguments("malformed")]
    public async Task UnacknowledgedResponseRetainsObject(string mode)
    {
        var keys = new List<Uri>();
        using var http = new HttpClient(new RecordingHandler((request, _) =>
        {
            keys.Add(request.RequestUri!);
            string body = mode switch { "false" => """{"success":false}""", "missing" => "{}", _ => "" };
            return Task.FromResult(keys.Count == 1 ? new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) } : Success());
        })) { BaseAddress = new Uri("https://api.cloudflare.com/client/v4/") };
        var sink = Sink(http);
        LogEvent[] events = [Event("pending")];

        try
        {
            await sink.EmitBatchAsync(events);
            throw new InvalidOperationException("Unacknowledged upload accepted");
        }
        catch (Exception ex) when (ex is System.IO.InvalidDataException or JsonException) { }

        await sink.EmitBatchAsync(events);
        await Assert.That(keys.Count).IsEqualTo(2);
        await Assert.That(keys[0]).IsEqualTo(keys[1]);
    }

    [Test]
    public async Task UploadDeadlineCancelsTransport()
    {
        using var http = new HttpClient(new RecordingHandler(async (_, token) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
            return Success();
        })) { BaseAddress = new Uri("https://api.cloudflare.com/client/v4/") };
        var sink = Sink(http, timeout: TimeSpan.FromMilliseconds(50));
        await Assert.That(async () => await sink.EmitBatchAsync([Event("deadline")]).WaitAsync(TimeSpan.FromSeconds(5)))
            .Throws<TaskCanceledException>();
    }

    [Test, Arguments("../other"), Arguments("a/./b"), Arguments("a//b"), Arguments("a\\b")]
    public async Task RejectsUnsafePrefixesBeforeStarting(string prefix)
    {
        await Assert.That(() => new LoggerConfiguration().WriteTo.R2(() => throw new InvalidOperationException(),
            "account", "bucket", prefix)).Throws<ArgumentException>();
    }

    [Test]
    public async Task RejectsPrefixExceedingUtf8KeyLimit()
    {
        await Assert.That(() => new LoggerConfiguration().WriteTo.R2(() => throw new InvalidOperationException(),
            "account", "bucket", new string('界', 324))).Throws<ArgumentException>();
    }
}


