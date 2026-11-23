[![](https://img.shields.io/nuget/v/soenneker.serilog.sinks.r2.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.serilog.sinks.r2/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.serilog.sinks.r2/build-and-test.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.serilog.sinks.r2/actions/workflows/build-and-test.yml)
[![](https://img.shields.io/nuget/dt/soenneker.serilog.sinks.r2.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.serilog.sinks.r2/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.serilog.sinks.r2/codeql.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.serilog.sinks.r2/actions/workflows/codeql.yml)

# ![](https://user-images.githubusercontent.com/4441470/224455560-91ed3ee7-f510-4041-a8d2-3fc093025112.png) Soenneker.Serilog.Sinks.R2
### A batched Serilog sink for structured logs in Cloudflare R2.

## Installation

```
dotnet add package Soenneker.Serilog.Sinks.R2
```

## Configure the sink

Register the Cloudflare HTTP client provider and resolve it when the first batch is ready. The following example uses Serilog's hosting integration.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Soenneker.Cloudflare.HttpClient.Abstract;
using Soenneker.Cloudflare.HttpClient.Registrars;
using Soenneker.Serilog.Sinks.R2;

builder.Services.AddCloudflareHttpClientAsSingleton();
builder.Services.AddSerilog((services, logger) => logger
    .WriteTo.Console()
    .WriteTo.R2(services.GetRequiredService<ICloudflareHttpClient>,
        accountId: "your-cloudflare-account-id",
        bucketName: "application-logs",
        prefix: "production/api"));
```

Create the destination bucket and supply a Cloudflare API token with object write access through the application's secret provider under `Cloudflare:ApiKey`. Set `Cloudflare:RequestResponseLogging` to `true` to log transport diagnostics. Upload diagnostics go to the other configured sinks so they cannot trigger recursive uploads.

## Storage and delivery

Each object contains UTF-8 newline-delimited JSON, for example `production/api/2026/11/23/70cc50f8-cd1c-48cb-9a7a-5356a224673e.ndjson`. Its UTC date is the date of the first event in that object. Each line contains the original timestamp, level, message template, structured properties and exception when present.

| Setting | Default | Effect |
| --- | --- | --- |
| `maxObjectBytes` | 4 MiB | Splits a batch on whole-event boundaries. An event larger than the entire limit is counted and skipped. |
| `uploadTimeout` | 30 seconds | Bounds one upload attempt, including reading the acknowledgement. |
| `batchingOptions.BatchSizeLimit` | 1,000 events | Maximum events delivered to the sink together. |
| `batchingOptions.BufferingTimeLimit` | 2 seconds | Maximum normal batching delay. |
| `batchingOptions.QueueLimit` | 100,000 events | Bounds the in-process Serilog queue by event count. |
| `batchingOptions.RetryTimeLimit` | 10 minutes | Controls how long Serilog retries failed batches. |

A successful write requires both an HTTP success status and a Cloudflare acknowledgement. After a timeout or lost response, the sink retries the exact bytes at the same key. Objects already acknowledged in a split batch are skipped on retry. Concurrent application instances generate independent keys.

Buffering is in memory. Process termination, queue overflow, an exhausted retry window or a failed shutdown flush can lose events. Dispose the logger before disposing its Cloudflare client provider. Use `await Log.CloseAndFlushAsync()` for the static logger. Applications that require a persistent audit record should also write to a durable destination.

## Monitor delivery

Subscribe to the .NET meter `Soenneker.Serilog.Sinks.R2` with a metrics listener or an OpenTelemetry meter provider. Counters have no per-object or tenant tags.

| Counter | Meaning |
| --- | --- |
| `r2.objects.uploaded` | Objects acknowledged by Cloudflare |
| `r2.bytes.uploaded` | NDJSON bytes in acknowledged objects |
| `r2.upload.failures` | Failed attempts, including timeouts and invalid acknowledgements |
| `r2.events.oversized` | Individual events skipped because they exceeded the object limit |

Enable `Serilog.Debugging.SelfLog.Enable(Console.Error)` for Serilog delivery failures and oversized-event diagnostics. Keep the diagnostic writer independent of the logger. Alert on sustained upload failures and oversized events. A failure count includes retried attempts and cannot be used as a count of lost events.
