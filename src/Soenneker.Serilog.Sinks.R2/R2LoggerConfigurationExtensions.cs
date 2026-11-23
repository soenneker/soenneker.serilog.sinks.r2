using System;
using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Soenneker.Cloudflare.R2.Abstract;

namespace Soenneker.Serilog.Sinks.R2;

/// <summary>Configures structured log archival in Cloudflare R2.</summary>
public static class R2LoggerConfigurationExtensions
{
    /// <summary>Uploads UTF-8 newline-delimited JSON to an existing R2 bucket using Serilog's background batching.</summary>
    /// <param name="writeTo">The logger's sink configuration.</param>
    /// <param name="getR2">Resolves the application-owned Cloudflare R2 utility when a batch needs uploading.</param>
    /// <param name="accountId">Cloudflare account that owns the bucket.</param>
    /// <param name="bucketName">Destination bucket name.</param>
    /// <param name="prefix">Optional object key prefix. An empty string writes directly under the UTC date partition.</param>
    /// <param name="batchingOptions">Serilog batch size, queue, buffering and retry settings.</param>
    /// <param name="restrictedToMinimumLevel">Minimum event level for this sink.</param>
    /// <param name="levelSwitch">Optional dynamic minimum level switch.</param>
    /// <param name="maxObjectBytes">Maximum UTF-8 bytes per object, including line delimiters. Oversized individual events are reported and skipped.</param>
    /// <param name="uploadTimeout">Deadline per upload attempt, including response processing. Defaults to 30 seconds.</param>
    public static LoggerConfiguration R2(this LoggerSinkConfiguration writeTo, Func<ICloudflareR2Util> getR2,
        string accountId, string bucketName, string prefix = "logs", BatchingOptions? batchingOptions = null,
        LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum, LoggingLevelSwitch? levelSwitch = null,
        int maxObjectBytes = 4 * 1024 * 1024, TimeSpan? uploadTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(writeTo);
        ArgumentNullException.ThrowIfNull(getR2);
        ArgumentException.ThrowIfNullOrWhiteSpace(accountId);
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentNullException.ThrowIfNull(prefix);

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxObjectBytes);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxObjectBytes, 300_000_000);
        TimeSpan timeout = uploadTimeout ?? TimeSpan.FromSeconds(30);
        if (timeout < TimeSpan.FromMilliseconds(1) || timeout.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(nameof(uploadTimeout));

        var destination = new R2Destination(accountId, bucketName, prefix);
        var sink = new R2Sink(getR2, destination, maxObjectBytes, timeout);
        batchingOptions ??= new BatchingOptions { EagerlyEmitFirstEvent = false };

        // Transport diagnostics still reach sibling sinks. Re-enqueuing them here would generate endless uploads.
        return writeTo.Conditional(_ => !R2Sink.UploadInProgress, target =>
            target.Sink(sink, batchingOptions, restrictedToMinimumLevel, levelSwitch));
    }
}
