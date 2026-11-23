using System;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Soenneker.Serilog.Sinks.R2;

internal sealed class R2Destination
{
    private readonly string _prefix;
    internal string AccountId { get; }
    internal string BucketName { get; }

    internal R2Destination(string accountId, string bucketName, string prefix)
    {
        ValidateSegment(accountId, nameof(accountId));
        ValidateSegment(bucketName, nameof(bucketName));
        AccountId = accountId;
        BucketName = bucketName;
        prefix = prefix.Trim('/');
        _prefix = prefix.Length == 0 ? string.Empty : prefix + "/";
        if (prefix.Length == 0)
            return;

        // Reserve a slash plus yyyy/MM/dd, a slash, the dashed GUID and the .ndjson suffix.
        if (Encoding.UTF8.GetByteCount(prefix) + 55 > 1024)
            throw new ArgumentException("The resulting R2 key exceeds 1,024 UTF-8 bytes.", nameof(prefix));

        foreach (string segment in prefix.Split('/'))
            ValidateSegment(segment, nameof(prefix));
    }

    internal string NewKey(DateTimeOffset firstTimestamp)
    {
        string date = firstTimestamp.UtcDateTime.ToString("yyyy'/'MM'/'dd", CultureInfo.InvariantCulture);
        return $"{_prefix}{date}/{Guid.NewGuid()}.ndjson";
    }

    private static void ValidateSegment(string value, string parameter)
    {
        if (string.IsNullOrWhiteSpace(value) || value is "." or ".." || value.Contains('/') || value.Contains('\\') || value.Any(char.IsControl))
            throw new ArgumentException("Expected a nonempty path segment without separators, dot traversal, or control characters.", parameter);
    }
}
