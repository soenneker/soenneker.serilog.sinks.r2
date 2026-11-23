using System.Net.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Soenneker.Cloudflare.HttpClient.Abstract;
using Soenneker.Cloudflare.R2.Registrars;

namespace Soenneker.Serilog.Sinks.R2.Tests;

internal static class R2TestServices
{
    public static ServiceProvider Create(HttpClient http)
    {
        return new ServiceCollection()
            .AddSingleton<IConfiguration>(_ => new ConfigurationManager
            {
                ["Cloudflare:ApiKey"] = "test-token",
                ["Cloudflare:RequestResponseLogging"] = "true"
            })
            .AddSingleton<ICloudflareHttpClient>(new RecordingCloudflareHttpClient(http, "test-token"))
            .AddCloudflareR2UtilAsSingleton()
            .BuildServiceProvider();
    }
}
