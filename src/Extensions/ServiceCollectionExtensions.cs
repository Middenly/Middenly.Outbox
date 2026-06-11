using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Middenly.Outbox.Dispatcher;
using Middenly.Outbox.Implementation;
using Middenly.Outbox.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Middenly.Outbox.Extensions;

public static class ServiceCollectionExtensions
{
    public static OutboxBuilder AddOutbox(
        this IServiceCollection services,
        Action<OutboxOptions>? configure = null)
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.Configure<OutboxOptions>(_ => { });
        }

        services.TryAddSingleton<IOutboxSerializer, SystemTextJsonOutboxSerializer>();
        services.TryAddSingleton<OutboxDispatcher>();
        services.TryAddSingleton<IHostedService>(sp => sp.GetRequiredService<OutboxDispatcher>());
        services.TryAddSingleton<DefaultOutbox>();
        services.TryAddSingleton<IOutbox>(sp => sp.GetRequiredService<DefaultOutbox>());

        return new OutboxBuilder(services);
    }

    public static OutboxBuilder AddOutbox(
        this IServiceCollection services,
        string postgresConnectionString,
        Action<OutboxOptions>? configure = null)
    {
        var builder = services.AddOutbox(configure);
        builder.UsePostgresStore(postgresConnectionString);
        return builder;
    }
}
