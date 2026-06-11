using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Middenly.Outbox.Kafka;
using Middenly.Outbox.Postgres;
using Middenly.Outbox.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Middenly.Outbox.Extensions;

public sealed class OutboxBuilder
{
    public IServiceCollection Services { get; }

    internal OutboxBuilder(IServiceCollection services)
    {
        Services = services;
    }

    public OutboxBuilder UsePostgresStore(string connectionString)
    {
        Services.AddSingleton(connectionString);
        Services.TryAddSingleton<IOutboxStore, PostgresOutboxStore>();
        return this;
    }

    public OutboxBuilder UseKafkaProducer(Action<KafkaOutboxOptions>? configure = null)
    {
        if (configure is not null)
        {
            Services.Configure(configure);
        }
        else
        {
            Services.Configure<KafkaOutboxOptions>(_ => { });
        }

        Services.TryAddSingleton<IOutboxProducer, KafkaOutboxProducer>();
        return this;
    }

    public OutboxBuilder UseSerializer<T>() where T : class, IOutboxSerializer
    {
        Services.AddSingleton<IOutboxSerializer, T>();
        return this;
    }

    public OutboxBuilder Topic(string destination, Action<TopicOptions> configure)
    {
        var options = new TopicOptions();
        configure(options);

        Services.PostConfigure<OutboxOptions>(o =>
        {
            o.Topics[destination] = options;
        });

        return this;
    }

    public OutboxBuilder DefaultTopic(Action<TopicOptions> configure)
    {
        var options = new TopicOptions();
        configure(options);

        Services.PostConfigure<OutboxOptions>(o =>
        {
            o.DefaultTopic = options;
        });

        return this;
    }
}
