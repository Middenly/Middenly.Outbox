using Confluent.Kafka;

namespace Middenly.Outbox.Configuration;

public sealed class KafkaOutboxOptions
{
    public required string BootstrapServers { get; set; }
    public Acks Acks { get; set; } = Acks.All;
    public bool EnableIdempotence { get; set; } = true;
    public int MaxInFlight { get; set; } = 5;
    public string? TransactionalId { get; set; }
    public TimeSpan MessageTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public Action<ProducerConfig>? ConfigureProducer { get; set; }

    public ProducerConfig CreateProducerConfig()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks,
            EnableIdempotence = EnableIdempotence,
            MaxInFlight = MaxInFlight,
            MessageTimeoutMs = (int)MessageTimeout.TotalMilliseconds
        };

        if (!string.IsNullOrEmpty(TransactionalId))
        {
            config.TransactionalId = TransactionalId;
        }

        ConfigureProducer?.Invoke(config);
        return config;
    }
}
