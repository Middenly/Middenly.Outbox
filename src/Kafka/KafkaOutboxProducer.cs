using System.Text.Json;
using Confluent.Kafka;
using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Middenly.Outbox.Kafka;

public sealed class KafkaOutboxProducer : IOutboxProducer
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly ILogger<KafkaOutboxProducer> _logger;
    private bool _disposed;

    public KafkaOutboxProducer(
        IOptions<KafkaOutboxOptions> options,
        ILogger<KafkaOutboxProducer> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        var opts = options?.Value ?? throw new ArgumentNullException(nameof(options));

        var config = opts.CreateProducerConfig();
        _producer = new ProducerBuilder<byte[], byte[]>(config)

            .SetErrorHandler((_, error) =>
                _logger.LogError("Kafka producer error: {Reason} (Code: {Code})", error.Reason, error.Code))
            .SetLogHandler((_, logMessage) =>
                _logger.LogDebug("Kafka producer log: {Message}", logMessage.Message))
            .Build();
    }

    public async Task ProduceAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var kafkaMessage = new Message<byte[], byte[]>
        {
            Key = message.Key!,
            Value = message.Body
        };

        if (message.Headers is { Count: > 0 })
        {
            kafkaMessage.Headers = new Headers();
            foreach (var (key, value) in message.Headers)
            {
                kafkaMessage.Headers.Add(key, value);
            }
        }

        try
        {
            DeliveryResult<byte[], byte[]> result;

            if (message.Partition.HasValue)
            {
                var partition = new Partition(message.Partition.Value);
                var topicPartition = new TopicPartition(message.Destination, partition);
                result = await _producer.ProduceAsync(topicPartition, kafkaMessage, cancellationToken);
            }
            else
            {
                result = await _producer.ProduceAsync(message.Destination, kafkaMessage, cancellationToken);
            }

            _logger.LogDebug(
                "Message {MessageId} delivered to {TopicPartitionOffset}",
                message.Id,
                result.TopicPartitionOffset);
        }
        catch (ProduceException<byte[], byte[]> ex)
        {
            _logger.LogError(ex,
                "Failed to produce message {MessageId} to {Destination}: {Reason}",
                message.Id,
                message.Destination,
                ex.Error.Reason);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing Kafka producer");
        }

        await ValueTask.CompletedTask;
    }
}
