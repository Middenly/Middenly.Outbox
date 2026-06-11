using System.Text;
using Confluent.Kafka;
using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Middenly.Outbox.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Kafka;
using Xunit;

namespace Middenly.Outbox.Tests.Integration;

public class KafkaOutboxProducerTests : IAsyncLifetime
{
    private readonly KafkaContainer _kafka = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:7.6.0")
        .Build();

    private KafkaOutboxProducer _producer = null!;
    private string _bootstrapServers = null!;

    public async Task InitializeAsync()
    {
        await _kafka.StartAsync();
        _bootstrapServers = _kafka.GetBootstrapAddress();

        var options = Options.Create(new KafkaOutboxOptions
        {
            BootstrapServers = _bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeout = TimeSpan.FromSeconds(10)
        });

        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var logger = loggerFactory.CreateLogger<KafkaOutboxProducer>();

        _producer = new KafkaOutboxProducer(options, logger);
    }

    public async Task DisposeAsync()
    {
        await _producer.DisposeAsync();
        await _kafka.DisposeAsync();
    }

    [Fact]
    public async Task ProduceAsync_ShouldDeliverMessage()
    {
        // Arrange
        var topic = $"test-topic-{Guid.NewGuid():N}";
        var messageValue = Encoding.UTF8.GetBytes("test-message");
        var message = new OutboxMessage
        {
            Destination = topic,
            Body = messageValue
        };

        // Act & Assert - should not throw
        await _producer.ProduceAsync(message);

        // Verify message was produced by consuming it
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var result = consumer.Consume(TimeSpan.FromSeconds(10));
        result.Should().NotBeNull();
        result.Message.Value.Should().BeEquivalentTo(messageValue);
    }

    [Fact]
    public async Task ProduceAsync_WithKey_ShouldDeliverMessageWithKey()
    {
        // Arrange
        var topic = $"test-topic-{Guid.NewGuid():N}";
        var messageKey = Encoding.UTF8.GetBytes("test-key");
        var messageValue = Encoding.UTF8.GetBytes("test-message");
        var message = new OutboxMessage
        {
            Destination = topic,
            Key = messageKey,
            Body = messageValue
        };

        // Act
        await _producer.ProduceAsync(message);

        // Assert
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var result = consumer.Consume(TimeSpan.FromSeconds(10));
        result.Should().NotBeNull();
        result.Message.Key.Should().BeEquivalentTo(messageKey);
        result.Message.Value.Should().BeEquivalentTo(messageValue);
    }

    [Fact]
    public async Task ProduceAsync_WithHeaders_ShouldDeliverMessageWithHeaders()
    {
        // Arrange
        var topic = $"test-topic-{Guid.NewGuid():N}";
        var headers = new Dictionary<string, byte[]>
        {
            ["header1"] = Encoding.UTF8.GetBytes("value1"),
            ["header2"] = Encoding.UTF8.GetBytes("value2")
        };
        var message = new OutboxMessage
        {
            Destination = topic,
            Body = Encoding.UTF8.GetBytes("test-message"),
            Headers = headers
        };

        // Act
        await _producer.ProduceAsync(message);

        // Assert
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var result = consumer.Consume(TimeSpan.FromSeconds(10));
        result.Should().NotBeNull();
        result.Message.Headers.Should().NotBeNull();
        result.Message.Headers.Should().Contain(h => h.Key == "header1");
        result.Message.Headers.Should().Contain(h => h.Key == "header2");
    }

    [Fact]
    public async Task ProduceAsync_WithPartition_ShouldDeliverToSpecificPartition()
    {
        // Arrange
        var topic = $"test-topic-{Guid.NewGuid():N}";
        var message = new OutboxMessage
        {
            Destination = topic,
            Body = Encoding.UTF8.GetBytes("test-message"),
            Partition = 0
        };

        // Act
        await _producer.ProduceAsync(message);

        // Assert
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var result = consumer.Consume(TimeSpan.FromSeconds(10));
        result.Should().NotBeNull();
        result.Partition.Value.Should().Be(0);
    }

    [Fact]
    public async Task ProduceAsync_MultipleMessages_ShouldDeliverAll()
    {
        // Arrange
        var topic = $"test-topic-{Guid.NewGuid():N}";
        var messages = Enumerable.Range(0, 5)
            .Select(i => new OutboxMessage
            {
                Destination = topic,
                Body = Encoding.UTF8.GetBytes($"message-{i}")
            })
            .ToList();

        // Act
        foreach (var message in messages)
        {
            await _producer.ProduceAsync(message);
        }

        // Assert
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var consumedMessages = new List<ConsumeResult<byte[], byte[]>>();
        for (int i = 0; i < 5; i++)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(10));
            if (result != null) consumedMessages.Add(result);
        }

        consumedMessages.Should().HaveCount(5);
    }
}
