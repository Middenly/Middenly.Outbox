using Middenly.Outbox.Configuration;
using FluentAssertions;
using Xunit;

namespace Middenly.Outbox.Tests.Unit;

public class OutboxOptionsTests
{
    [Fact]
    public void OutboxOptions_ShouldHaveDefaultValues()
    {
        // Arrange & Act
        var options = new OutboxOptions();

        // Assert
        options.BatchSize.Should().Be(100);
        options.PollingInterval.Should().Be(TimeSpan.FromSeconds(5));
        options.MaxAttempts.Should().Be(5);
        options.RetryDelay.Should().Be(TimeSpan.FromSeconds(30));
        options.EnableDeadLetter.Should().BeTrue();
        options.CleanupInterval.Should().BeNull();
        options.MessageRetention.Should().Be(TimeSpan.FromDays(7));
        options.TableName.Should().Be("outbox_messages");
        options.SchemaName.Should().Be("public");
    }

    [Fact]
    public void OutboxOptions_ShouldAllowCustomization()
    {
        // Arrange & Act
        var options = new OutboxOptions
        {
            BatchSize = 50,
            PollingInterval = TimeSpan.FromSeconds(10),
            MaxAttempts = 3,
            RetryDelay = TimeSpan.FromMinutes(1),
            EnableDeadLetter = false,
            CleanupInterval = TimeSpan.FromHours(1),
            MessageRetention = TimeSpan.FromDays(30),
            TableName = "custom_outbox",
            SchemaName = "messaging"
        };

        // Assert
        options.BatchSize.Should().Be(50);
        options.PollingInterval.Should().Be(TimeSpan.FromSeconds(10));
        options.MaxAttempts.Should().Be(3);
        options.RetryDelay.Should().Be(TimeSpan.FromMinutes(1));
        options.EnableDeadLetter.Should().BeFalse();
        options.CleanupInterval.Should().Be(TimeSpan.FromHours(1));
        options.MessageRetention.Should().Be(TimeSpan.FromDays(30));
        options.TableName.Should().Be("custom_outbox");
        options.SchemaName.Should().Be("messaging");
    }
}

public class KafkaOutboxOptionsTests
{
    [Fact]
    public void KafkaOutboxOptions_ShouldHaveDefaultValues()
    {
        // Arrange & Act
        var options = new KafkaOutboxOptions
        {
            BootstrapServers = "localhost:9092"
        };

        // Assert
        options.BootstrapServers.Should().Be("localhost:9092");
        options.Acks.Should().Be(Confluent.Kafka.Acks.All);
        options.EnableIdempotence.Should().BeTrue();
        options.MaxInFlight.Should().Be(5);
        options.TransactionalId.Should().BeNull();
        options.MessageTimeout.Should().Be(TimeSpan.FromSeconds(30));
        options.ConfigureProducer.Should().BeNull();
    }

    [Fact]
    public void KafkaOutboxOptions_ShouldAllowCustomization()
    {
        // Arrange & Act
        var options = new KafkaOutboxOptions
        {
            BootstrapServers = "broker1:9092,broker2:9092",
            Acks = Confluent.Kafka.Acks.Leader,
            EnableIdempotence = false,
            MaxInFlight = 10,
            TransactionalId = "my-transaction",
            MessageTimeout = TimeSpan.FromSeconds(60)
        };

        // Assert
        options.BootstrapServers.Should().Be("broker1:9092,broker2:9092");
        options.Acks.Should().Be(Confluent.Kafka.Acks.Leader);
        options.EnableIdempotence.Should().BeFalse();
        options.MaxInFlight.Should().Be(10);
        options.TransactionalId.Should().Be("my-transaction");
        options.MessageTimeout.Should().Be(TimeSpan.FromSeconds(60));
    }

    [Fact]
    public void CreateProducerConfig_ShouldReturnCorrectConfig()
    {
        // Arrange
        var options = new KafkaOutboxOptions
        {
            BootstrapServers = "localhost:9092",
            Acks = Confluent.Kafka.Acks.All,
            EnableIdempotence = true,
            MaxInFlight = 5,
            MessageTimeout = TimeSpan.FromSeconds(30)
        };

        // Act
        var config = options.CreateProducerConfig();

        // Assert
        config.BootstrapServers.Should().Be("localhost:9092");
        config.Acks.Should().Be(Confluent.Kafka.Acks.All);
        config.EnableIdempotence.Should().BeTrue();
        config.MaxInFlight.Should().Be(5);
        config.MessageTimeoutMs.Should().Be(30000);
    }

    [Fact]
    public void CreateProducerConfig_WithTransactionalId_ShouldSetTransactionalId()
    {
        // Arrange
        var options = new KafkaOutboxOptions
        {
            BootstrapServers = "localhost:9092",
            TransactionalId = "my-transaction"
        };

        // Act
        var config = options.CreateProducerConfig();

        // Assert
        config.TransactionalId.Should().Be("my-transaction");
    }

    [Fact]
    public void CreateProducerConfig_WithConfigureProducer_ShouldApplyConfiguration()
    {
        // Arrange
        var options = new KafkaOutboxOptions
        {
            BootstrapServers = "localhost:9092",
            ConfigureProducer = config =>
            {
                config.SocketTimeoutMs = 5000;
                config.RequestTimeoutMs = 10000;
            }
        };

        // Act
        var config = options.CreateProducerConfig();

        // Assert
        config.SocketTimeoutMs.Should().Be(5000);
        config.RequestTimeoutMs.Should().Be(10000);
    }
}
