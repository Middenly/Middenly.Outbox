using System.Text;
using Confluent.Kafka;
using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Middenly.Outbox.Dispatcher;
using Middenly.Outbox.Extensions;
using Middenly.Outbox.Kafka;
using Middenly.Outbox.Postgres;
using Middenly.Outbox.Serialization;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Xunit;

namespace Middenly.Outbox.Tests.Integration;

public class EndToEndTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("outbox_test")
        .WithUsername("test")
        .WithPassword("test")
        .Build();

    private readonly KafkaContainer _kafka = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:7.6.0")
        .Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await _kafka.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _kafka.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task FullFlow_ShouldDeliverMessageToKafka()
    {
        // Arrange
        var topic = $"test-topic-{Guid.NewGuid():N}";
        var connectionString = _postgres.GetConnectionString();
        var bootstrapServers = _kafka.GetBootstrapAddress();

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());

        services.AddOutbox(options =>
        {
            options.BatchSize = 10;
            options.PollingInterval = TimeSpan.FromSeconds(1);
            options.MaxAttempts = 3;
        })
        .UsePostgresStore(connectionString)
        .UseKafkaProducer(kafka =>
        {
            kafka.BootstrapServers = bootstrapServers;
            kafka.Acks = Acks.All;
            kafka.EnableIdempotence = true;
        });

        var serviceProvider = services.BuildServiceProvider();

        // Initialize the store
        var store = serviceProvider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();

        // Start the dispatcher
        var hostedServices = serviceProvider.GetServices<IHostedService>();
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StartAsync(CancellationToken.None);
        }

        // Act - publish a message to the outbox
        var outbox = serviceProvider.GetRequiredService<IOutbox>();
        var messageValue = Encoding.UTF8.GetBytes("test-message-content");
        await outbox.PublishAsync(topic, messageValue);

        // Wait for the dispatcher to process
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert - consume from Kafka to verify message was delivered
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var result = consumer.Consume(TimeSpan.FromSeconds(10));
        result.Should().NotBeNull();
        result.Message.Value.Should().BeEquivalentTo(messageValue);

        // Cleanup
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
        await serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task FullFlow_WithKeyAndHeaders_ShouldDeliverCompleteMessage()
    {
        // Arrange
        var topic = $"test-topic-{Guid.NewGuid():N}";
        var connectionString = _postgres.GetConnectionString();
        var bootstrapServers = _kafka.GetBootstrapAddress();

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());

        services.AddOutbox(options =>
        {
            options.BatchSize = 10;
            options.PollingInterval = TimeSpan.FromSeconds(1);
            options.MaxAttempts = 3;
        })
        .UsePostgresStore(connectionString)
        .UseKafkaProducer(kafka =>
        {
            kafka.BootstrapServers = bootstrapServers;
            kafka.Acks = Acks.All;
            kafka.EnableIdempotence = true;
        });

        var serviceProvider = services.BuildServiceProvider();

        var store = serviceProvider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();

        var hostedServices = serviceProvider.GetServices<IHostedService>();
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StartAsync(CancellationToken.None);
        }

        // Act
        var outbox = serviceProvider.GetRequiredService<IOutbox>();
        var key = Encoding.UTF8.GetBytes("message-key");
        var headers = new Dictionary<string, byte[]>
        {
            ["correlation-id"] = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
            ["timestamp"] = Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O"))
        };
        var messageValue = Encoding.UTF8.GetBytes("test-message-with-extras");

        await outbox.PublishAsync(topic, messageValue, options =>
        {
            options.WithKey(key);
            foreach (var header in headers)
                options.WithHeader(header.Key, header.Value);
        });

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var result = consumer.Consume(TimeSpan.FromSeconds(10));
        result.Should().NotBeNull();
        result.Message.Key.Should().BeEquivalentTo(key);
        result.Message.Value.Should().BeEquivalentTo(messageValue);
        result.Message.Headers.Should().NotBeNull();
        result.Message.Headers.Should().Contain(h => h.Key == "correlation-id");
        result.Message.Headers.Should().Contain(h => h.Key == "timestamp");

        // Cleanup
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
        await serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task FullFlow_MultipleMessages_ShouldDeliverAll()
    {
        // Arrange
        var topic = $"test-topic-{Guid.NewGuid():N}";
        var connectionString = _postgres.GetConnectionString();
        var bootstrapServers = _kafka.GetBootstrapAddress();

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());

        services.AddOutbox(options =>
        {
            options.BatchSize = 10;
            options.PollingInterval = TimeSpan.FromSeconds(1);
            options.MaxAttempts = 3;
        })
        .UsePostgresStore(connectionString)
        .UseKafkaProducer(kafka =>
        {
            kafka.BootstrapServers = bootstrapServers;
            kafka.Acks = Acks.All;
            kafka.EnableIdempotence = true;
        });

        var serviceProvider = services.BuildServiceProvider();

        var store = serviceProvider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();

        var hostedServices = serviceProvider.GetServices<IHostedService>();
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StartAsync(CancellationToken.None);
        }

        // Act - publish multiple messages
        var outbox = serviceProvider.GetRequiredService<IOutbox>();
        var messageCount = 5;
        var sentMessages = new List<byte[]>();

        for (int i = 0; i < messageCount; i++)
        {
            var value = Encoding.UTF8.GetBytes($"message-{i}");
            sentMessages.Add(value);
            await outbox.PublishAsync(topic, value);
        }

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - consume all messages from Kafka
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var consumedMessages = new List<byte[]>();
        for (int i = 0; i < messageCount; i++)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(10));
            if (result != null)
            {
                consumedMessages.Add(result.Message.Value);
            }
        }

        consumedMessages.Should().HaveCount(messageCount);
        consumedMessages.Should().BeEquivalentTo(sentMessages);

        // Cleanup
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
        await serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task FullFlow_WithSerializer_ShouldSerializeAndDeliver()
    {
        // Arrange
        var topic = $"test-topic-{Guid.NewGuid():N}";
        var connectionString = _postgres.GetConnectionString();
        var bootstrapServers = _kafka.GetBootstrapAddress();

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());

        services.AddOutbox(options =>
        {
            options.BatchSize = 10;
            options.PollingInterval = TimeSpan.FromSeconds(1);
            options.MaxAttempts = 3;
        })
        .UsePostgresStore(connectionString)
        .UseKafkaProducer(kafka =>
        {
            kafka.BootstrapServers = bootstrapServers;
            kafka.Acks = Acks.All;
            kafka.EnableIdempotence = true;
        })
        .UseSerializer<SystemTextJsonOutboxSerializer>();

        var serviceProvider = services.BuildServiceProvider();

        var store = serviceProvider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();

        var hostedServices = serviceProvider.GetServices<IHostedService>();
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StartAsync(CancellationToken.None);
        }

        // Act - publish a typed message
        var outbox = serviceProvider.GetRequiredService<IOutbox>();
        var serializer = serviceProvider.GetRequiredService<IOutboxSerializer>();

        var testMessage = new TestMessage
        {
            Id = Guid.NewGuid(),
            Name = "Test Order",
            Amount = 42.50m,
            CreatedAt = DateTimeOffset.UtcNow
        };

        await outbox.PublishAsync(topic, testMessage);

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert - consume and deserialize from Kafka
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var result = consumer.Consume(TimeSpan.FromSeconds(10));
        result.Should().NotBeNull();

        var deserializedMessage = serializer.Deserialize<TestMessage>(result.Message.Value);
        deserializedMessage.Should().NotBeNull();
        deserializedMessage!.Id.Should().Be(testMessage.Id);
        deserializedMessage.Name.Should().Be(testMessage.Name);
        deserializedMessage.Amount.Should().Be(testMessage.Amount);

        // Cleanup
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
        await serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task FullFlow_ParallelDelivery_ShouldDeliverAllMessages()
    {
        // Arrange
        var topic = $"test-topic-{Guid.NewGuid():N}";
        var connectionString = _postgres.GetConnectionString();
        var bootstrapServers = _kafka.GetBootstrapAddress();
        var messageCount = 200;

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());

        services.AddOutbox(options =>
        {
            options.BatchSize = 100;
            options.PollingInterval = TimeSpan.FromSeconds(1);
            options.MaxAttempts = 3;
        })
        .UsePostgresStore(connectionString)
        .UseKafkaProducer(kafka =>
        {
            kafka.BootstrapServers = bootstrapServers;
            kafka.Acks = Acks.All;
            kafka.EnableIdempotence = true;
            kafka.ConfigureProducer = config =>
            {
                config.LingerMs = 5;
                config.BatchSize = 65536;
            };
        });

        var serviceProvider = services.BuildServiceProvider();

        var store = serviceProvider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();

        var hostedServices = serviceProvider.GetServices<IHostedService>();
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StartAsync(CancellationToken.None);
        }

        // Act - store all messages first
        var outbox = serviceProvider.GetRequiredService<IOutbox>();
        var sentMessages = new List<byte[]>();

        for (int i = 0; i < messageCount; i++)
        {
            var value = Encoding.UTF8.GetBytes($"parallel-message-{i}");
            sentMessages.Add(value);
            await outbox.PublishAsync(topic, value);
        }

        // Wait for parallel delivery
        await Task.Delay(TimeSpan.FromSeconds(15));

        // Assert - consume all from Kafka
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            SessionTimeoutMs = 30000
        }).Build();

        consumer.Subscribe(topic);

        var consumedMessages = new List<byte[]>();
        var deadline = DateTime.UtcNow.AddSeconds(30);

        while (consumedMessages.Count < messageCount && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(2));
            if (result != null)
            {
                consumedMessages.Add(result.Message.Value);
            }
        }

        consumedMessages.Should().HaveCount(messageCount);

        // Cleanup
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
        await serviceProvider.DisposeAsync();
    }

    private class TestMessage
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }
}
