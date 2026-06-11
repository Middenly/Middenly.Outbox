# Integration Testing

Middenly.Outbox includes comprehensive integration tests using [Testcontainers](https://dotnet.testcontainers.org/) that spin up real PostgreSQL and Kafka instances in Docker.

## Running Tests

```bash
# Run all tests
dotnet test

# Run only unit tests
dotnet test --filter "FullyQualifiedName~Unit"

# Run only integration tests
dotnet test --filter "FullyQualifiedName~Integration"

# Run specific test class
dotnet test --filter "FullyQualifiedName~PostgresOutboxStoreTests"
```

## Prerequisites

- Docker Desktop (or compatible Docker runtime)
- .NET 10.0 SDK

Tests automatically start PostgreSQL and Kafka containers using Testcontainers. No manual setup required.

## Test Structure

```
tests/Middenly.Outbox.Tests/
├── Unit/
│   ├── OutboxMessageTests.cs       # Message model tests
│   └── ConfigurationTests.cs       # Options/configuration tests
└── Integration/
    ├── PostgresOutboxStoreTests.cs  # PostgreSQL store tests
    ├── KafkaOutboxProducerTests.cs  # Kafka producer tests
    └── EndToEndTests.cs            # Full flow tests
```

## Writing Your Own Tests

### Testing with Mocked IOutbox

For unit tests in your application, mock the `IOutbox` interface:

```csharp
using Moq;
using Middenly.Outbox.Abstractions;

public class OrderServiceTests
{
    [Fact]
    public async Task CreateOrder_ShouldPublishEvent()
    {
        // Arrange
        var outboxMock = new Mock<IOutbox>();
        var loggerMock = new Mock<ILogger<OrderService>>();

        var service = new OrderService(outboxMock.Object, loggerMock.Object);

        // Act
        await service.CreateOrderAsync("John", 99.99m);

        // Assert — verify PublishAsync was called with expected destination and message
        outboxMock.Verify(x => x.PublishAsync(
            "order-events",
            It.IsAny<OrderCreatedEvent>(),
            It.IsAny<Action<PublishOptions>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }
}
```

### Integration Testing with Testcontainers

For full integration tests in your application:

```csharp
using Testcontainers.PostgreSql;
using Testcontainers.Kafka;
using Middenly.Outbox.Extensions;

public class MyIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
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
    public async Task FullFlow_ShouldWork()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddOutbox(options =>
        {
            options.PollingInterval = TimeSpan.FromSeconds(1);
        })
        .UsePostgresStore(_postgres.GetConnectionString())
        .UseKafkaProducer(kafka =>
        {
            kafka.BootstrapServers = _kafka.GetBootstrapAddress();
        });

        var provider = services.BuildServiceProvider();

        // Initialize store
        var store = provider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();

        // Start dispatcher
        var dispatcher = provider.GetServices<IHostedService>();
        foreach (var svc in dispatcher) await svc.StartAsync(CancellationToken.None);

        // Act
        var outbox = provider.GetRequiredService<IOutbox>();
        await outbox.PublishAsync("test-topic", Encoding.UTF8.GetBytes("test"));

        // Wait for delivery
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert - consume from Kafka
        // ...

        // Cleanup
        foreach (var svc in dispatcher) await svc.StopAsync(CancellationToken.None);
    }
}
```

## Test Coverage

The test suite covers:

| Area | Tests | Description |
|------|-------|-------------|
| **PostgresOutboxStore** | 16 | CRUD operations, concurrent access, cleanup |
| **KafkaOutboxProducer** | 5 | Message delivery with keys, headers, partitions |
| **End-to-End** | 5 | Full flow from outbox to Kafka |
| **EF Core** | 4 | Transactional outbox with SaveChanges |
| **Unit** | 30 | Message model, configuration options |

### Key Test Scenarios

- Store and retrieve messages
- Message ordering (FIFO by creation time)
- Batch size limits
- In-progress locking
- Mark as completed/failed
- Dead letter queue
- Delayed delivery
- Concurrent access from multiple instances
- Headers with binary values
- Specific partition targeting
- Serialization round-trip
- Transactional outbox with EF Core SaveChanges
