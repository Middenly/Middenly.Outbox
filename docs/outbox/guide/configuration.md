# Configuration

Middenly.Outbox provides two main configuration classes: `OutboxOptions` for the outbox behavior and `KafkaOutboxOptions` for the Kafka producer.

## OutboxOptions

Controls the outbox dispatcher behavior, retry policy, and database settings.

```csharp
builder.Services.AddOutbox(options =>
{
    options.BatchSize = 100;
    options.PollingInterval = TimeSpan.FromSeconds(5);
    options.MaxAttempts = 5;
    options.RetryDelay = TimeSpan.FromSeconds(30);
    options.EnableDeadLetter = true;
    options.CleanupInterval = TimeSpan.FromHours(1);
    options.MessageRetention = TimeSpan.FromDays(7);
    options.StuckMessageTimeout = TimeSpan.FromMinutes(5);
    options.RecoveryPollingInterval = TimeSpan.FromMinutes(1);
    options.TableName = "outbox_messages";
    options.SchemaName = "public";
});
```

### Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `BatchSize` | `int` | `100` | Maximum number of messages to read from the database per polling cycle |
| `PollingInterval` | `TimeSpan` | `5 seconds` | How often the dispatcher polls for new messages |
| `MaxAttempts` | `int` | `5` | Maximum delivery attempts before moving to dead letter queue |
| `RetryDelay` | `TimeSpan` | `30 seconds` | Minimum delay between retry attempts |
| `EnableDeadLetter` | `bool` | `true` | Whether to move permanently failed messages to the dead letter queue |
| `CleanupInterval` | `TimeSpan?` | `null` | How often to run cleanup (null = disabled) |
| `MessageRetention` | `TimeSpan` | `7 days` | How long to keep completed/dead-lettered messages before cleanup |
| `StuckMessageTimeout` | `TimeSpan` | `5 minutes` | How long an InProgress message can be stuck before recovery |
| `RecoveryPollingInterval` | `TimeSpan` | `1 minute` | How often to check for stuck InProgress messages |
| `TableName` | `string` | `"outbox_messages"` | Name of the outbox table in PostgreSQL |
| `SchemaName` | `string` | `"public"` | PostgreSQL schema name for the outbox table |

### Per-Topic Configuration

Use the fluent `Topic()` API to configure per-topic behavior:

```csharp
builder.Services.AddOutbox(options => { /* global options */ })
    .UsePostgresStore(connectionString)
    .UseKafkaProducer(kafka => { /* kafka options */ })
    .Topic("payment-events", t => t
        .Ordered()           // FIFO delivery
        .MaxAttempts(10))    // more retries for critical topic
    .Topic("analytics-events", t => t
        .Profile("high-throughput"))
    .DefaultTopic(t => t.MaxAttempts(3)); // fallback for unmapped topics
```

See [Per-Topic Configuration](/guide/topic-configuration) for details.

### Tuning Guide

**High Throughput Systems:**
```csharp
options.BatchSize = 500;
options.PollingInterval = TimeSpan.FromSeconds(1);
```

**Low Latency Systems:**
```csharp
options.PollingInterval = TimeSpan.FromMilliseconds(500);
options.BatchSize = 50;
```

**Reliable Delivery (more retries):**
```csharp
options.MaxAttempts = 10;
options.RetryDelay = TimeSpan.FromSeconds(10);
options.EnableDeadLetter = true;
```

## KafkaOutboxOptions

Controls the Confluent.Kafka producer configuration. Configured via `UseKafkaProducer()`:

```csharp
builder.Services.AddOutbox(options => { /* ... */ })
    .UsePostgresStore(connectionString)
    .UseKafkaProducer(kafka =>
    {
        kafka.BootstrapServers = "localhost:9092";
        kafka.Acks = Confluent.Kafka.Acks.All;
        kafka.EnableIdempotence = true;
        kafka.MaxInFlight = 5;
        kafka.TransactionalId = "my-service";
        kafka.MessageTimeout = TimeSpan.FromSeconds(30);
        kafka.ConfigureProducer = config =>
        {
            config.SocketTimeoutMs = 5000;
            config.RequestTimeoutMs = 10000;
        };
    });
```

### Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `BootstrapServers` | `string` | *(required)* | Kafka bootstrap servers (comma-separated) |
| `Acks` | `Acks` | `All` | Number of acknowledgments the producer requires |
| `EnableIdempotence` | `bool` | `true` | Enable idempotent producer for exactly-once semantics |
| `MaxInFlight` | `int` | `5` | Maximum number of unacknowledged requests per connection |
| `TransactionalId` | `string?` | `null` | Transactional ID for exactly-once semantics |
| `MessageTimeout` | `TimeSpan` | `30 seconds` | Local message timeout |
| `ConfigureProducer` | `Action<ProducerConfig>?` | `null` | Callback for additional producer configuration |

### Acks Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `Acks.None` | No acknowledgment | Maximum throughput, data loss possible |
| `Acks.Leader` | Leader acknowledgment | Balance of performance and safety |
| `Acks.All` | All in-sync replicas | Maximum durability (recommended) |

### Idempotent Producer

When `EnableIdempotence = true` (default), Kafka guarantees that messages are delivered exactly once to a particular topic partition. This requires:
- `Acks.All`
- `MaxInFlight` between 1 and 5
- Retries > 0

Middenly.Outbox sets these defaults automatically.

## Full Example

```csharp
builder.Services.AddOutbox(options =>
{
    options.BatchSize = 200;
    options.PollingInterval = TimeSpan.FromSeconds(2);
    options.MaxAttempts = 5;
    options.EnableDeadLetter = true;
    options.CleanupInterval = TimeSpan.FromHours(6);
    options.MessageRetention = TimeSpan.FromDays(3);
    options.StuckMessageTimeout = TimeSpan.FromMinutes(5);
    options.RecoveryPollingInterval = TimeSpan.FromMinutes(1);
    options.TableName = "my_outbox";
    options.SchemaName = "messaging";
})
.UsePostgresStore(builder.Configuration.GetConnectionString("Default")!)
.UseKafkaProducer(kafka =>
{
    kafka.BootstrapServers = "broker1:9092,broker2:9092,broker3:9092";
    kafka.Acks = Confluent.Kafka.Acks.All;
    kafka.EnableIdempotence = true;
    kafka.MaxInFlight = 5;
    kafka.MessageTimeout = TimeSpan.FromSeconds(60);
    kafka.ConfigureProducer = config =>
    {
        config.LingerMs = 10;
        config.BatchSize = 16384;
        config.CompressionType = Confluent.Kafka.CompressionType.Lz4;
    };
})
.Topic("payment-events", t => t.Ordered().MaxAttempts(10))
.DefaultTopic(t => t.MaxAttempts(3));
```
