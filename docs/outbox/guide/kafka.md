# Kafka Producer

Middenly.Outbox uses [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) as the Kafka producer.

## Setup

```csharp
builder.Services.AddOutbox(options => { /* ... */ })
    .UsePostgresStore(connectionString)
    .UseKafkaProducer(kafka =>
    {
        kafka.BootstrapServers = "localhost:9092";
        kafka.Acks = Confluent.Kafka.Acks.All;
        kafka.EnableIdempotence = true;
    });
```

## Message Features

### Simple Message

```csharp
await _outbox.PublishAsync("my-topic", Encoding.UTF8.GetBytes("Hello, Kafka!"));
```

### Typed Message

```csharp
await _outbox.PublishAsync("order-events", new OrderCreated
{
    OrderId = order.Id,
    Total = order.Total
});
```

### Message with Key

Keys are used for partition affinity — messages with the same key go to the same partition:

```csharp
await _outbox.PublishAsync("orders", message, opts =>
{
    opts.WithKey("order-123");
});
```

### Message with Headers

Headers carry metadata alongside the message:

```csharp
await _outbox.PublishAsync("orders", message, opts =>
{
    opts.WithHeader("correlation-id", Guid.NewGuid().ToString());
    opts.WithHeader("source", "order-service");
    opts.WithHeader("timestamp", DateTimeOffset.UtcNow.ToString("O"));
});
```

### Message to Specific Partition

```csharp
await _outbox.PublishAsync("orders", message, opts =>
{
    opts.WithPartition(0);
});
```

### Delayed Delivery

Schedule a message for future delivery:

```csharp
await _outbox.PublishAsync("orders", message, opts =>
{
    opts.DeliverAfterDelay(TimeSpan.FromMinutes(30));
});
```

The message will remain in the `Pending` state in PostgreSQL until the `deliver_after` timestamp is reached.

## Producer Configuration

### High Reliability

```csharp
kafka.Acks = Confluent.Kafka.Acks.All;
kafka.EnableIdempotence = true;
kafka.MaxInFlight = 5;
```

### High Throughput

The dispatcher fires all `ProduceAsync` calls in a batch concurrently. The Confluent.Kafka producer handles backpressure automatically — when the internal buffer (`buffer.memory`) is full, `ProduceAsync` blocks until space is available.

To tune throughput, configure the Kafka producer directly:

```csharp
builder.Services.AddOutbox(options =>
{
    options.BatchSize = 500;
    options.PollingInterval = TimeSpan.FromSeconds(1);
})
.UsePostgresStore(connectionString)
.UseKafkaProducer(kafka =>
{
    kafka.BootstrapServers = "localhost:9092";
    kafka.ConfigureProducer = config =>
    {
        config.LingerMs = 10;           // wait 10ms to fill a batch
        config.BatchSize = 65536;       // 64KB per batch
        config.CompressionType = Confluent.Kafka.CompressionType.Lz4;
        config.BufferMemory = 67108864; // 64MB buffer
        config.MaxInFlight = 5;         // max in-flight requests per connection
    };
});
```

No additional concurrency configuration needed — the Kafka producer is the natural backpressure mechanism.

### Low Latency

```csharp
kafka.ConfigureProducer = config =>
{
    config.LingerMs = 0;
    config.BatchSize = 1;
};
kafka.Acks = Confluent.Kafka.Acks.Leader;
```

### Multiple Brokers

```csharp
kafka.BootstrapServers = "broker1:9092,broker2:9092,broker3:9092";
```

### Custom Producer Configuration

Use the `ConfigureProducer` callback for any additional Kafka producer settings:

```csharp
kafka.ConfigureProducer = config =>
{
    config.SocketTimeoutMs = 5000;
    config.RequestTimeoutMs = 10000;
    config.MessageTimeoutMs = 60000;
    config.RetryBackoffMs = 100;
    config.MessageSendMaxRetries = 3;
};
```

## Error Handling

When the Kafka producer fails to deliver a message, the outbox dispatcher:

1. Catches the `ProduceException`
2. Marks the message as `Failed` with the error description
3. Increments the `Attempts` counter
4. On the next polling cycle, retries the message

After `MaxAttempts` failures, the message is moved to the dead letter queue (if enabled).

## Idempotent Producer

When `EnableIdempotence = true` (default), Kafka guarantees that messages are written to the log exactly once per partition. This requires:

- `Acks = All`
- `MaxInFlight` between 1 and 5

Middenly.Outbox validates and sets these defaults automatically.

::: warning
Idempotent producer only prevents duplicates at the Kafka broker level. Due to the at-least-once nature of the outbox pattern, messages may still be delivered more than once if the dispatcher crashes between delivery and marking the message as complete.
:::
