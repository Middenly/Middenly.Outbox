# Per-Topic Configuration

Configure per-destination behavior using the fluent `Topic()` API.

## Basic Usage

```csharp
builder.Services.AddOutbox(options =>
{
    options.BatchSize = 100;
    options.PollingInterval = TimeSpan.FromSeconds(5);
})
.UsePostgresStore(connectionString)
.UseKafkaProducer(kafka =>
{
    kafka.BootstrapServers = "localhost:9092";
})
.Topic("payment-events", t =>
{
    t.Ordered = true;      // FIFO delivery
    t.MaxAttempts = 10;    // more retries for critical topic
})
.Topic("analytics-events", t =>
{
    t.MaxAttempts = 2;     // less retries, fire-and-forget
})
.DefaultTopic(t =>
{
    t.MaxAttempts = 5;     // fallback for unmapped destinations
});
```

## TopicOptions Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Ordered` | `bool?` | `null` (false) | Sequential delivery (FIFO) |
| `MaxAttempts` | `int?` | `null` (uses global) | Per-topic max retry attempts |

## How Merging Works

Topic-specific options override `DefaultTopic` options:

```csharp
builder.Services.AddOutbox(options =>
{
    options.MaxAttempts = 5; // global default
})
.DefaultTopic(t =>
{
    t.MaxAttempts = 3;      // overrides global
})
.Topic("payment-events", t =>
{
    t.Ordered = true;       // topic-specific
    t.MaxAttempts = 10;     // overrides default topic
});
```

Result for `payment-events`:
- `Ordered = true` (from topic)
- `MaxAttempts = 10` (from topic, overrides default)

Result for any other topic:
- `Ordered = null` (default)
- `MaxAttempts = 3` (from default topic)

## Case Sensitivity

Topic names are **case-insensitive**:

```csharp
.Topic("Payment-Events", t => t.Ordered = true)
// matches "payment-events", "PAYMENT-EVENTS", etc.
```

## Ordered Delivery

When `Ordered = true`, messages within that destination are delivered **sequentially** (FIFO from PostgreSQL). This is slower but guarantees ordering.

When `Ordered = false` (default), messages are delivered **concurrently** via `Task.WhenAll`. The Kafka producer internally preserves order, so this is usually fine.

Use `Ordered = true` when:
- Payment processing (strict order matters)
- Audit logs (sequential integrity)
- State machines (state transitions must be ordered)
