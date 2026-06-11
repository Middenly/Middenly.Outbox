# Multiple Producers

Middenly.Outbox uses a single `IOutboxProducer` registered in DI. For most services, one producer is enough.

## Single Producer (Default)

```csharp
builder.Services.AddOutbox(options => { ... })
    .UsePostgresStore(connectionString)
    .UseKafkaProducer(kafka =>
    {
        kafka.BootstrapServers = "localhost:9092";
        kafka.Acks = Acks.All;
    });
```

All messages go through the same producer regardless of destination.

## Custom Producer

If you need a completely custom producer (e.g., for a different Kafka cluster), implement `IOutboxProducer`:

```csharp
public class CustomKafkaProducer : IOutboxProducer
{
    public async Task ProduceAsync(OutboxMessage message, CancellationToken ct)
    {
        // Your custom logic
    }

    public async ValueTask DisposeAsync() { /* cleanup */ }
}

// Register
builder.Services.AddSingleton<IOutboxProducer, CustomKafkaProducer>();
```

Or pass an existing instance:

```csharp
builder.Services.AddOutbox()
    .UsePostgresStore(connectionString)
    .UseKafkaProducer(); // registers KafkaOutboxProducer

// Override with custom
builder.Services.AddSingleton<IOutboxProducer>(myCustomProducer);
```
