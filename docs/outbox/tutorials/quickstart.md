# Quickstart

This tutorial walks you through setting up Middenly.Outbox in a real application.

## Prerequisites

- .NET 10.0 SDK
- PostgreSQL (local or Docker)
- Kafka (local or Docker)

### Docker Compose for Local Development

Create a `docker-compose.yml`:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: outbox_demo
      POSTGRES_USER: demo
      POSTGRES_PASSWORD: demo
    ports:
      - "5432:5432"

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    ports:
      - "9092:9092"
```

Start services:

```bash
docker-compose up -d
```

## Step 1: Create the Project

```bash
dotnet new webapi -n OutboxDemo
cd OutboxDemo
dotnet add package Middenly.Outbox
```

## Step 2: Define Your Models

```csharp
// Models/Order.cs
namespace OutboxDemo.Models;

public class Order
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string CustomerName { get; set; } = string.Empty;
    public decimal Total { get; set; }
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
}

public class OrderCreatedEvent
{
    public Guid OrderId { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public decimal Total { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}
```

## Step 3: Configure Services

```csharp
// Program.cs
using Middenly.Outbox.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Add Outbox + Kafka producer in one chain
builder.Services.AddOutbox(options =>
{
    options.BatchSize = 100;
    options.PollingInterval = TimeSpan.FromSeconds(2);
    options.MaxAttempts = 5;
    options.EnableDeadLetter = true;
})
.UsePostgresStore(builder.Configuration.GetConnectionString("Default")!)
.UseKafkaProducer(kafka =>
{
    kafka.BootstrapServers = "localhost:9092";
    kafka.Acks = Confluent.Kafka.Acks.All;
    kafka.EnableIdempotence = true;
});

var app = builder.Build();
```

## Step 4: Create the Order Service

```csharp
// Services/OrderService.cs
using Middenly.Outbox.Abstractions;
using OutboxDemo.Models;

namespace OutboxDemo.Services;

public class OrderService
{
    private readonly IOutbox _outbox;
    private readonly ILogger<OrderService> _logger;

    public OrderService(IOutbox outbox, ILogger<OrderService> logger)
    {
        _outbox = outbox;
        _logger = logger;
    }

    public async Task<Order> CreateOrderAsync(string customerName, decimal total)
    {
        var order = new Order
        {
            CustomerName = customerName,
            Total = total
        };

        // In a real app, you'd save to your database here
        // await _dbContext.Orders.AddAsync(order);
        // await _dbContext.SaveChangesAsync();

        // Publish event to outbox — serializer is resolved automatically
        await _outbox.PublishAsync("order-events", new OrderCreatedEvent
        {
            OrderId = order.Id,
            CustomerName = order.CustomerName,
            Total = order.Total,
            CreatedAt = order.CreatedAt
        });

        _logger.LogInformation("Order {OrderId} created, event published to outbox", order.Id);

        return order;
    }
}
```

## Step 5: Create the API Endpoint

```csharp
// Program.cs (continued)
using OutboxDemo.Services;

builder.Services.AddSingleton<OrderService>();

var app = builder.Build();

app.MapPost("/orders", async (
    CreateOrderRequest request,
    OrderService orderService) =>
{
    var order = await orderService.CreateOrderAsync(request.CustomerName, request.Total);
    return Results.Created($"/orders/{order.Id}", order);
});

app.MapGet("/orders/{id}", (Guid id) =>
{
    // In a real app, fetch from database
    return Results.Ok(new { Id = id, Status = "Created" });
});

app.Run();

record CreateOrderRequest(string CustomerName, decimal Total);
```

## Step 6: Run and Test

```bash
dotnet run
```

Create an order:

```bash
curl -X POST http://localhost:5000/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName": "John Doe", "total": 99.99}'
```

## Step 7: Verify in Kafka

Consume messages from the `order-events` topic:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --from-beginning
```

You should see the `OrderCreatedEvent` JSON message.

## What Happened

1. The API received the order request
2. `OrderService.CreateOrderAsync()` was called
3. The `OrderCreatedEvent` was serialized and stored in PostgreSQL via `IOutbox.PublishAsync()`
4. The `OutboxDispatcher` (running in the background) picked up the message
5. The message was delivered to Kafka via `IOutboxProducer`
6. The message was marked as `Completed` in PostgreSQL

If Kafka was temporarily unavailable, the message would remain in the `Pending` state and be retried on the next polling cycle.

## Advanced: Partition Key and Headers

```csharp
await _outbox.PublishAsync("order-events", new OrderCreatedEvent { ... }, opts =>
{
    opts.WithKey(order.Id.ToString());           // messages with same key → same partition
    opts.WithPartition(0);                        // or explicit partition
    opts.WithHeader("correlation-id", requestId); // custom headers
    opts.DeliverAfterDelay(TimeSpan.FromMinutes(5)); // delayed delivery
});
```

## Next Steps

- Learn about [Configuration](/guide/configuration) options
- Set up [Dead Letter Queue](/guide/dead-letter) monitoring
- Read about [Integration Testing](/tutorials/testing)
- Configure [Multiple Producers](/guide/multiple-producers) for different topics
