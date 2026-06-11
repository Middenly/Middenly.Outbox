# EF Core Integration

Middenly.Outbox.EntityFrameworkCore provides transactional outbox support with Entity Framework Core. Messages are stored in the **same database transaction** as your business data via `SaveChangesAsync()`.

## Installation

```bash
dotnet add package Middenly.Outbox.EntityFrameworkCore
```

## Setup

```csharp
using Middenly.Outbox.Extensions;
using Middenly.Outbox.EntityFrameworkCore.Extensions;

builder.Services.AddOutbox(options =>
{
    options.BatchSize = 100;
    options.PollingInterval = TimeSpan.FromSeconds(2);
    options.MaxAttempts = 5;
})
.UsePostgresStore(connectionString)
.UseKafkaProducer(kafka =>
{
    kafka.BootstrapServers = "localhost:9092";
})
.UseEfCoreOutbox<AppDbContext>();  // ← enables EF Core integration

builder.Services.AddDbContext<AppDbContext>((sp, options) =>
{
    options.UseNpgsql(connectionString);
    options.UseOutboxInterceptor(sp);  // ← registers the outbox interceptor
});
```

## How It Works

```
┌──────────────────┐     ┌─────────────────────┐     ┌───────────────────┐
│  Your Service    │     │ SaveChangesAsync()  │     │ Outbox Dispatcher │
│                  │     │                     │     │ (Background)      │
│ IOutbox ─────────┼────►│ 1. INSERT entity    │     │                   │
│ PublishAsync()   │     │ 2. INSERT outbox msg│────►│ 3. Deliver to     │
│ (queues message) │     │    (same transaction)│     │    Kafka          │
└──────────────────┘     └─────────────────────┘     └───────────────────┘
```

1. `IOutbox.PublishAsync()` queues the message in memory (scoped)
2. `SaveChangesAsync()` writes both the entity and the outbox message in **one transaction**
3. After commit, the background `OutboxDispatcher` picks up the message and delivers to Kafka

**If the transaction rolls back** — the outbox message is never stored. No ghost messages.

## Usage

```csharp
public class OrderService
{
    private readonly IOutbox _outbox;
    private readonly AppDbContext _context;

    public OrderService(IOutbox outbox, AppDbContext context)
    {
        _outbox = outbox;
        _context = context;
    }

    public async Task<Order> CreateOrderAsync(string customer, decimal total)
    {
        var order = new Order
        {
            CustomerName = customer,
            Total = total
        };

        _context.Orders.Add(order);

        // Queue outbox message — NOT stored in DB yet
        await _outbox.PublishAsync("order-events", new OrderCreated
        {
            OrderId = order.Id,
            CustomerName = customer,
            Total = total
        });

        // Commit both in one transaction
        await _context.SaveChangesAsync();

        return order;
    }
}
```

### With Partition Key and Headers

```csharp
await _outbox.PublishAsync("order-events", new OrderCreated { ... }, opts =>
{
    opts.WithKey(order.Id.ToString());
    opts.WithHeader("correlation-id", requestId);
});

await _context.SaveChangesAsync();
```

## Without EF Core

Without EF Core, `IOutbox.PublishAsync()` writes directly to PostgreSQL:

```
IOutbox.PublishAsync() → IOutboxStore.StoreAsync() → PostgreSQL
                                                       ↓
                                              OutboxDispatcher → Kafka
```

With EF Core, `IOutbox.PublishAsync()` queues in memory, then `SaveChangesAsync()` writes everything:

```
IOutbox.PublishAsync() → OutboxMessageCollector (memory)
                              ↓
SaveChangesAsync() → Entity INSERT + Outbox INSERT (one transaction)
                              ↓
                     OutboxDispatcher → Kafka
```

## Architecture

### OutboxMessageCollector

A per-scope message collector that holds pending messages until `SaveChangesAsync()` is called. Uses `AsyncLocal` for proper scope isolation.

### EfCoreOutbox

An `IOutbox` implementation that queues messages to the collector instead of writing to the database directly.

### OutboxSaveChangesInterceptor

An EF Core `SaveChangesInterceptor` that:
1. Drains the collector during `SavingChangesAsync()`
2. Inserts outbox messages into the same database transaction
3. After commit, notifies the `OutboxDispatcher`

## Configuration

All standard `OutboxOptions` apply:

```csharp
builder.Services.AddOutbox(options =>
{
    options.BatchSize = 200;
    options.PollingInterval = TimeSpan.FromSeconds(1);
    options.MaxAttempts = 10;
    options.EnableDeadLetter = true;
    options.StuckMessageTimeout = TimeSpan.FromMinutes(5);
})
.UsePostgresStore(connectionString)
.UseKafkaProducer(kafka => { /* ... */ })
.UseEfCoreOutbox<AppDbContext>();
```

## Multiple DbContexts

If you have multiple `DbContext` types, register the outbox for each:

```csharp
builder.Services.AddOutbox(options => { /* ... */ })
    .UsePostgresStore(connectionString)
    .UseKafkaProducer(kafka => { /* ... */ })
    .UseEfCoreOutbox<OrderDbContext>()
    .UseEfCoreOutbox<PaymentDbContext>();
```

Both contexts will share the same outbox table and dispatcher.

## Troubleshooting

### Messages not being delivered

1. Ensure `UseOutboxInterceptor(sp)` is called in `AddDbContext`
2. Ensure the `OutboxDispatcher` is running (registered via `AddOutbox()`)
3. Check that `SaveChangesAsync()` is called after `PublishAsync()`

### Messages delivered multiple times

This is expected — the outbox provides **at-least-once** delivery. Design your consumers to be idempotent.

### Transaction too long

If your business transaction takes a long time, the outbox message will be held in memory until `SaveChangesAsync()`. This is by design — the message is part of the transaction.
