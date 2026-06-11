# Middenly.Outbox

[![NuGet](https://img.shields.io/nuget/v/Middenly.Outbox.svg)](https://www.nuget.org/packages/Middenly.Outbox)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Transactional Outbox pattern implementation for Confluent.Kafka with PostgreSQL support.

## Overview

The Outbox pattern ensures reliable message delivery by storing outgoing messages in a local database table (the "outbox") within the same transaction as your business data. A separate background process then reads and dispatches these messages to Kafka.

This approach guarantees **at-least-once delivery** and avoids dual-write problems in microservice architectures.

### Key Features

- **Transactional Outbox** - Store messages in PostgreSQL alongside your business data
- **Confluent.Kafka Integration** - Native Kafka producer with full header and partition support
- **EF Core Integration** - Atomic SaveChanges with `UseEfCoreOutbox<T>()`
- **Per-Topic Configuration** - Fluent API for ordering, retries, producer profiles
- **Background Dispatcher** - Automatic polling and delivery with configurable intervals
- **Retry with Dead Letter** - Configurable retry policies with dead letter queue support
- **Delayed Delivery** - Schedule messages for future delivery
- **Parallel Delivery** - Per-topic concurrent dispatching with Kafka internal batching
- **Pessimistic Locking** - `FOR UPDATE SKIP LOCKED` for safe concurrent access
- **Stuck Message Recovery** - Automatic recovery of InProgress messages after crashes
- **Extensible Serialization** - Pluggable serializer interface (System.Text.Json included)

## Installation

```bash
dotnet add package Middenly.Outbox
```

### Package Manager

```
Install-Package Middenly.Outbox
```

## Quick Start

### 1. Register Services

```csharp
using Middenly.Outbox.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOutbox(options =>
{
    options.BatchSize = 100;
    options.PollingInterval = TimeSpan.FromSeconds(5);
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
```

### 2. Use in Your Service

```csharp
using Middenly.Outbox.Abstractions;

public class OrderService
{
    private readonly IOutbox _outbox;

    public OrderService(IOutbox outbox)
    {
        _outbox = outbox;
    }

    public async Task CreateOrderAsync(Order order)
    {
        // Save order to database...

        // Publish event — serializer is resolved automatically
        await _outbox.PublishAsync("order-events", new OrderCreatedEvent
        {
            OrderId = order.Id,
            Total = order.Total,
            CreatedAt = DateTimeOffset.UtcNow
        });
    }
}
```

### 3. Advanced Usage

```csharp
// Publish with key, partition, headers via fluent options
await _outbox.PublishAsync("order-events", new OrderCreatedEvent { ... }, opts =>
{
    opts.WithKey(order.Id.ToString());
    opts.WithPartition(0);
    opts.WithHeader("correlation-id", requestId);
    opts.DeliverAfterDelay(TimeSpan.FromMinutes(30));
});
```

## Requirements

- .NET 10.0+
- PostgreSQL 12+
- Kafka (Confluent.Kafka compatible)

## License

[MIT](LICENSE)
