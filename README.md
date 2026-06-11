# Middenly.Outbox

[![NuGet](https://img.shields.io/nuget/v/Middenly.Outbox.svg)](https://www.nuget.org/packages/Middenly.Outbox)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Outbox pattern implementation for reliable messaging in distributed systems.

## Overview

The Outbox pattern ensures reliable message delivery by storing outgoing messages in a local database table (the "outbox") within the same transaction as your business data. A separate process then reads and dispatches these messages to the message broker.

This approach guarantees **at-least-once delivery** and avoids dual-write problems in microservice architectures.

## Installation

```bash
dotnet add package Middenly.Outbox
```

### Package Manager

```
Install-Package Middenly.Outbox
```

## Quick Start

```csharp
// Registration
builder.Services.AddOutbox(options =>
{
    options.UseDatabase(builder.Configuration.GetConnectionString("Default"));
    options.UseRabbitMq(builder.Configuration.GetConnectionString("RabbitMq"));
});

// Usage in a service
public class OrderService
{
    private readonly IOutbox _outbox;

    public OrderService(IOutbox outbox)
    {
        _outbox = outbox;
    }

    public async Task CreateOrderAsync(Order order)
    {
        // Business logic + outbox message in one transaction
        await _outbox.PublishAsync(new OrderCreatedEvent
        {
            OrderId = order.Id,
            Total = order.Total
        });
    }
}
```

## Features

- Transactional outbox with EF Core support
- Background dispatcher with configurable polling
- At-least-once delivery guarantee
- Configurable retry policies
- Dead letter support
- Extensible serializer interface

## Requirements

- .NET 10.0+

## License

[MIT](LICENSE)
