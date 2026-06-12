# The Outbox Pattern

## Overview

The Transactional Outbox pattern is a technique for reliably publishing messages to a message broker, such as Kafka, while keeping message creation consistent with database state.

## The Dual-Write Problem

Without the outbox pattern, you face the dual-write problem:

```csharp
// BAD: dual-write is not atomic.
public async Task CreateOrderAsync(CreateOrderCommand command)
{
    var order = new Order { /* ... */ };
    await _dbContext.Orders.AddAsync(order);
    await _dbContext.SaveChangesAsync();

    await _kafkaProducer.ProduceAsync("orders", order);
    // What if the database write succeeds but Kafka publish fails?
}
```

If the database write succeeds but the Kafka publish fails, your system is in an inconsistent state.

## The Outbox Solution

With the outbox pattern, messages are stored in the database within the same transaction as your business data:

```csharp
public async Task CreateOrderAsync(CreateOrderCommand command)
{
    var order = new Order { /* ... */ };
    await _dbContext.Orders.AddAsync(order);

    await _outbox.PublishAsync("orders", new OrderCreated
    {
        OrderId = order.Id,
        Total = order.Total
    });

    await _dbContext.SaveChangesAsync();
    // Message is now in PostgreSQL and will be delivered to Kafka asynchronously.
}
```

## Message Lifecycle

Each message in the outbox goes through the following states:

```text
Pending -> InProgress -> Completed
              |
              -> Failed -> InProgress (after RetryDelay)
              |
              -> DeadLettered (after MaxAttempts, when enabled)
```

| Status | Description |
|--------|-------------|
| `Pending` (0) | Message stored, waiting for delivery |
| `InProgress` (1) | Picked up by dispatcher, being delivered |
| `Completed` (2) | Successfully delivered to Kafka |
| `Failed` (3) | Delivery failed; will retry after `RetryDelay`, unless terminal |
| `DeadLettered` (4) | Max attempts exceeded |

## Delivery Guarantees

Middenly.Outbox provides at-least-once delivery:

- Messages are guaranteed to be delivered at least once.
- In rare cases, such as a crash after delivery but before marking complete, a message may be delivered more than once.
- Consumers should be idempotent and handle duplicate messages gracefully.

## Concurrency Handling

Multiple instances of your service can run simultaneously. The outbox uses PostgreSQL's `FOR UPDATE SKIP LOCKED` to ensure safe concurrent access:

```sql
UPDATE outbox_messages
SET status = 1, updated_at = NOW()
WHERE id IN (
    SELECT id FROM outbox_messages
    WHERE (
        status = 0
        AND (deliver_after IS NULL OR deliver_after <= NOW())
      )
      OR (
        status = 3
        AND deliver_after IS NOT NULL
        AND deliver_after <= NOW()
      )
    ORDER BY created_at
    LIMIT 100
    FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

This ensures that:

- Each message is picked up by exactly one dispatcher instance.
- Concurrent dispatchers do not block each other on locked rows.
- Failed messages are retried only after their retry delay has elapsed.

## Comparison with Two-Phase Commit

| Approach | Complexity | Performance | Consistency |
|----------|------------|-------------|-------------|
| Two-Phase Commit | High | Low | Strong |
| Transactional Outbox | Low | High | Eventual |
| No coordination | Low | High | None |

The outbox pattern trades strong consistency for high availability and performance, which is the right trade-off for most microservice architectures.

## Further Reading

- [Microservices.io: Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)
- [Microsoft: Outbox Pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/transactional-outbox)
