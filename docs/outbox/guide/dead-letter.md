# Dead Letter Queue

When a message fails to deliver after the maximum number of attempts, it is moved to the dead letter queue (DLQ). This prevents poison messages from blocking the outbox.

## How It Works

```
Message created (Pending)
    ↓
Delivery attempt 1 → Failed
    ↓
Delivery attempt 2 → Failed
    ↓
...
    ↓
Delivery attempt N → Failed (N = MaxAttempts)
    ↓
Moved to Dead Letter Queue (DeadLettered)
```

## Configuration

```csharp
builder.Services.AddOutbox(options =>
{
    options.MaxAttempts = 5;         // Move to DLQ after 5 failed attempts
    options.EnableDeadLetter = true; // Enable DLQ (default: true)
});
```

### Disable Dead Letter Queue

If you want failed messages to remain in the `Failed` state indefinitely:

```csharp
options.EnableDeadLetter = false;
```

Failed messages will still be retried on each polling cycle.

## Monitoring Dead Letters

Dead-lettered messages remain in the outbox table with `status = 4`. You can query them directly:

```sql
-- View all dead-lettered messages
SELECT id, topic, key, value, attempts, last_error, created_at, updated_at
FROM outbox_messages
WHERE status = 4
ORDER BY updated_at DESC;

-- Count dead-lettered messages
SELECT COUNT(*) FROM outbox_messages WHERE status = 4;

-- Get error distribution
SELECT last_error, COUNT(*) as count
FROM outbox_messages
WHERE status = 4
GROUP BY last_error
ORDER BY count DESC;
```

## Cleanup

Dead-lettered messages are cleaned up along with completed messages based on the `MessageRetention` setting:

```csharp
options.CleanupInterval = TimeSpan.FromHours(1);  // Run cleanup every hour
options.MessageRetention = TimeSpan.FromDays(7);   // Keep messages for 7 days
```

The cleanup query:

```sql
DELETE FROM outbox_messages
WHERE status IN (2, 4)  -- Completed or DeadLettered
  AND updated_at < @older_than;
```

## Replaying Dead-Lettered Messages

To manually replay dead-lettered messages, update their status back to `Pending`:

```sql
-- Replay all dead-lettered messages
UPDATE outbox_messages
SET status = 0, attempts = 0, last_error = NULL, updated_at = NOW()
WHERE status = 4;

-- Replay specific messages
UPDATE outbox_messages
SET status = 0, attempts = 0, last_error = NULL, updated_at = NOW()
WHERE status = 4
  AND topic = 'order-events'
  AND updated_at > NOW() - INTERVAL '1 day';
```

## Best Practices

1. **Set appropriate MaxAttempts**: Too low may cause unnecessary DLQ entries for transient failures; too high wastes resources on poison messages.

2. **Monitor DLQ size**: Set up alerts for growing dead letter counts.

3. **Investigate root causes**: Each dead-lettered message includes the `last_error` — use it to diagnose the issue.

4. **Implement consumer idempotency**: Since the outbox provides at-least-once delivery, consumers should handle duplicate messages gracefully.

5. **Clean up regularly**: Use `CleanupInterval` and `MessageRetention` to prevent unbounded table growth.
