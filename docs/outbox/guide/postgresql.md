# PostgreSQL Store

Middenly.Outbox uses PostgreSQL as the backing store for outbox messages via the [Npgsql](https://www.npgsql.org/) driver directly (no ORM required).

## Setup

```csharp
builder.Services.AddOutbox(options => { /* ... */ })
    .UsePostgresStore(builder.Configuration.GetConnectionString("Default")!);
```

The connection string should be a standard Npgsql connection string:

```
Host=localhost;Database=mydb;Username=myuser;Password=mypassword
```

## Automatic Schema Management

The outbox table and indexes are created automatically when the dispatcher starts. You don't need to run migrations manually.

The store calls `InitializeAsync()` on startup which executes:

```sql
CREATE SCHEMA IF NOT EXISTS "public";

CREATE TABLE IF NOT EXISTS "public"."outbox_messages" (
    id UUID PRIMARY KEY,
    topic VARCHAR(500) NOT NULL,
    key BYTEA,
    value BYTEA NOT NULL,
    headers JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deliver_after TIMESTAMPTZ,
    attempts INT NOT NULL DEFAULT 0,
    status SMALLINT NOT NULL DEFAULT 0,
    last_error TEXT,
    partition INT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for efficient polling of pending messages
CREATE INDEX IF NOT EXISTS idx_outbox_messages_status_created
    ON "public"."outbox_messages" (status, created_at)
    WHERE status = 0;

-- Index for delayed delivery
CREATE INDEX IF NOT EXISTS idx_outbox_messages_status_deliver
    ON "public"."outbox_messages" (status, deliver_after)
    WHERE status = 0;

-- Index for cleanup of completed/dead-lettered messages
CREATE INDEX IF NOT EXISTS idx_outbox_messages_cleanup
    ON "public"."outbox_messages" (status, updated_at)
    WHERE status IN (2, 4);
```

## Custom Table and Schema

You can customize the table and schema names:

```csharp
builder.Services.AddOutbox(options =>
{
    options.TableName = "my_outbox";
    options.SchemaName = "messaging";
});
```

This creates the table at `messaging.my_outbox`.

## How Polling Works

The dispatcher uses PostgreSQL's `FOR UPDATE SKIP LOCKED` for safe concurrent access:

```sql
UPDATE outbox_messages
SET status = 1, updated_at = NOW()
WHERE id IN (
    SELECT id FROM outbox_messages
    WHERE status = 0
      AND (deliver_after IS NULL OR deliver_after <= NOW())
    ORDER BY created_at
    LIMIT @batch_size
    FOR UPDATE SKIP LOCKED
)
RETURNING id, topic, key, value, headers, created_at, deliver_after, attempts, status, last_error, partition;
```

### Why FOR UPDATE SKIP LOCKED?

- **FOR UPDATE**: Locks the selected rows to prevent other transactions from modifying them
- **SKIP LOCKED**: If a row is already locked by another transaction, skip it instead of waiting

This means multiple dispatcher instances can run simultaneously without:
- Deadlocks
- Duplicate processing
- Blocking each other

## Headers Storage

Message headers are stored as JSONB in PostgreSQL:

```json
{
    "correlation-id": "Base64EncodedValue",
    "source": "Base64EncodedValue"
}
```

Header values are Base64-encoded because they are binary (`byte[]`) in the Kafka protocol.

## Performance Considerations

### Batch Size

The `BatchSize` option controls how many messages are read per polling cycle. Larger batches reduce database round-trips but increase memory usage.

```csharp
options.BatchSize = 500; // High throughput
options.BatchSize = 50;  // Low latency
```

### Indexes

The automatically created indexes cover:
- **Polling**: `(status, created_at) WHERE status = 0` — fast lookup of pending messages
- **Delayed delivery**: `(status, deliver_after) WHERE status = 0` — fast lookup of messages ready for delivery
- **Cleanup**: `(status, updated_at) WHERE status IN (2, 4)` — fast cleanup of completed messages

### Connection Pooling

Npgsql uses connection pooling by default. For high-throughput systems, consider tuning the connection string:

```
Host=localhost;Database=mydb;Username=user;Password=pass;Minimum Pool Size=5;Maximum Pool Size=100
```

## Multiple Instances

You can run multiple instances of your service safely. The `FOR UPDATE SKIP LOCKED` mechanism ensures that each message is processed by exactly one instance.

```
Instance A ──► picks up messages 1-100
Instance B ──► picks up messages 101-200 (skips locked)
Instance C ──► no messages available, waits
```

### Kubernetes Considerations

When running in Kubernetes with multiple replicas:

**Crash Recovery**: If a pod crashes while processing a message (between `GetPendingAsync` and `MarkCompletedAsync`), the message stays in `InProgress` status. The dispatcher automatically recovers stuck messages after `StuckMessageTimeout` (default: 5 minutes):

```csharp
builder.Services.AddOutbox(options =>
{
    options.StuckMessageTimeout = TimeSpan.FromMinutes(5);
    options.RecoveryPollingInterval = TimeSpan.FromMinutes(1);
});
```

**Graceful Shutdown**: When a pod receives SIGTERM, the `BackgroundService` cancellation token is triggered. The dispatcher stops polling, but in-flight messages may not complete. These are recovered by other pods via the stuck message mechanism.

**Status Guards**: `MarkCompletedAsync` and `MarkFailedAsync` only update messages in `InProgress` status. This prevents race conditions where two instances try to act on the same message.
