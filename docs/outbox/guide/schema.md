# Database Schema

Middenly.Outbox automatically creates and manages the PostgreSQL schema. This page documents the table structure for reference.

## Outbox Table

### Table: `outbox_messages`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | `UUID` | No | `gen_random_uuid()` | Unique message identifier |
| `topic` | `VARCHAR(500)` | No | — | Kafka topic name |
| `key` | `BYTEA` | Yes | `NULL` | Kafka message key (for partition affinity) |
| `value` | `BYTEA` | No | — | Serialized message body |
| `headers` | `JSONB` | Yes | `NULL` | Message headers as JSON |
| `created_at` | `TIMESTAMPTZ` | No | `NOW()` | When the message was created |
| `deliver_after` | `TIMESTAMPTZ` | Yes | `NULL` | When the message is eligible for delivery |
| `attempts` | `INT` | No | `0` | Number of delivery attempts |
| `status` | `SMALLINT` | No | `0` | Message status (see below) |
| `last_error` | `TEXT` | Yes | `NULL` | Last error message |
| `partition` | `INT` | Yes | `NULL` | Target Kafka partition |
| `updated_at` | `TIMESTAMPTZ` | No | `NOW()` | Last update timestamp |

### Status Transitions

The following status transitions are enforced:

```
Pending (0) → InProgress (1)   [GetPendingAsync - automatic]
InProgress (1) → Completed (2) [MarkCompletedAsync - guarded]
InProgress (1) → Failed (3)    [MarkFailedAsync - guarded]
InProgress (1) → DeadLettered (4) [MoveToDeadLetterAsync - guarded]
InProgress (1) → Pending (0)   [RecoverStuckMessagesAsync - automatic]
Failed (3) → Pending (0)       [GetPendingAsync - on next poll]
```

::: warning
`MarkCompletedAsync`, `MarkFailedAsync`, and `MoveToDeadLetterAsync` only update messages that are currently in `InProgress` status. This prevents race conditions in multi-instance deployments where two instances may try to act on the same message.
:::

| Value | Name | Description |
|-------|------|-------------|
| `0` | Pending | Waiting for delivery |
| `1` | InProgress | Currently being delivered |
| `2` | Completed | Successfully delivered |
| `3` | Failed | Delivery failed, will retry |
| `4` | DeadLettered | Max attempts exceeded |

## Indexes

### `idx_outbox_messages_status_created`

```sql
CREATE INDEX idx_outbox_messages_status_created
    ON outbox_messages (status, created_at)
    WHERE status = 0;
```

Used by `GetPendingAsync()` to efficiently find pending messages ordered by creation time.

### `idx_outbox_messages_status_deliver`

```sql
CREATE INDEX idx_outbox_messages_status_deliver
    ON outbox_messages (status, deliver_after)
    WHERE status = 0;
```

Used to efficiently find messages with delayed delivery that are now eligible.

### `idx_outbox_messages_cleanup`

```sql
CREATE INDEX idx_outbox_messages_cleanup
    ON outbox_messages (status, updated_at)
    WHERE status IN (2, 4);
```

Used by `CleanupAsync()` to efficiently find completed and dead-lettered messages for deletion.

## Headers Storage

Headers are stored as a JSONB object where keys are header names and values are Base64-encoded binary data:

```json
{
    "correlation-id": "NTUwZTg0MDAtZTJjYi00MTQ5LWExMjMtZDM0OTY3YjY1MjA5",
    "source": "b3JkZXItc2VydmljZQ==",
    "content-type": "YXBwbGljYXRpb24vanNvbg=="
}
```

Base64 encoding is used because Kafka headers are binary (`byte[]`), but JSONB only supports string values.

## Custom Schema

You can customize the schema and table names:

```csharp
builder.Services.AddOutbox(options =>
{
    options.SchemaName = "messaging";
    options.TableName = "kafka_outbox";
});
```

This creates the table at `messaging.kafka_outbox`.

## Manual Schema Creation

If you prefer to manage the schema yourself (e.g., for migration tools), you can disable auto-initialization and run the SQL manually. The exact SQL is shown in the [PostgreSQL Store](/guide/postgresql#automatic-schema-management) section.
