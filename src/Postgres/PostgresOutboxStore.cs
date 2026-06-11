using System.Text;
using System.Text.Json;
using Middenly.Outbox.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Middenly.Outbox.Configuration;
using Npgsql;
using NpgsqlTypes;

namespace Middenly.Outbox.Postgres;

public sealed class PostgresOutboxStore : IOutboxStore
{
    private readonly string _connectionString;
    private readonly OutboxOptions _options;
    private readonly ILogger<PostgresOutboxStore> _logger;
    private readonly string _tableName;

    public PostgresOutboxStore(
        string connectionString,
        IOptions<OutboxOptions> options,
        ILogger<PostgresOutboxStore> logger)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _tableName = $"\"{_options.SchemaName}\".\"{_options.TableName}\"";
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            CREATE SCHEMA IF NOT EXISTS "{_options.SchemaName}";

            CREATE TABLE IF NOT EXISTS {_tableName} (
                id UUID PRIMARY KEY,
                destination VARCHAR(500) NOT NULL,
                key BYTEA,
                body BYTEA NOT NULL,
                headers JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                deliver_after TIMESTAMPTZ,
                attempts INT NOT NULL DEFAULT 0,
                status SMALLINT NOT NULL DEFAULT 0,
                last_error TEXT,
                partition INT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_status_created
                ON {_tableName} (status, created_at)
                WHERE status = 0;

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_status_deliver
                ON {_tableName} (status, deliver_after)
                WHERE status = 0;

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_cleanup
                ON {_tableName} (status, updated_at)
                WHERE status IN (2, 4);
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        await cmd.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogDebug("Outbox table {TableName} initialized", _tableName);
    }

    public async Task StoreAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var headersJson = message.Headers is { Count: > 0 }
            ? JsonSerializer.Serialize(message.Headers.ToDictionary(
                kv => kv.Key,
                kv => Convert.ToBase64String(kv.Value)))
            : null;

        var sql = $"""
            INSERT INTO {_tableName} (id, destination, key, body, headers, created_at, deliver_after, attempts, status, partition)
            VALUES (@id, @destination, @key, @body, @headers::jsonb, @created_at, @deliver_after, @attempts, @status, @partition)
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.Add("id", NpgsqlDbType.Uuid).Value = message.Id;
        cmd.Parameters.Add("destination", NpgsqlDbType.Varchar).Value = message.Destination;
        cmd.Parameters.Add("key", NpgsqlDbType.Bytea).Value = (object?)message.Key ?? DBNull.Value;
        cmd.Parameters.Add("body", NpgsqlDbType.Bytea).Value = message.Body;
        cmd.Parameters.Add("headers", NpgsqlDbType.Jsonb).Value = (object?)headersJson ?? DBNull.Value;
        cmd.Parameters.Add("created_at", NpgsqlDbType.TimestampTz).Value = message.CreatedAt.UtcDateTime;
        cmd.Parameters.Add("deliver_after", NpgsqlDbType.TimestampTz).Value = (object?)message.DeliverAfter?.UtcDateTime ?? DBNull.Value;
        cmd.Parameters.Add("attempts", NpgsqlDbType.Integer).Value = message.Attempts;
        cmd.Parameters.Add("status", NpgsqlDbType.Smallint).Value = (short)message.Status;
        cmd.Parameters.Add("partition", NpgsqlDbType.Integer).Value = (object?)message.Partition ?? DBNull.Value;

        await cmd.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogDebug("Stored outbox message {MessageId} for {Destination}", message.Id, message.Destination);
    }

    public async Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            UPDATE {_tableName}
            SET status = @in_progress, updated_at = NOW()
            WHERE id IN (
                SELECT id FROM {_tableName}
                WHERE status = @pending
                  AND (deliver_after IS NULL OR deliver_after <= NOW())
                ORDER BY created_at
                LIMIT @batch_size
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, destination, key, body, headers, created_at, deliver_after, attempts, status, last_error, partition;
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.Add("pending", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.Pending;
        cmd.Parameters.Add("in_progress", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.InProgress;
        cmd.Parameters.Add("batch_size", NpgsqlDbType.Integer).Value = batchSize;

        var messages = new List<OutboxMessage>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            messages.Add(ReadMessage(reader));
        }

        return messages;
    }

    public async Task MarkCompletedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            UPDATE {_tableName}
            SET status = @status, updated_at = NOW()
            WHERE id = @id AND status = @expected_status
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.Add("id", NpgsqlDbType.Uuid).Value = messageId;
        cmd.Parameters.Add("status", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.Completed;
        cmd.Parameters.Add("expected_status", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.InProgress;

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            UPDATE {_tableName}
            SET status = @status, attempts = attempts + 1, last_error = @error, updated_at = NOW()
            WHERE id = @id AND status = @expected_status
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.Add("id", NpgsqlDbType.Uuid).Value = messageId;
        cmd.Parameters.Add("status", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.Failed;
        cmd.Parameters.Add("expected_status", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.InProgress;
        cmd.Parameters.Add("error", NpgsqlDbType.Text).Value = error;

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MoveToDeadLetterAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            UPDATE {_tableName}
            SET status = @status, last_error = @error, updated_at = NOW()
            WHERE id = @id AND status = @expected_status
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.Add("id", NpgsqlDbType.Uuid).Value = messageId;
        cmd.Parameters.Add("status", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.DeadLettered;
        cmd.Parameters.Add("expected_status", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.InProgress;
        cmd.Parameters.Add("error", NpgsqlDbType.Text).Value = error;

        await cmd.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogWarning("Message {MessageId} moved to dead letter queue: {Error}", messageId, error);
    }

    public async Task<int> RecoverStuckMessagesAsync(
        TimeSpan stuckAfter,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            UPDATE {_tableName}
            SET status = @pending_status, updated_at = NOW()
            WHERE status = @in_progress_status
              AND updated_at < @stuck_after
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.Add("pending_status", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.Pending;
        cmd.Parameters.Add("in_progress_status", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.InProgress;
        cmd.Parameters.Add("stuck_after", NpgsqlDbType.TimestampTz).Value = DateTimeOffset.UtcNow.Subtract(stuckAfter).UtcDateTime;

        var recovered = await cmd.ExecuteNonQueryAsync(cancellationToken);

        if (recovered > 0)
        {
            _logger.LogWarning(
                "Recovered {Count} stuck InProgress messages older than {Timeout}",
                recovered,
                stuckAfter);
        }

        return recovered;
    }

    public async Task<int> CleanupAsync(DateTimeOffset olderThan, CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            DELETE FROM {_tableName}
            WHERE status IN (@completed, @dead_lettered)
              AND updated_at < @older_than
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.Add("completed", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.Completed;
        cmd.Parameters.Add("dead_lettered", NpgsqlDbType.Smallint).Value = (short)OutboxMessageStatus.DeadLettered;
        cmd.Parameters.Add("older_than", NpgsqlDbType.TimestampTz).Value = olderThan.UtcDateTime;

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken);

        if (deleted > 0)
        {
            _logger.LogInformation("Cleaned up {Count} outbox messages older than {Date}", deleted, olderThan);
        }

        return deleted;
    }

    private static OutboxMessage ReadMessage(NpgsqlDataReader reader)
    {
        var headersJson = reader.IsDBNull(reader.GetOrdinal("headers"))
            ? null
            : reader.GetString(reader.GetOrdinal("headers"));

        Dictionary<string, byte[]>? headers = null;
        if (!string.IsNullOrEmpty(headersJson))
        {
            var dict = JsonSerializer.Deserialize<Dictionary<string, string>>(headersJson);
            if (dict is not null)
            {
                headers = dict.ToDictionary(
                    kv => kv.Key,
                    kv => Convert.FromBase64String(kv.Value));
            }
        }

        return new OutboxMessage
        {
            Id = reader.GetGuid(reader.GetOrdinal("id")),
            Destination = reader.GetString(reader.GetOrdinal("destination")),
            Key = reader.IsDBNull(reader.GetOrdinal("key"))
                ? null
                : (byte[])reader.GetValue(reader.GetOrdinal("key")),
            Body = (byte[])reader.GetValue(reader.GetOrdinal("body")),
            Headers = headers,
            CreatedAt = new DateTimeOffset(reader.GetDateTime(reader.GetOrdinal("created_at")), TimeSpan.Zero),
            DeliverAfter = reader.IsDBNull(reader.GetOrdinal("deliver_after"))
                ? null
                : new DateTimeOffset(reader.GetDateTime(reader.GetOrdinal("deliver_after")), TimeSpan.Zero),
            Attempts = reader.GetInt32(reader.GetOrdinal("attempts")),
            Status = (OutboxMessageStatus)reader.GetInt16(reader.GetOrdinal("status")),
            LastError = reader.IsDBNull(reader.GetOrdinal("last_error"))
                ? null
                : reader.GetString(reader.GetOrdinal("last_error")),
            Partition = reader.IsDBNull(reader.GetOrdinal("partition"))
                ? null
                : reader.GetInt32(reader.GetOrdinal("partition"))
        };
    }
}
