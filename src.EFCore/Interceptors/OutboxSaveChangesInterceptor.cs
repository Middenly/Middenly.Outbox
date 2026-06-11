using System.Text.Json;
using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Middenly.Outbox.Dispatcher;
using Middenly.Outbox.EntityFrameworkCore.Implementation;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Middenly.Outbox.EntityFrameworkCore.Interceptors;

public sealed class OutboxSaveChangesInterceptor : SaveChangesInterceptor
{
    private readonly IOutboxStore _store;
    private readonly OutboxDispatcher? _dispatcher;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxSaveChangesInterceptor> _logger;

    public OutboxSaveChangesInterceptor(
        IOutboxStore store,
        IOptions<OutboxOptions> options,
        ILogger<OutboxSaveChangesInterceptor> logger,
        OutboxDispatcher? dispatcher = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dispatcher = dispatcher;
    }

    public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        var collector = OutboxMessageCollector.Current;
        var messages = collector.Drain();
        OutboxMessageCollector.Reset();

        if (messages.Count == 0)
            return result;

        var context = eventData.Context;
        if (context is null)
            return result;

        var tableName = $"\"{_options.SchemaName}\".\"{_options.TableName}\"";

        foreach (var message in messages)
        {
            var headersJson = message.Headers is { Count: > 0 }
                ? JsonSerializer.Serialize(message.Headers.ToDictionary(
                    kv => kv.Key,
                    kv => Convert.ToBase64String(kv.Value)))
                : null;

            var sql = string.Format(
                "INSERT INTO {0} (id, destination, key, body, headers, created_at, deliver_after, attempts, status, partition) " +
                "VALUES (@p0, @p1, @p2, @p3, @p4::jsonb, @p5, @p6, @p7, @p8, @p9)",
                tableName);

            var parameters = new Npgsql.NpgsqlParameter[]
            {
                new("p0", message.Id),
                new("p1", message.Destination),
                new("p2", (object?)message.Key ?? DBNull.Value) { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bytea },
                new("p3", message.Body) { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bytea },
                new("p4", (object?)headersJson ?? DBNull.Value) { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Jsonb },
                new("p5", message.CreatedAt.UtcDateTime) { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz },
                new("p6", (object?)message.DeliverAfter?.UtcDateTime ?? DBNull.Value) { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz },
                new("p7", message.Attempts),
                new("p8", (short)message.Status),
                new("p9", (object?)message.Partition ?? DBNull.Value) { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer }
            };

            await context.Database.ExecuteSqlRawAsync(sql, parameters, cancellationToken);

            _logger.LogDebug("Stored outbox message {MessageId} for {Destination} in EF Core transaction",
                message.Id, message.Destination);
        }

        return result;
    }

    public override int SavedChanges(SaveChangesCompletedEventData eventData, int result)
    {
        NotifyDispatcher();
        return result;
    }

    public override ValueTask<int> SavedChangesAsync(
        SaveChangesCompletedEventData eventData,
        int result,
        CancellationToken cancellationToken = default)
    {
        NotifyDispatcher();
        return ValueTask.FromResult(result);
    }

    public override void SaveChangesFailed(DbContextErrorEventData eventData)
    {
        _logger.LogWarning("SaveChanges failed, discarding pending outbox messages");
    }

    public override Task SaveChangesFailedAsync(
        DbContextErrorEventData eventData,
        CancellationToken cancellationToken = default)
    {
        _logger.LogWarning("SaveChanges failed, discarding pending outbox messages");
        return Task.CompletedTask;
    }

    private void NotifyDispatcher()
    {
        _dispatcher?.NotifyNewMessage();
    }
}
