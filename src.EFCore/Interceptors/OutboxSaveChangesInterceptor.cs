using System.Text.Json;
using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Middenly.Outbox.Dispatcher;
using Middenly.Outbox.EntityFrameworkCore.Implementation;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Middenly.Outbox.EntityFrameworkCore.Interceptors;

public sealed class OutboxSaveChangesInterceptor : SaveChangesInterceptor, IDisposable, IAsyncDisposable
{
    private readonly object _transactionLock = new();
    private readonly Dictionary<DbContext, OwnedTransaction> _ownedTransactions = new();
    private readonly IOutboxStore _store;
    private readonly OutboxDispatcher? _dispatcher;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxSaveChangesInterceptor> _logger;
    private bool _disposed;

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

    public override InterceptionResult<int> SavingChanges(
        DbContextEventData eventData,
        InterceptionResult<int> result)
    {
        var collector = OutboxMessageCollector.Current;
        var messages = collector.Drain();
        OutboxMessageCollector.Reset();

        if (messages.Count == 0)
            return result;

        var context = eventData.Context;
        if (context is null)
            return result;

        try
        {
            EnsureOwnedTransaction(context);
            StoreMessages(context, messages);
        }
        catch
        {
            RollbackOwnedTransaction(context);
            throw;
        }

        return result;
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

        try
        {
            await EnsureOwnedTransactionAsync(context, cancellationToken);
            await StoreMessagesAsync(context, messages, cancellationToken);
        }
        catch
        {
            await RollbackOwnedTransactionAsync(context, cancellationToken);
            throw;
        }

        return result;
    }

    public override int SavedChanges(SaveChangesCompletedEventData eventData, int result)
    {
        CommitOwnedTransaction(eventData.Context);
        NotifyDispatcher();
        return result;
    }

    public override async ValueTask<int> SavedChangesAsync(
        SaveChangesCompletedEventData eventData,
        int result,
        CancellationToken cancellationToken = default)
    {
        await CommitOwnedTransactionAsync(eventData.Context, cancellationToken);
        NotifyDispatcher();
        return result;
    }

    public override void SaveChangesFailed(DbContextErrorEventData eventData)
    {
        RollbackOwnedTransaction(eventData.Context);
        _logger.LogWarning("SaveChanges failed, discarding pending outbox messages");
    }

    public override async Task SaveChangesFailedAsync(
        DbContextErrorEventData eventData,
        CancellationToken cancellationToken = default)
    {
        await RollbackOwnedTransactionAsync(eventData.Context, cancellationToken);
        _logger.LogWarning("SaveChanges failed, discarding pending outbox messages");
    }

    private void EnsureOwnedTransaction(DbContext context)
    {
        if (context.Database.CurrentTransaction is not null || HasOwnedTransaction(context))
            return;

        var transaction = context.Database.BeginTransaction();
        AddOwnedTransaction(context, transaction);
    }

    private async Task EnsureOwnedTransactionAsync(DbContext context, CancellationToken cancellationToken)
    {
        if (context.Database.CurrentTransaction is not null || HasOwnedTransaction(context))
            return;

        var transaction = await context.Database.BeginTransactionAsync(cancellationToken);
        AddOwnedTransaction(context, transaction);
    }

    private void StoreMessages(DbContext context, IReadOnlyCollection<OutboxMessage> messages)
    {
        var tableName = $"\"{_options.SchemaName}\".\"{_options.TableName}\"";

        foreach (var message in messages)
        {
            var (sql, parameters) = CreateInsertCommand(tableName, message);
            context.Database.ExecuteSqlRaw(sql, parameters);

            _logger.LogDebug("Stored outbox message {MessageId} for {Destination} in EF Core transaction",
                message.Id, message.Destination);
        }
    }

    private async Task StoreMessagesAsync(
        DbContext context,
        IReadOnlyCollection<OutboxMessage> messages,
        CancellationToken cancellationToken)
    {
        var tableName = $"\"{_options.SchemaName}\".\"{_options.TableName}\"";

        foreach (var message in messages)
        {
            var (sql, parameters) = CreateInsertCommand(tableName, message);
            await context.Database.ExecuteSqlRawAsync(sql, parameters, cancellationToken);

            _logger.LogDebug("Stored outbox message {MessageId} for {Destination} in EF Core transaction",
                message.Id, message.Destination);
        }
    }

    private static (string Sql, Npgsql.NpgsqlParameter[] Parameters) CreateInsertCommand(
        string tableName,
        OutboxMessage message)
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

        return (sql, parameters);
    }

    private void CommitOwnedTransaction(DbContext? context)
    {
        if (context is null || !TryRemoveOwnedTransaction(context, out var owned))
            return;

        try
        {
            owned.Transaction.Commit();
        }
        finally
        {
            owned.Transaction.Dispose();
        }
    }

    private async Task CommitOwnedTransactionAsync(DbContext? context, CancellationToken cancellationToken)
    {
        if (context is null || !TryRemoveOwnedTransaction(context, out var owned))
            return;

        try
        {
            await owned.Transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            await owned.Transaction.DisposeAsync();
        }
    }

    private void RollbackOwnedTransaction(DbContext? context)
    {
        if (context is null || !TryRemoveOwnedTransaction(context, out var owned))
            return;

        try
        {
            owned.Transaction.Rollback();
        }
        finally
        {
            owned.Transaction.Dispose();
        }
    }

    private async Task RollbackOwnedTransactionAsync(DbContext? context, CancellationToken cancellationToken)
    {
        if (context is null || !TryRemoveOwnedTransaction(context, out var owned))
            return;

        try
        {
            await owned.Transaction.RollbackAsync(cancellationToken);
        }
        finally
        {
            await owned.Transaction.DisposeAsync();
        }
    }

    private bool HasOwnedTransaction(DbContext context)
    {
        lock (_transactionLock)
        {
            return _ownedTransactions.ContainsKey(context);
        }
    }

    private void AddOwnedTransaction(DbContext context, IDbContextTransaction transaction)
    {
        lock (_transactionLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _ownedTransactions.Add(context, new OwnedTransaction(transaction));
        }
    }

    private bool TryRemoveOwnedTransaction(DbContext context, out OwnedTransaction owned)
    {
        lock (_transactionLock)
        {
            if (!_ownedTransactions.Remove(context, out owned!))
            {
                return false;
            }

            return true;
        }
    }

    private List<OwnedTransaction> DrainOwnedTransactions()
    {
        lock (_transactionLock)
        {
            _disposed = true;
            var transactions = _ownedTransactions.Values.ToList();
            _ownedTransactions.Clear();
            return transactions;
        }
    }

    private void NotifyDispatcher()
    {
        _dispatcher?.NotifyNewMessage();
    }

    public void Dispose()
    {
        foreach (var owned in DrainOwnedTransactions())
        {
            try
            {
                owned.Transaction.Rollback();
            }
            finally
            {
                owned.Transaction.Dispose();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var owned in DrainOwnedTransactions())
        {
            try
            {
                await owned.Transaction.RollbackAsync();
            }
            finally
            {
                await owned.Transaction.DisposeAsync();
            }
        }
    }

    private sealed class OwnedTransaction
    {
        public OwnedTransaction(IDbContextTransaction transaction)
        {
            Transaction = transaction;
        }

        public IDbContextTransaction Transaction { get; }
    }
}
