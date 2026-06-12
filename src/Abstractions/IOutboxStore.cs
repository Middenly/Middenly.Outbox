namespace Middenly.Outbox.Abstractions;

public interface IOutboxStore
{
    Task StoreAsync(OutboxMessage message, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(
        int batchSize,
        CancellationToken cancellationToken = default);

    Task MarkCompletedAsync(Guid messageId, CancellationToken cancellationToken = default);

    Task MarkFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default);

    Task MarkTerminalFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default);

    Task MoveToDeadLetterAsync(Guid messageId, string error, CancellationToken cancellationToken = default);

    Task<int> RecoverStuckMessagesAsync(
        TimeSpan stuckAfter,
        CancellationToken cancellationToken = default);

    Task<int> CleanupAsync(DateTimeOffset olderThan, CancellationToken cancellationToken = default);

    Task InitializeAsync(CancellationToken cancellationToken = default);
}
