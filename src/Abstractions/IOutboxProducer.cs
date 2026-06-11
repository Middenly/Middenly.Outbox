namespace Middenly.Outbox.Abstractions;

public interface IOutboxProducer : IAsyncDisposable
{
    Task ProduceAsync(OutboxMessage message, CancellationToken cancellationToken = default);
}
