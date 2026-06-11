namespace Middenly.Outbox.Abstractions;

public interface IOutbox
{
    Task PublishAsync(
        string destination,
        byte[] body,
        Action<PublishOptions>? configure = null,
        CancellationToken cancellationToken = default);

    Task PublishAsync<T>(
        string destination,
        T message,
        Action<PublishOptions>? configure = null,
        CancellationToken cancellationToken = default);
}
