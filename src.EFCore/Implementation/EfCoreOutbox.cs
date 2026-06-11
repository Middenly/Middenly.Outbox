using Middenly.Outbox.Abstractions;
using Microsoft.Extensions.Logging;

namespace Middenly.Outbox.EntityFrameworkCore.Implementation;

public sealed class EfCoreOutbox : IOutbox
{
    private readonly IOutboxSerializer _serializer;
    private readonly ILogger<EfCoreOutbox> _logger;

    public EfCoreOutbox(
        IOutboxSerializer serializer,
        ILogger<EfCoreOutbox> logger)
    {
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task PublishAsync(
        string destination,
        byte[] body,
        Action<PublishOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(destination);
        ArgumentNullException.ThrowIfNull(body);

        var options = new PublishOptions();
        configure?.Invoke(options);

        var message = new OutboxMessage
        {
            Destination = destination,
            Body = body,
            Key = options.Key,
            Headers = options.Headers.Count > 0 ? options.Headers : null,
            DeliverAfter = options.DeliverAfter,
            Partition = options.Partition
        };

        OutboxMessageCollector.Current.Add(message);

        _logger.LogDebug("Queued message {MessageId} for {Destination} (will be stored on SaveChanges)",
            message.Id, destination);

        return Task.CompletedTask;
    }

    public Task PublishAsync<T>(
        string destination,
        T message,
        Action<PublishOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        var body = _serializer.Serialize(message);

        return PublishAsync(destination, body, configure, cancellationToken);
    }
}
