using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Dispatcher;
using Microsoft.Extensions.Logging;

namespace Middenly.Outbox.Implementation;

public sealed class DefaultOutbox : IOutbox
{
    private readonly IOutboxStore _store;
    private readonly IOutboxSerializer _serializer;
    private readonly OutboxDispatcher? _dispatcher;
    private readonly ILogger<DefaultOutbox> _logger;

    public DefaultOutbox(
        IOutboxStore store,
        IOutboxSerializer serializer,
        ILogger<DefaultOutbox> logger,
        OutboxDispatcher? dispatcher = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dispatcher = dispatcher;
    }

    public async Task PublishAsync(
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

        await _store.StoreAsync(message, cancellationToken);

        _logger.LogDebug("Published message {MessageId} to outbox for {Destination}", message.Id, destination);

        _dispatcher?.NotifyNewMessage();
    }

    public async Task PublishAsync<T>(
        string destination,
        T message,
        Action<PublishOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        var body = _serializer.Serialize(message);

        await PublishAsync(destination, body, configure, cancellationToken);
    }
}
