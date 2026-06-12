using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Middenly.Outbox.Dispatcher;

public sealed class OutboxDispatcher : BackgroundService
{
    private readonly IOutboxStore _store;
    private readonly IOutboxProducer _producer;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxDispatcher> _logger;
    private readonly SemaphoreSlim _signal = new(0, int.MaxValue);

    public OutboxDispatcher(
        IOutboxStore store,
        IOutboxProducer producer,
        IOptions<OutboxOptions> options,
        ILogger<OutboxDispatcher> logger)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public void NotifyNewMessage()
    {
        _signal.Release();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox dispatcher started with polling interval {Interval}", _options.PollingInterval);

        await _store.InitializeAsync(stoppingToken);

        var lastRecovery = DateTimeOffset.UtcNow;
        var lastCleanup = DateTimeOffset.UtcNow;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (DateTimeOffset.UtcNow - lastRecovery >= _options.RecoveryPollingInterval)
                {
                    await RecoverStuckMessagesAsync(stoppingToken);
                    lastRecovery = DateTimeOffset.UtcNow;
                }

                await ProcessPendingMessagesAsync(stoppingToken);

                if (_options.CleanupInterval.HasValue &&
                    DateTimeOffset.UtcNow - lastCleanup >= _options.CleanupInterval.Value)
                {
                    await CleanupOldMessagesAsync(stoppingToken);
                    lastCleanup = DateTimeOffset.UtcNow;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing outbox messages");
            }

            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                cts.CancelAfter(_options.PollingInterval);
                await _signal.WaitAsync(cts.Token);
            }
            catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
            {
                // Polling interval elapsed, continue processing
            }
        }

        _logger.LogInformation("Outbox dispatcher stopped");
    }

    private async Task RecoverStuckMessagesAsync(CancellationToken cancellationToken)
    {
        try
        {
            var recovered = await _store.RecoverStuckMessagesAsync(
                _options.StuckMessageTimeout,
                cancellationToken);

            if (recovered > 0)
            {
                _logger.LogWarning("Recovered {Count} stuck messages", recovered);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error recovering stuck messages");
        }
    }

    private async Task CleanupOldMessagesAsync(CancellationToken cancellationToken)
    {
        try
        {
            var cutoff = DateTimeOffset.UtcNow.Subtract(_options.MessageRetention);
            var deleted = await _store.CleanupAsync(cutoff, cancellationToken);

            if (deleted > 0)
            {
                _logger.LogInformation("Cleaned up {Count} old outbox messages", deleted);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error cleaning up old messages");
        }
    }

    private async Task ProcessPendingMessagesAsync(CancellationToken cancellationToken)
    {
        var messages = await _store.GetPendingAsync(_options.BatchSize, cancellationToken);

        if (messages.Count == 0)
        {
            _logger.LogTrace("No pending messages found");
            return;
        }

        _logger.LogInformation("Processing {Count} pending outbox messages", messages.Count);

        var byDestination = messages.GroupBy(m => m.Destination);

        var destinationTasks = byDestination.Select(group =>
        {
            var topicOpts = _options.GetTopicOptions(group.Key);
            return ProcessDestinationBatchAsync(group.Key, group.ToList(), topicOpts, cancellationToken);
        });

        await Task.WhenAll(destinationTasks);
    }

    private async Task ProcessDestinationBatchAsync(
        string destination,
        IReadOnlyList<OutboxMessage> messages,
        TopicOptions topicOpts,
        CancellationToken cancellationToken)
    {
        if (topicOpts.Ordered == true)
        {
            _logger.LogDebug("Processing {Count} messages for {Destination} (ordered)", messages.Count, destination);
            foreach (var message in messages)
            {
                await ProcessMessageAsync(message, topicOpts, cancellationToken);
            }
        }
        else
        {
            _logger.LogDebug("Processing {Count} messages for {Destination} (parallel)", messages.Count, destination);
            var tasks = messages.Select(m => ProcessMessageAsync(m, topicOpts, cancellationToken));
            await Task.WhenAll(tasks);
        }
    }

    private async Task ProcessMessageAsync(
        OutboxMessage message,
        TopicOptions topicOpts,
        CancellationToken cancellationToken)
    {
        var maxAttempts = topicOpts.MaxAttempts ?? _options.MaxAttempts;

        try
        {
            if (message.Attempts >= maxAttempts)
            {
                if (_options.EnableDeadLetter)
                {
                    await _store.MoveToDeadLetterAsync(
                        message.Id,
                        $"Max attempts ({maxAttempts}) exceeded",
                        cancellationToken);

                    _logger.LogWarning(
                        "Message {MessageId} exceeded max attempts, moved to dead letter queue",
                        message.Id);
                }
                else
                {
                    await _store.MarkTerminalFailedAsync(
                        message.Id,
                        $"Max attempts ({maxAttempts}) exceeded",
                        cancellationToken);

                    _logger.LogWarning(
                        "Message {MessageId} exceeded max attempts, left in failed state because dead letter queue is disabled",
                        message.Id);
                }

                return;
            }

            await _producer.ProduceAsync(message, cancellationToken);
            await _store.MarkCompletedAsync(message.Id, cancellationToken);

            _logger.LogDebug("Successfully dispatched message {MessageId} to {Destination}",
                message.Id, message.Destination);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to dispatch message {MessageId} to {Destination} (attempt {Attempt})",
                message.Id,
                message.Destination,
                message.Attempts + 1);

            await _store.MarkFailedAsync(message.Id, ex.Message, cancellationToken);
        }
    }

    public override void Dispose()
    {
        _signal.Dispose();
        base.Dispose();
    }
}
