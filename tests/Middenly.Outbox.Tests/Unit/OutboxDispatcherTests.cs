using System.Text;
using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Middenly.Outbox.Dispatcher;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Middenly.Outbox.Tests.Unit;

public class OutboxDispatcherTests
{
    [Fact]
    public async Task ExecuteAsync_MaxAttemptsExceededWithDeadLetterEnabled_MovesMessageToDeadLetter()
    {
        // Arrange
        var message = CreateMessage(attempts: 3);
        var store = new FakeOutboxStore(message);
        var producer = new FakeOutboxProducer();
        var dispatcher = CreateDispatcher(store, producer, new OutboxOptions
        {
            MaxAttempts = 3,
            EnableDeadLetter = true,
            PollingInterval = TimeSpan.FromHours(1),
            RecoveryPollingInterval = TimeSpan.FromHours(1)
        });

        // Act
        await dispatcher.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => store.DeadLetteredMessageId.HasValue);
        await dispatcher.StopAsync(CancellationToken.None);

        // Assert
        store.DeadLetteredMessageId.Should().Be(message.Id);
        store.TerminalFailedMessageId.Should().BeNull();
        producer.Produced.Should().BeFalse();
    }

    [Fact]
    public async Task ExecuteAsync_MaxAttemptsExceededWithDeadLetterDisabled_MarksTerminalFailed()
    {
        // Arrange
        var message = CreateMessage(attempts: 3);
        var store = new FakeOutboxStore(message);
        var producer = new FakeOutboxProducer();
        var dispatcher = CreateDispatcher(store, producer, new OutboxOptions
        {
            MaxAttempts = 3,
            EnableDeadLetter = false,
            PollingInterval = TimeSpan.FromHours(1),
            RecoveryPollingInterval = TimeSpan.FromHours(1)
        });

        // Act
        await dispatcher.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => store.TerminalFailedMessageId.HasValue);
        await dispatcher.StopAsync(CancellationToken.None);

        // Assert
        store.TerminalFailedMessageId.Should().Be(message.Id);
        store.DeadLetteredMessageId.Should().BeNull();
        producer.Produced.Should().BeFalse();
    }

    private static OutboxDispatcher CreateDispatcher(
        IOutboxStore store,
        IOutboxProducer producer,
        OutboxOptions options)
    {
        return new OutboxDispatcher(
            store,
            producer,
            Options.Create(options),
            NullLogger<OutboxDispatcher>.Instance);
    }

    private static OutboxMessage CreateMessage(int attempts)
    {
        return new OutboxMessage
        {
            Destination = "test-topic",
            Body = Encoding.UTF8.GetBytes("test-message"),
            Attempts = attempts,
            Status = OutboxMessageStatus.InProgress
        };
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(5);
        while (!condition() && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(50);
        }

        condition().Should().BeTrue();
    }

    private sealed class FakeOutboxStore : IOutboxStore
    {
        private readonly OutboxMessage _message;
        private int _readCount;

        public FakeOutboxStore(OutboxMessage message)
        {
            _message = message;
        }

        public Guid? DeadLetteredMessageId { get; private set; }
        public Guid? TerminalFailedMessageId { get; private set; }

        public Task StoreAsync(OutboxMessage message, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(
            int batchSize,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult<IReadOnlyList<OutboxMessage>>(
                Interlocked.Increment(ref _readCount) == 1
                    ? [_message]
                    : []);
        }

        public Task MarkCompletedAsync(Guid messageId, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task MarkFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task MarkTerminalFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
        {
            TerminalFailedMessageId = messageId;
            return Task.CompletedTask;
        }

        public Task MoveToDeadLetterAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
        {
            DeadLetteredMessageId = messageId;
            return Task.CompletedTask;
        }

        public Task<int> RecoverStuckMessagesAsync(
            TimeSpan stuckAfter,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(0);

        public Task<int> CleanupAsync(DateTimeOffset olderThan, CancellationToken cancellationToken = default) =>
            Task.FromResult(0);

        public Task InitializeAsync(CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    private sealed class FakeOutboxProducer : IOutboxProducer
    {
        public bool Produced { get; private set; }

        public Task ProduceAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            Produced = true;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
