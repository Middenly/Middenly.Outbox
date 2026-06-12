using System.Text;
using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Middenly.Outbox.Postgres;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.PostgreSql;
using Xunit;

namespace Middenly.Outbox.Tests.Integration;

public class PostgresOutboxStoreTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("outbox_test")
        .WithUsername("test")
        .WithPassword("test")
        .Build();

    private PostgresOutboxStore _store = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

        var options = Options.Create(new OutboxOptions
        {
            TableName = "outbox_messages",
            SchemaName = "public"
        });

        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var logger = loggerFactory.CreateLogger<PostgresOutboxStore>();

        _store = new PostgresOutboxStore(_postgres.GetConnectionString(), options, logger);
        await _store.InitializeAsync();
    }

    public async Task DisposeAsync()
    {
        await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task StoreAsync_ShouldPersistMessage()
    {
        // Arrange
        var message = CreateTestMessage("test-topic");

        // Act
        await _store.StoreAsync(message);

        // Assert
        var pending = await _store.GetPendingAsync(10);
        pending.Should().HaveCount(1);
        pending[0].Id.Should().Be(message.Id);
        pending[0].Destination.Should().Be("test-topic");
    }

    [Fact]
    public async Task StoreAsync_WithKey_ShouldPersistKey()
    {
        // Arrange
        var key = Encoding.UTF8.GetBytes("test-key");
        var message = CreateTestMessage("test-topic", key: key);

        // Act
        await _store.StoreAsync(message);

        // Assert
        var pending = await _store.GetPendingAsync(10);
        pending.Should().HaveCount(1);
        pending[0].Key.Should().BeEquivalentTo(key);
    }

    [Fact]
    public async Task StoreAsync_WithHeaders_ShouldPersistHeaders()
    {
        // Arrange
        var headers = new Dictionary<string, byte[]>
        {
            ["header1"] = Encoding.UTF8.GetBytes("value1"),
            ["header2"] = Encoding.UTF8.GetBytes("value2")
        };
        var message = CreateTestMessage("test-topic", headers: headers);

        // Act
        await _store.StoreAsync(message);

        // Assert
        var pending = await _store.GetPendingAsync(10);
        pending.Should().HaveCount(1);
        pending[0].Headers.Should().NotBeNull();
        pending[0].Headers!["header1"].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("value1"));
        pending[0].Headers!["header2"].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("value2"));
    }

    [Fact]
    public async Task StoreAsync_WithDeliverAfter_ShouldNotReturnBeforeTime()
    {
        // Arrange
        var message = CreateTestMessage("test-topic", deliverAfter: DateTimeOffset.UtcNow.AddHours(1));

        // Act
        await _store.StoreAsync(message);

        // Assert
        var pending = await _store.GetPendingAsync(10);
        pending.Should().BeEmpty();
    }

    [Fact]
    public async Task GetPendingAsync_ShouldReturnAllPendingMessages()
    {
        // Arrange
        var message1 = CreateTestMessage("topic1");
        var message2 = CreateTestMessage("topic2");
        var message3 = CreateTestMessage("topic3");

        await _store.StoreAsync(message1);
        await _store.StoreAsync(message2);
        await _store.StoreAsync(message3);

        // Act
        var pending = await _store.GetPendingAsync(10);

        // Assert
        pending.Should().HaveCount(3);
        pending.Select(m => m.Id).Should().Contain(message1.Id);
        pending.Select(m => m.Id).Should().Contain(message2.Id);
        pending.Select(m => m.Id).Should().Contain(message3.Id);
    }

    [Fact]
    public async Task GetPendingAsync_ShouldRespectBatchSize()
    {
        // Arrange
        for (int i = 0; i < 5; i++)
        {
            await _store.StoreAsync(CreateTestMessage($"topic{i}"));
        }

        // Act
        var pending = await _store.GetPendingAsync(3);

        // Assert
        pending.Should().HaveCount(3);
    }

    [Fact]
    public async Task GetPendingAsync_ShouldMarkMessagesAsInProgress()
    {
        // Arrange
        var message = CreateTestMessage("test-topic");
        await _store.StoreAsync(message);

        // Act
        var pending = await _store.GetPendingAsync(10);

        // Assert
        pending.Should().HaveCount(1);
        pending[0].Status.Should().Be(OutboxMessageStatus.InProgress);

        // Should not return the same message again
        var pending2 = await _store.GetPendingAsync(10);
        pending2.Should().BeEmpty();
    }

    [Fact]
    public async Task MarkCompletedAsync_ShouldUpdateStatus()
    {
        // Arrange
        var message = CreateTestMessage("test-topic");
        await _store.StoreAsync(message);
        var pending = await _store.GetPendingAsync(10);

        // Act
        await _store.MarkCompletedAsync(pending[0].Id);

        // Assert
        var allMessages = await _store.GetPendingAsync(10);
        allMessages.Should().BeEmpty();
    }

    [Fact]
    public async Task MarkFailedAsync_ShouldIncrementAttempts()
    {
        // Arrange
        var message = CreateTestMessage("test-topic");
        await _store.StoreAsync(message);
        var pending = await _store.GetPendingAsync(10);

        // Act
        await _store.MarkFailedAsync(pending[0].Id, "test error");

        // Assert
        var stored = await ReadStoredMessageAsync(message.Id);
        stored.Status.Should().Be(OutboxMessageStatus.Failed);
        stored.Attempts.Should().Be(1);
        stored.LastError.Should().Be("test error");
        stored.DeliverAfter.Should().NotBeNull();
    }

    [Fact]
    public async Task GetPendingAsync_FailedBeforeRetryDelay_ShouldNotReturnMessage()
    {
        // Arrange
        var options = Options.Create(new OutboxOptions
        {
            TableName = "outbox_messages",
            SchemaName = "public",
            RetryDelay = TimeSpan.FromHours(1)
        });
        var retryStore = new PostgresOutboxStore(
            _postgres.GetConnectionString(),
            options,
            LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<PostgresOutboxStore>());

        var message = CreateTestMessage("test-topic");
        await retryStore.StoreAsync(message);
        var pending = await retryStore.GetPendingAsync(10);
        await retryStore.MarkFailedAsync(pending[0].Id, "temporary failure");

        // Act
        var retryable = await retryStore.GetPendingAsync(10);

        // Assert
        retryable.Should().BeEmpty();
    }

    [Fact]
    public async Task GetPendingAsync_FailedAfterRetryDelay_ShouldReturnMessage()
    {
        // Arrange
        var options = Options.Create(new OutboxOptions
        {
            TableName = "outbox_messages",
            SchemaName = "public",
            RetryDelay = TimeSpan.Zero
        });
        var retryStore = new PostgresOutboxStore(
            _postgres.GetConnectionString(),
            options,
            LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<PostgresOutboxStore>());

        var message = CreateTestMessage("test-topic");
        await retryStore.StoreAsync(message);
        var pending = await retryStore.GetPendingAsync(10);
        await retryStore.MarkFailedAsync(pending[0].Id, "temporary failure");

        // Act
        var retryable = await retryStore.GetPendingAsync(10);

        // Assert
        retryable.Should().HaveCount(1);
        retryable[0].Id.Should().Be(message.Id);
        retryable[0].Attempts.Should().Be(1);
        retryable[0].Status.Should().Be(OutboxMessageStatus.InProgress);
    }

    [Fact]
    public async Task ConcurrentGetPending_WithRetryableFailedMessages_ShouldNotReturnDuplicates()
    {
        // Arrange
        var options = Options.Create(new OutboxOptions
        {
            TableName = "outbox_messages",
            SchemaName = "public",
            RetryDelay = TimeSpan.Zero
        });
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var store1 = new PostgresOutboxStore(
            _postgres.GetConnectionString(), options, loggerFactory.CreateLogger<PostgresOutboxStore>());
        var store2 = new PostgresOutboxStore(
            _postgres.GetConnectionString(), options, loggerFactory.CreateLogger<PostgresOutboxStore>());

        for (int i = 0; i < 20; i++)
        {
            var message = CreateTestMessage($"topic{i}");
            await store1.StoreAsync(message);
            var pending = await store1.GetPendingAsync(1);
            await store1.MarkFailedAsync(pending[0].Id, "temporary failure");
        }

        // Act
        var task1 = store1.GetPendingAsync(15);
        var task2 = store2.GetPendingAsync(15);
        await Task.WhenAll(task1, task2);

        var messages1 = await task1;
        var messages2 = await task2;

        // Assert
        var allIds = messages1.Select(m => m.Id).Concat(messages2.Select(m => m.Id)).ToList();
        allIds.Should().OnlyHaveUniqueItems();
        allIds.Should().HaveCount(20);
    }

    [Fact]
    public async Task MarkTerminalFailedAsync_ShouldLeaveFailedMessageOutOfPolling()
    {
        // Arrange
        var message = CreateTestMessage("test-topic");
        await _store.StoreAsync(message);
        var pending = await _store.GetPendingAsync(10);

        // Act
        await _store.MarkTerminalFailedAsync(pending[0].Id, "max attempts exceeded");

        // Assert
        var stored = await ReadStoredMessageAsync(message.Id);
        stored.Status.Should().Be(OutboxMessageStatus.Failed);
        stored.DeliverAfter.Should().BeNull();

        var retryable = await _store.GetPendingAsync(10);
        retryable.Should().BeEmpty();
    }

    [Fact]
    public async Task MoveToDeadLetterAsync_ShouldUpdateStatus()
    {
        // Arrange
        var message = CreateTestMessage("test-topic");
        await _store.StoreAsync(message);
        var pending = await _store.GetPendingAsync(10);

        // Act
        await _store.MoveToDeadLetterAsync(pending[0].Id, "max attempts exceeded");

        // Assert
        var allPending = await _store.GetPendingAsync(10);
        allPending.Should().BeEmpty();
    }

    [Fact]
    public async Task CleanupAsync_ShouldRemoveOldCompletedMessages()
    {
        // Arrange
        var message = CreateTestMessage("test-topic");
        await _store.StoreAsync(message);
        var pending = await _store.GetPendingAsync(10);
        await _store.MarkCompletedAsync(pending[0].Id);

        // Act - cleanup messages older than now (should remove the completed message)
        var deleted = await _store.CleanupAsync(DateTimeOffset.UtcNow.AddSeconds(1));

        // Assert
        deleted.Should().Be(1);
    }

    [Fact]
    public async Task StoreAsync_WithPartition_ShouldPersistPartition()
    {
        // Arrange
        var message = CreateTestMessage("test-topic", partition: 5);

        // Act
        await _store.StoreAsync(message);

        // Assert
        var pending = await _store.GetPendingAsync(10);
        pending.Should().HaveCount(1);
        pending[0].Partition.Should().Be(5);
    }

    [Fact]
    public async Task MultipleMessages_ShouldHandleConcurrentAccess()
    {
        // Arrange
        var tasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            var msg = CreateTestMessage($"topic{i}");
            tasks.Add(_store.StoreAsync(msg));
        }

        // Act
        await Task.WhenAll(tasks);

        // Assert
        var pending = await _store.GetPendingAsync(100);
        pending.Should().HaveCount(10);
    }

    [Fact]
    public async Task ConcurrentGetPending_ShouldNotReturnDuplicateMessages()
    {
        // Arrange - simulate two "instances" using separate store objects
        var options = Options.Create(new OutboxOptions
        {
            TableName = "outbox_messages",
            SchemaName = "public"
        });
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var store2 = new PostgresOutboxStore(
            _postgres.GetConnectionString(), options, loggerFactory.CreateLogger<PostgresOutboxStore>());

        for (int i = 0; i < 20; i++)
        {
            await _store.StoreAsync(CreateTestMessage($"topic{i}"));
        }

        // Act - both stores read concurrently
        var task1 = _store.GetPendingAsync(15);
        var task2 = store2.GetPendingAsync(15);
        await Task.WhenAll(task1, task2);

        var messages1 = await task1;
        var messages2 = await task2;

        // Assert - no message should appear in both results
        var allIds = messages1.Select(m => m.Id).Concat(messages2.Select(m => m.Id)).ToList();
        allIds.Should().OnlyHaveUniqueItems();
        allIds.Should().HaveCount(20);
    }

    [Fact]
    public async Task MarkCompletedAsync_WithWrongStatus_ShouldBeNoOp()
    {
        // Arrange - store a message (status = Pending, not InProgress)
        var message = CreateTestMessage("test-topic");
        await _store.StoreAsync(message);

        // Act - try to mark as completed without going through GetPendingAsync
        await _store.MarkCompletedAsync(message.Id);

        // Assert - message should still be Pending
        var pending = await _store.GetPendingAsync(10);
        pending.Should().HaveCount(1);
        pending[0].Id.Should().Be(message.Id);
        pending[0].Status.Should().Be(OutboxMessageStatus.InProgress);
    }

    [Fact]
    public async Task MarkFailedAsync_WithWrongStatus_ShouldBeNoOp()
    {
        // Arrange - store a message (status = Pending)
        var message = CreateTestMessage("test-topic");
        await _store.StoreAsync(message);

        // Act - try to mark as failed without going through GetPendingAsync
        await _store.MarkFailedAsync(message.Id, "should not apply");

        // Assert - message should still be Pending (attempts not incremented)
        var pending = await _store.GetPendingAsync(10);
        pending.Should().HaveCount(1);
        pending[0].Attempts.Should().Be(0);
    }

    [Fact]
    public async Task RecoverStuckMessagesAsync_ShouldRecoverStuckInProgressMessages()
    {
        // Arrange - store message and move to InProgress
        var message = CreateTestMessage("test-topic");
        await _store.StoreAsync(message);
        var pending = await _store.GetPendingAsync(10);
        pending.Should().HaveCount(1);
        pending[0].Status.Should().Be(OutboxMessageStatus.InProgress);

        // Act - recover messages stuck for more than 0 seconds (immediate)
        var recovered = await _store.RecoverStuckMessagesAsync(TimeSpan.Zero);

        // Assert
        recovered.Should().Be(1);

        // Message should be back to Pending
        var pendingAfter = await _store.GetPendingAsync(10);
        pendingAfter.Should().HaveCount(1);
        pendingAfter[0].Id.Should().Be(message.Id);
        pendingAfter[0].Status.Should().Be(OutboxMessageStatus.InProgress);
    }

    [Fact]
    public async Task RecoverStuckMessagesAsync_ShouldNotRecoverFreshMessages()
    {
        // Arrange - store message and move to InProgress
        var message = CreateTestMessage("test-topic");
        await _store.StoreAsync(message);
        await _store.GetPendingAsync(10);

        // Act - recover messages stuck for more than 1 hour (should find nothing)
        var recovered = await _store.RecoverStuckMessagesAsync(TimeSpan.FromHours(1));

        // Assert
        recovered.Should().Be(0);

        // Message should still be InProgress
        var pending = await _store.GetPendingAsync(10);
        pending.Should().BeEmpty();
    }

    [Fact]
    public async Task ConcurrentStoreAndProcess_ShouldHandleRaceCondition()
    {
        // Arrange - simulate concurrent store and process
        var store2 = new PostgresOutboxStore(
            _postgres.GetConnectionString(),
            Options.Create(new OutboxOptions { TableName = "outbox_messages", SchemaName = "public" }),
            LoggerFactory.Create(b => b.AddConsole()).CreateLogger<PostgresOutboxStore>());

        var processedByStore1 = new List<Guid>();
        var processedByStore2 = new List<Guid>();

        // Act - store messages concurrently from both stores
        var storeTasks = new List<Task>();
        for (int i = 0; i < 30; i++)
        {
            var store = i % 2 == 0 ? _store : store2;
            storeTasks.Add(store.StoreAsync(CreateTestMessage($"topic{i}")));
        }
        await Task.WhenAll(storeTasks);

        // Process concurrently - both stores try to get pending
        var processTasks = new List<Task>();
        for (int round = 0; round < 3; round++)
        {
            processTasks.Add(Task.Run(async () =>
            {
                var msgs = await _store.GetPendingAsync(10);
                foreach (var msg in msgs)
                {
                    processedByStore1.Add(msg.Id);
                    await _store.MarkCompletedAsync(msg.Id);
                }
            }));
            processTasks.Add(Task.Run(async () =>
            {
                var msgs = await store2.GetPendingAsync(10);
                foreach (var msg in msgs)
                {
                    processedByStore2.Add(msg.Id);
                    await store2.MarkCompletedAsync(msg.Id);
                }
            }));
        }
        await Task.WhenAll(processTasks);

        // Assert - no duplicates between stores
        var overlap = processedByStore1.Intersect(processedByStore2).ToList();
        overlap.Should().BeEmpty("no message should be processed by both stores");

        // All messages should eventually be processed
        var remaining = await _store.GetPendingAsync(100);
        remaining.Should().BeEmpty("all messages should be processed");
    }

    private static OutboxMessage CreateTestMessage(
        string topic,
        byte[]? key = null,
        Dictionary<string, byte[]>? headers = null,
        DateTimeOffset? deliverAfter = null,
        DateTimeOffset? createdAt = null,
        int? partition = null)
    {
        return new OutboxMessage
        {
            Destination = topic,
            Body = Encoding.UTF8.GetBytes($"test-value-{Guid.NewGuid()}"),
            Key = key,
            Headers = headers,
            DeliverAfter = deliverAfter,
            CreatedAt = createdAt ?? DateTimeOffset.UtcNow,
            Partition = partition
        };
    }

    private async Task<OutboxMessage> ReadStoredMessageAsync(Guid id)
    {
        await using var connection = new Npgsql.NpgsqlConnection(_postgres.GetConnectionString());
        await connection.OpenAsync();

        await using var cmd = new Npgsql.NpgsqlCommand(
            "SELECT id, destination, key, body, headers, created_at, deliver_after, attempts, status, last_error, partition FROM outbox_messages WHERE id = @id",
            connection);
        cmd.Parameters.AddWithValue("id", id);

        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();

        return new OutboxMessage
        {
            Id = reader.GetGuid(reader.GetOrdinal("id")),
            Destination = reader.GetString(reader.GetOrdinal("destination")),
            Key = reader.IsDBNull(reader.GetOrdinal("key"))
                ? null
                : (byte[])reader.GetValue(reader.GetOrdinal("key")),
            Body = (byte[])reader.GetValue(reader.GetOrdinal("body")),
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
