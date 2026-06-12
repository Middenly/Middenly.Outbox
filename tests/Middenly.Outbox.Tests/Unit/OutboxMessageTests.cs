using System.Text;
using Middenly.Outbox.Abstractions;
using FluentAssertions;
using Xunit;

namespace Middenly.Outbox.Tests.Unit;

public class OutboxMessageTests
{
    [Fact]
    public void NewMessage_ShouldHaveDefaultValues()
    {
        // Arrange & Act
        var beforeCreate = DateTimeOffset.UtcNow;
        var message = new OutboxMessage
        {
            Destination = "test-topic",
            Body = Encoding.UTF8.GetBytes("test")
        };
        var afterCreate = DateTimeOffset.UtcNow;

        // Assert
        message.Id.Should().NotBeEmpty();
        message.Destination.Should().Be("test-topic");
        message.Status.Should().Be(OutboxMessageStatus.Pending);
        message.Attempts.Should().Be(0);
        message.CreatedAt.Should().BeOnOrAfter(beforeCreate);
        message.CreatedAt.Should().BeOnOrBefore(afterCreate);
        message.DeliverAfter.Should().BeNull();
        message.LastError.Should().BeNull();
        message.Key.Should().BeNull();
        message.Headers.Should().BeNull();
        message.Partition.Should().BeNull();
    }

    [Fact]
    public void NewMessage_ShouldHaveUniqueId()
    {
        // Arrange & Act
        var message1 = new OutboxMessage { Destination = "test", Body = Encoding.UTF8.GetBytes("test") };
        var message2 = new OutboxMessage { Destination = "test", Body = Encoding.UTF8.GetBytes("test") };

        // Assert
        message1.Id.Should().NotBe(message2.Id);
    }

    [Fact]
    public void Message_ShouldAllowSettingStatus()
    {
        // Arrange
        var message = new OutboxMessage
        {
            Destination = "test-topic",
            Body = Encoding.UTF8.GetBytes("test")
        };

        // Act
        message.Status = OutboxMessageStatus.InProgress;

        // Assert
        message.Status.Should().Be(OutboxMessageStatus.InProgress);
    }

    [Fact]
    public void Message_ShouldAllowIncrementingAttempts()
    {
        // Arrange
        var message = new OutboxMessage
        {
            Destination = "test-topic",
            Body = Encoding.UTF8.GetBytes("test")
        };

        // Act
        message.Attempts++;
        message.Attempts++;

        // Assert
        message.Attempts.Should().Be(2);
    }

    [Fact]
    public void Message_ShouldAllowSettingLastError()
    {
        // Arrange
        var message = new OutboxMessage
        {
            Destination = "test-topic",
            Body = Encoding.UTF8.GetBytes("test")
        };

        // Act
        message.LastError = "Connection timeout";

        // Assert
        message.LastError.Should().Be("Connection timeout");
    }

    [Fact]
    public void Message_WithDeliverAfter_ShouldStoreDelayedDeliveryTime()
    {
        // Arrange
        var deliverAfter = DateTimeOffset.UtcNow.AddHours(1);

        // Act
        var message = new OutboxMessage
        {
            Destination = "test-topic",
            Body = Encoding.UTF8.GetBytes("test"),
            DeliverAfter = deliverAfter
        };

        // Assert
        message.DeliverAfter.Should().Be(deliverAfter);
    }

    [Fact]
    public void Message_WithKey_ShouldStoreKey()
    {
        // Arrange
        var key = Encoding.UTF8.GetBytes("message-key");

        // Act
        var message = new OutboxMessage
        {
            Destination = "test-topic",
            Body = Encoding.UTF8.GetBytes("test"),
            Key = key
        };

        // Assert
        message.Key.Should().BeEquivalentTo(key);
    }

    [Fact]
    public void Message_WithHeaders_ShouldStoreHeaders()
    {
        // Arrange
        var headers = new Dictionary<string, byte[]>
        {
            ["header1"] = Encoding.UTF8.GetBytes("value1"),
            ["header2"] = Encoding.UTF8.GetBytes("value2")
        };

        // Act
        var message = new OutboxMessage
        {
            Destination = "test-topic",
            Body = Encoding.UTF8.GetBytes("test"),
            Headers = headers
        };

        // Assert
        message.Headers.Should().BeEquivalentTo(headers);
    }

    [Fact]
    public void Message_WithPartition_ShouldStorePartition()
    {
        // Arrange & Act
        var message = new OutboxMessage
        {
            Destination = "test-topic",
            Body = Encoding.UTF8.GetBytes("test"),
            Partition = 5
        };

        // Assert
        message.Partition.Should().Be(5);
    }
}

public class OutboxMessageStatusTests
{
    [Fact]
    public void Status_ShouldHaveCorrectValues()
    {
        // Assert
        ((int)OutboxMessageStatus.Pending).Should().Be(0);
        ((int)OutboxMessageStatus.InProgress).Should().Be(1);
        ((int)OutboxMessageStatus.Completed).Should().Be(2);
        ((int)OutboxMessageStatus.Failed).Should().Be(3);
        ((int)OutboxMessageStatus.DeadLettered).Should().Be(4);
    }

    [Fact]
    public void Status_ShouldHaveAllExpectedMembers()
    {
        // Arrange
        var expectedStatuses = new[]
        {
            OutboxMessageStatus.Pending,
            OutboxMessageStatus.InProgress,
            OutboxMessageStatus.Completed,
            OutboxMessageStatus.Failed,
            OutboxMessageStatus.DeadLettered
        };

        // Act
        var actualStatuses = Enum.GetValues<OutboxMessageStatus>();

        // Assert
        actualStatuses.Should().BeEquivalentTo(expectedStatuses);
    }
}
