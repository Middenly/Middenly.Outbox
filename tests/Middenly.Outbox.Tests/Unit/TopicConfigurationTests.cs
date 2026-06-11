using Middenly.Outbox.Configuration;
using FluentAssertions;
using Xunit;

namespace Middenly.Outbox.Tests.Unit;

public class TopicOptionsTests
{
    [Fact]
    public void TopicOptions_Defaults_ShouldBeNull()
    {
        // Arrange & Act
        var options = new TopicOptions();

        // Assert
        options.Ordered.Should().BeNull();
        options.MaxAttempts.Should().BeNull();
    }

    [Fact]
    public void TopicOptions_ShouldAllowSettingOrdered()
    {
        // Arrange & Act
        var options = new TopicOptions { Ordered = true };

        // Assert
        options.Ordered.Should().BeTrue();
    }

    [Fact]
    public void TopicOptions_ShouldAllowSettingMaxAttempts()
    {
        // Arrange & Act
        var options = new TopicOptions { MaxAttempts = 10 };

        // Assert
        options.MaxAttempts.Should().Be(10);
    }
}

public class OutboxOptionsTopicTests
{
    [Fact]
    public void GetTopicOptions_WithConfiguredTopic_ShouldReturnTopicOptions()
    {
        // Arrange
        var options = new OutboxOptions();
        options.Topics["payment-events"] = new TopicOptions
        {
            Ordered = true,
            MaxAttempts = 10
        };

        // Act
        var result = options.GetTopicOptions("payment-events");

        // Assert
        result.Ordered.Should().BeTrue();
        result.MaxAttempts.Should().Be(10);
    }

    [Fact]
    public void GetTopicOptions_WithUnconfiguredTopic_ShouldReturnDefault()
    {
        // Arrange
        var options = new OutboxOptions();
        options.DefaultTopic = new TopicOptions
        {
            MaxAttempts = 3
        };

        // Act
        var result = options.GetTopicOptions("unknown-topic");

        // Assert
        result.MaxAttempts.Should().Be(3);
    }

    [Fact]
    public void GetTopicOptions_WithPartialConfig_ShouldMergeWithDefault()
    {
        // Arrange
        var options = new OutboxOptions();
        options.DefaultTopic = new TopicOptions
        {
            MaxAttempts = 5
        };
        options.Topics["payment-events"] = new TopicOptions
        {
            Ordered = true
        };

        // Act
        var result = options.GetTopicOptions("payment-events");

        // Assert
        result.Ordered.Should().BeTrue();
        result.MaxAttempts.Should().Be(5); // from default
    }

    [Fact]
    public void GetTopicOptions_TopicOverridesDefault_ShouldUseTopicValue()
    {
        // Arrange
        var options = new OutboxOptions();
        options.DefaultTopic = new TopicOptions
        {
            MaxAttempts = 5
        };
        options.Topics["payment-events"] = new TopicOptions
        {
            MaxAttempts = 20
        };

        // Act
        var result = options.GetTopicOptions("payment-events");

        // Assert
        result.MaxAttempts.Should().Be(20);
    }

    [Fact]
    public void GetTopicOptions_CaseInsensitive_ShouldMatch()
    {
        // Arrange
        var options = new OutboxOptions();
        options.Topics["Payment-Events"] = new TopicOptions { Ordered = true };

        // Act
        var result = options.GetTopicOptions("payment-events");

        // Assert
        result.Ordered.Should().BeTrue();
    }

    [Fact]
    public void GetTopicOptions_NoTopicsConfigured_ShouldReturnDefaults()
    {
        // Arrange
        var options = new OutboxOptions();

        // Act
        var result = options.GetTopicOptions("any-topic");

        // Assert
        result.Ordered.Should().BeNull();
        result.MaxAttempts.Should().BeNull();
    }
}
