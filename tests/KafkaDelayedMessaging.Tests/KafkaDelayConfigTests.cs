using System.ComponentModel.DataAnnotations;
using KafkaDelayedMessaging.Helpers;
using KafkaDelayedMessaging.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace KafkaDelayedMessaging.Tests;

public class KafkaDelayConfigTests
{
    [Fact]
    public void Validate_WithValidConfiguration_ShouldNotThrow()
    {
        // Arrange
        var config = new KafkaDelayConfig
        {
            BootstrapServers = "localhost:9092",
            ConsumerGroupId = "test-group"
        };

        // Act & Assert
        config.Validate(); // Should not throw
    }

    [Fact]
    public void Validate_WithEmptyBootstrapServers_ShouldThrowValidationException()
    {
        // Arrange
        var config = new KafkaDelayConfig
        {
            BootstrapServers = "",
            ConsumerGroupId = "test-group"
        };

        // Act & Assert
        var exception = Assert.Throws<ValidationException>(() => config.Validate());
        Assert.Contains("BootstrapServers", exception.Message);
    }

    [Fact]
    public void Validate_WithEmptyConsumerGroupId_ShouldThrowValidationException()
    {
        // Arrange
        var config = new KafkaDelayConfig
        {
            BootstrapServers = "localhost:9092",
            ConsumerGroupId = ""
        };

        // Act & Assert
        var exception = Assert.Throws<ValidationException>(() => config.Validate());
        Assert.Contains("ConsumerGroupId", exception.Message);
    }

    [Fact]
    public void Validate_WithNegativeRetries_ShouldThrowValidationException()
    {
        // Arrange
        var config = new KafkaDelayConfig
        {
            BootstrapServers = "localhost:9092",
            ConsumerGroupId = "test-group",
            Retries = -1
        };

        // Act & Assert
        var exception = Assert.Throws<ValidationException>(() => config.Validate());
        Assert.Contains("Retries", exception.Message);
    }

    [Fact]
    public void GetDelayTopicName_WithValidDelayDuration_ShouldReturnCorrectTopicName()
    {
        // Arrange
        var config = new KafkaDelayConfig { DelayTopicPrefix = "delay-" };

        // Act
        var topicName = config.GetDelayTopicName(DelayDuration.Seconds5);

        // Assert
        Assert.Equal("delay-5s", topicName);
    }

    [Fact]
    public void GetConsumerGroupName_WithValidDelayDuration_ShouldReturnCorrectGroupName()
    {
        // Arrange
        var config = new KafkaDelayConfig 
        { 
            ConsumerGroupId = "test-group",
            DelayTopicPrefix = "delay-"
        };

        // Act
        var groupName = config.GetConsumerGroupName(DelayDuration.Seconds10);

        // Assert
        Assert.Equal("test-group-delay-10s", groupName);
    }

    [Theory]
    [InlineData(DelayDuration.Seconds5, "delay-5s")]
    [InlineData(DelayDuration.Minutes1, "delay-80s")]
    [InlineData(DelayDuration.Hours1, "delay-5120s")]
    [InlineData(DelayDuration.Days5, "delay-432000s")]
    public void GetDelayTopicName_WithVariousDelayDurations_ShouldReturnCorrectNames(
        DelayDuration duration, string expectedTopicName)
    {
        // Arrange
        var config = new KafkaDelayConfig { DelayTopicPrefix = "delay-" };

        // Act
        var topicName = config.GetDelayTopicName(duration);

        // Assert
        Assert.Equal(expectedTopicName, topicName);
    }
}

public class ConfigurationHelperTests
{
    [Fact]
    public void CreateDefaultConfig_ShouldReturnValidConfiguration()
    {
        // Act
        var config = ConfigurationHelper.CreateDefaultConfig();

        // Assert
        Assert.NotNull(config);
        Assert.Equal("localhost:9092", config.BootstrapServers);
        Assert.Equal("kafka-delay-consumer", config.ConsumerGroupId);
        config.Validate(); // Should not throw
    }

    [Fact]
    public void CreateLoggerFactory_ShouldReturnValidLoggerFactory()
    {
        // Act
        using var loggerFactory = ConfigurationHelper.CreateLoggerFactory();
        var logger = loggerFactory.CreateLogger<ConfigurationHelperTests>();

        // Assert
        Assert.NotNull(loggerFactory);
        Assert.NotNull(logger);
    }

    [Fact]
    public void TryValidateConfig_WithValidConfig_ShouldReturnTrue()
    {
        // Arrange
        var config = ConfigurationHelper.CreateDefaultConfig();
        using var loggerFactory = ConfigurationHelper.CreateLoggerFactory();
        var logger = loggerFactory.CreateLogger<ConfigurationHelperTests>();

        // Act
        var result = ConfigurationHelper.TryValidateConfig(config, logger);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void TryValidateConfig_WithInvalidConfig_ShouldReturnFalse()
    {
        // Arrange
        var config = new KafkaDelayConfig(); // Invalid - missing required fields
        using var loggerFactory = ConfigurationHelper.CreateLoggerFactory();
        var logger = loggerFactory.CreateLogger<ConfigurationHelperTests>();

        // Act
        var result = ConfigurationHelper.TryValidateConfig(config, logger);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void LoadAndValidateConfig_WithValidConfiguration_ShouldReturnConfig()
    {
        // Arrange
        var configData = new Dictionary<string, string?>
        {
            ["KafkaDelay:BootstrapServers"] = "localhost:9092",
            ["KafkaDelay:ConsumerGroupId"] = "test-group"
        };
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        using var loggerFactory = ConfigurationHelper.CreateLoggerFactory();
        var logger = loggerFactory.CreateLogger<ConfigurationHelperTests>();

        // Act
        var config = ConfigurationHelper.LoadAndValidateConfig(configuration, logger: logger);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("localhost:9092", config.BootstrapServers);
        Assert.Equal("test-group", config.ConsumerGroupId);
    }
}

public class LogEventsTests
{
    [Fact]
    public void LogEvents_ShouldHaveUniqueEventIds()
    {
        // Arrange
        var eventIds = new[]
        {
            LogEvents.MessageProduced.Id,
            LogEvents.MessageConsumed.Id,
            LogEvents.MessageForwarded.Id,
            LogEvents.ConsumerStarted.Id,
            LogEvents.ConsumerStopped.Id,
            LogEvents.ConsumerSleeping.Id,
            LogEvents.ConfigurationValidated.Id,
            LogEvents.HeaderMalformed.Id,
            LogEvents.MessageSkipped.Id,
            LogEvents.RetryAttempt.Id,
            LogEvents.ConsumerLag.Id,
            LogEvents.ProcessingError.Id,
            LogEvents.KafkaConnectionError.Id,
            LogEvents.TargetTopicUnreachable.Id,
            LogEvents.SerializationError.Id,
            LogEvents.DeserializationError.Id,
            LogEvents.ConfigurationError.Id,
            LogEvents.CriticalError.Id,
            LogEvents.RebalanceError.Id,
            LogEvents.OffsetCommitError.Id
        };

        // Act & Assert
        var uniqueIds = eventIds.Distinct().ToArray();
        Assert.Equal(eventIds.Length, uniqueIds.Length);
    }

    [Fact]
    public void LogEvents_ShouldHaveCorrectEventIdRanges()
    {
        // Assert informational events (1xxx)
        Assert.InRange(LogEvents.MessageProduced.Id, 1000, 1999);
        Assert.InRange(LogEvents.MessageConsumed.Id, 1000, 1999);
        Assert.InRange(LogEvents.MessageForwarded.Id, 1000, 1999);

        // Assert warning events (2xxx)
        Assert.InRange(LogEvents.HeaderMalformed.Id, 2000, 2999);
        Assert.InRange(LogEvents.MessageSkipped.Id, 2000, 2999);

        // Assert error events (3xxx)
        Assert.InRange(LogEvents.ProcessingError.Id, 3000, 3999);
        Assert.InRange(LogEvents.CriticalError.Id, 3000, 3999);
    }
}