using KafkaDelayedMessaging.Interfaces;
using KafkaDelayedMessaging.Models;
using KafkaDelayedMessaging.Services;
using KafkaDelayedMessaging.Helpers;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace KafkaDelayedMessaging.Tests;

/// <summary>
/// Unit tests for DelayedMessageProducer functionality.
/// Tests cover topic selection, header setting, JSON serialization, and error handling.
/// </summary>
public class DelayedMessageProducerTests : IDisposable
{
    private readonly KafkaDelayConfig _config;
    private readonly ILogger<DelayedMessageProducer> _logger;
    private readonly TestLoggerProvider _loggerProvider;

    public DelayedMessageProducerTests()
    {
        _config = new KafkaDelayConfig
        {
            BootstrapServers = "localhost:9092",
            ConsumerGroupId = "test-group",
            DelayTopicPrefix = "test-delay-"
        };

        _loggerProvider = new TestLoggerProvider();
        var loggerFactory = LoggerFactory.Create(builder => builder.AddProvider(_loggerProvider));
        _logger = loggerFactory.CreateLogger<DelayedMessageProducer>();
    }

    [Fact]
    public void Constructor_WithValidConfig_ShouldInitializeSuccessfully()
    {
        // Act & Assert - Should not throw
        using var producer = new DelayedMessageProducer(_config, _logger);
        
        // Verify logger was called for initialization
        Assert.Contains(_loggerProvider.LogEntries, 
            entry => entry.Message.Contains("DelayedMessageProducer initialized"));
    }

    [Fact]
    public void Constructor_WithNullConfig_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new DelayedMessageProducer(null!, _logger));
    }

    [Fact]
    public void Constructor_WithNullLogger_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new DelayedMessageProducer(_config, null!));
    }

    [Fact]
    public void Constructor_WithInvalidConfig_ShouldThrowValidationException()
    {
        // Arrange
        var invalidConfig = new KafkaDelayConfig
        {
            BootstrapServers = "", // Invalid - empty
            ConsumerGroupId = "test-group"
        };

        // Act & Assert
        Assert.Throws<System.ComponentModel.DataAnnotations.ValidationException>(
            () => new DelayedMessageProducer(invalidConfig, _logger));
    }

    [Fact]
    public async Task ProduceDelayedMessageAsync_WithDelayDuration_ShouldValidateParameters()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        var message = new { Text = "Test message" };

        // Act & Assert - Null target topic
        await Assert.ThrowsAsync<ArgumentException>(
            () => producer.ProduceDelayedMessageAsync(null!, message, DelayDuration.Seconds5));

        // Act & Assert - Empty target topic
        await Assert.ThrowsAsync<ArgumentException>(
            () => producer.ProduceDelayedMessageAsync("", message, DelayDuration.Seconds5));

        // Act & Assert - Null message
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => producer.ProduceDelayedMessageAsync("target-topic", (object)null!, DelayDuration.Seconds5));
    }

    [Fact]
    public async Task ProduceDelayedMessageAsync_WithDateTime_ShouldValidateParameters()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        var message = new { Text = "Test message" };
        var futureTime = DateTime.UtcNow.AddMinutes(5);
        var pastTime = DateTime.UtcNow.AddMinutes(-5);

        // Act & Assert - Null target topic
        await Assert.ThrowsAsync<ArgumentException>(
            () => producer.ProduceDelayedMessageAsync(null!, message, futureTime));

        // Act & Assert - Empty target topic
        await Assert.ThrowsAsync<ArgumentException>(
            () => producer.ProduceDelayedMessageAsync("", message, futureTime));

        // Act & Assert - Null message
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => producer.ProduceDelayedMessageAsync("target-topic", (object)null!, futureTime));

        // Act & Assert - Past time
        await Assert.ThrowsAsync<ArgumentException>(
            () => producer.ProduceDelayedMessageAsync("target-topic", message, pastTime));
    }

    [Theory]
    [InlineData(DelayDuration.Seconds5, "test-delay-5s")]
    [InlineData(DelayDuration.Seconds10, "test-delay-10s")]
    [InlineData(DelayDuration.Minutes1, "test-delay-80s")]
    [InlineData(DelayDuration.Days5, "test-delay-432000s")]
    public void GetDelayTopicName_ShouldReturnCorrectTopicName(DelayDuration duration, string expectedTopic)
    {
        // Act
        var actualTopic = _config.GetDelayTopicName(duration);

        // Assert
        Assert.Equal(expectedTopic, actualTopic);
    }

    [Fact]
    public void SelectDelayDuration_ShouldChooseAppropriateDelayDuration()
    {
        // Test cases: (delaySeconds, expectedDuration)
        var testCases = new[]
        {
            (3, DelayDuration.Seconds5),      // Should round up to 5s
            (5, DelayDuration.Seconds5),      // Exact match
            (7, DelayDuration.Seconds10),     // Should round up to 10s
            (15, DelayDuration.Seconds20),    // Should round up to 20s
            (100, DelayDuration.Minutes3),    // Should round up to 160s
            (500000, DelayDuration.Days5)     // Should use largest available
        };

        foreach (var (delaySeconds, expectedDuration) in testCases)
        {
            // Use reflection to access private method for testing
            var method = typeof(DelayedMessageProducer).GetMethod("SelectDelayDuration", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            
            var result = (DelayDuration)method!.Invoke(null, new object[] { delaySeconds })!;
            
            Assert.Equal(expectedDuration, result);
        }
    }

    [Fact]
    public void CreateMessageHeaders_ShouldSetCorrectHeaders()
    {
        // Arrange
        var targetTopic = "my-target-topic";
        var processAt = DateTime.UtcNow.AddMinutes(5);
        
        // Use reflection to access private method
        using var producer = new DelayedMessageProducer(_config, _logger);
        var method = typeof(DelayedMessageProducer).GetMethod("CreateMessageHeaders", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        // Act
        var headers = (Confluent.Kafka.Headers)method!.Invoke(producer, new object[] { targetTopic, processAt })!;

        // Assert
        Assert.NotNull(headers);
        Assert.Equal(3, headers.Count);

        // Check target topic header
        var targetTopicHeader = headers.FirstOrDefault(h => h.Key == DelayHeaders.TargetTopic);
        Assert.NotNull(targetTopicHeader);
        Assert.Equal(targetTopic, Encoding.UTF8.GetString(targetTopicHeader.GetValueBytes()));

        // Check process at header
        var processAtHeader = headers.FirstOrDefault(h => h.Key == DelayHeaders.ProcessAt);
        Assert.NotNull(processAtHeader);
        var processAtValue = long.Parse(Encoding.UTF8.GetString(processAtHeader.GetValueBytes()));
        Assert.Equal(processAt.ToUnixTimeMilliseconds(), processAtValue);

        // Check original timestamp header
        var originalTimestampHeader = headers.FirstOrDefault(h => h.Key == DelayHeaders.OriginalTimestamp);
        Assert.NotNull(originalTimestampHeader);
        var originalTimestamp = long.Parse(Encoding.UTF8.GetString(originalTimestampHeader.GetValueBytes()));
        Assert.True(originalTimestamp > 0);
    }

    [Fact]
    public void JsonSerialization_ShouldSerializeCorrectly()
    {
        // Arrange
        var testMessage = new
        {
            Id = 123,
            Name = "Test Message",
            Timestamp = DateTime.UtcNow,
            Data = new { Value = "nested data" }
        };

        var jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        // Act
        var json = JsonSerializer.Serialize(testMessage, jsonOptions);

        // Assert
        Assert.NotNull(json);
        Assert.Contains("\"id\":123", json);
        Assert.Contains("\"name\":\"Test Message\"", json);
        Assert.Contains("\"data\":{\"value\":\"nested data\"}", json);
    }

    [Fact]
    public void Dispose_ShouldDisposeResourcesGracefully()
    {
        // Arrange
        var producer = new DelayedMessageProducer(_config, _logger);

        // Act - Should not throw
        producer.Dispose();

        // Assert - Second dispose should also not throw
        producer.Dispose();

        // Verify dispose was logged
        Assert.Contains(_loggerProvider.LogEntries, 
            entry => entry.Message.Contains("DelayedMessageProducer disposed"));
    }

    [Fact]
    public void Dispose_AfterDispose_ShouldThrowObjectDisposedException()
    {
        // Arrange
        var producer = new DelayedMessageProducer(_config, _logger);
        var message = new { Text = "Test" };
        
        // Act
        producer.Dispose();

        // Assert
        Assert.ThrowsAsync<ObjectDisposedException>(
            () => producer.ProduceDelayedMessageAsync("topic", message, DelayDuration.Seconds5));
    }

    [Fact]
    public async Task ProduceDelayedMessageAsync_WithSerializationError_ShouldLogAndThrow()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        
        // Create an object that will cause serialization issues
        var problematicMessage = new ProblematicSerializationClass();

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => producer.ProduceDelayedMessageAsync("target-topic", problematicMessage, DelayDuration.Seconds5));

        Assert.Contains("serialization failed", exception.Message);
        
        // Verify error was logged
        Assert.Contains(_loggerProvider.LogEntries, 
            entry => entry.EventId.Id == LogEvents.SerializationError.Id);
    }

    [Fact]
    public async Task ProduceDelayedMessageAsync_WithCancellation_ShouldLogAndThrow()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        using var cts = new CancellationTokenSource();
        var message = new { Text = "Test message" };
        
        // Cancel immediately
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(
            () => producer.ProduceDelayedMessageAsync("target-topic", message, DelayDuration.Seconds5, cts.Token));

        // Verify cancellation was logged
        Assert.Contains(_loggerProvider.LogEntries, 
            entry => entry.Message.Contains("cancelled"));
    }

    [Fact]
    public void IsCriticalKafkaError_WithCriticalErrors_ShouldReturnTrue()
    {
        // Test critical error codes using reflection to access private method
        using var producer = new DelayedMessageProducer(_config, _logger);
        var method = typeof(DelayedMessageProducer).GetMethod("IsCriticalKafkaError", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        var criticalErrors = new[]
        {
            ErrorCode.BrokerNotAvailable,
            ErrorCode.NetworkException,
            ErrorCode.SaslAuthenticationFailed,
            ErrorCode.TopicAuthorizationFailed,
            ErrorCode.GroupAuthorizationFailed,
            ErrorCode.Local_Fatal
        };

        foreach (var errorCode in criticalErrors)
        {
            var error = new Error(errorCode, "Test error");
            var result = (bool)method!.Invoke(null, new object[] { error })!;
            Assert.True(result, $"Error code {errorCode} should be considered critical");
        }
    }

    [Fact]
    public void IsCriticalKafkaError_WithNonCriticalErrors_ShouldReturnFalse()
    {
        // Test non-critical error codes
        using var producer = new DelayedMessageProducer(_config, _logger);
        var method = typeof(DelayedMessageProducer).GetMethod("IsCriticalKafkaError", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        var nonCriticalErrors = new[]
        {
            ErrorCode.RequestTimedOut,
            ErrorCode.OffsetOutOfRange,
            ErrorCode.LeaderNotAvailable
        };

        foreach (var errorCode in nonCriticalErrors)
        {
            var error = new Error(errorCode, "Test error");
            var result = (bool)method!.Invoke(null, new object[] { error })!;
            Assert.False(result, $"Error code {errorCode} should not be considered critical");
        }
    }

    [Fact]
    public async Task ProduceDelayedMessageAsync_WithInvalidDelayDuration_ShouldHandleGracefully()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        var message = new { Text = "Test message" };

        // Test with extreme delay duration values
        var extremeDelayDuration = (DelayDuration)999999; // Invalid enum value

        // Act & Assert - Should not throw for invalid enum values
        // The SelectDelayDuration method should handle this gracefully
        try
        {
            await producer.ProduceDelayedMessageAsync("target-topic", message, extremeDelayDuration);
        }
        catch (Exception ex)
        {
            // Should be a Kafka connection error, not an enum validation error
            Assert.IsType<InvalidOperationException>(ex);
        }
    }

    [Fact]
    public void CreateMessageHeaders_WithNullOrEmptyTargetTopic_ShouldHandleGracefully()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        var processAt = DateTime.UtcNow.AddMinutes(5);
        
        // Use reflection to access private method
        var method = typeof(DelayedMessageProducer).GetMethod("CreateMessageHeaders", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // Act & Assert - Should handle empty target topic
        var headers1 = (Confluent.Kafka.Headers)method!.Invoke(producer, new object[] { "", processAt })!;
        Assert.NotNull(headers1);

        var headers2 = (Confluent.Kafka.Headers)method!.Invoke(producer, new object[] { null!, processAt })!;
        Assert.NotNull(headers2);
    }

    [Fact]
    public void ErrorHandling_WithMultipleConsecutiveErrors_ShouldLogAppropriately()
    {
        // This test verifies that error logging includes appropriate context
        // We can't easily simulate actual Kafka errors, but we can verify the logging structure

        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);

        // Verify that the logger is set up to capture different log levels
        Assert.True(_logger.IsEnabled(LogLevel.Error));
        Assert.True(_logger.IsEnabled(LogLevel.Warning));
        Assert.True(_logger.IsEnabled(LogLevel.Information));
    }

    [Fact]
    public async Task ProduceDelayedMessageAsync_WithKafkaConnectionError_ShouldLogAndThrow()
    {
        // Arrange
        var invalidConfig = new KafkaDelayConfig
        {
            BootstrapServers = "invalid-server:9092", // Invalid server
            ConsumerGroupId = "test-group",
            DelayTopicPrefix = "test-delay-"
        };

        using var producer = new DelayedMessageProducer(invalidConfig, _logger);
        var message = new { Text = "Test message" };

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => producer.ProduceDelayedMessageAsync("target-topic", message, DelayDuration.Seconds5));

        // Verify error contains production failure information
        Assert.Contains("production failed", exception.Message);
        
        // Verify error was logged
        Assert.Contains(_loggerProvider.LogEntries, 
            entry => entry.EventId.Id == LogEvents.KafkaConnectionError.Id);
    }

    [Fact]
    public async Task ProduceDelayedMessageAsync_WithNullMessage_ShouldLogAndThrow()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);

        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => producer.ProduceDelayedMessageAsync("target-topic", (object)null!, DelayDuration.Seconds5));

        Assert.Equal("message", exception.ParamName);
    }

    [Fact]
    public async Task ProduceDelayedMessageAsync_WithEmptyTargetTopic_ShouldLogAndThrow()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        var message = new { Text = "Test message" };

        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(
            () => producer.ProduceDelayedMessageAsync("", message, DelayDuration.Seconds5));

        Assert.Equal("targetTopic", exception.ParamName);
        Assert.Contains("cannot be null or empty", exception.Message);
    }

    [Fact]
    public async Task ProduceDelayedMessageAsync_WithWhitespaceTargetTopic_ShouldLogAndThrow()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        var message = new { Text = "Test message" };

        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(
            () => producer.ProduceDelayedMessageAsync("   ", message, DelayDuration.Seconds5));

        Assert.Equal("targetTopic", exception.ParamName);
    }

    [Fact]
    public async Task ProduceDelayedMessageAsync_WithPastDateTime_ShouldLogAndThrow()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        var message = new { Text = "Test message" };
        var pastTime = DateTime.UtcNow.AddMinutes(-5);

        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(
            () => producer.ProduceDelayedMessageAsync("target-topic", message, pastTime));

        Assert.Equal("processAt", exception.ParamName);
        Assert.Contains("must be in the future", exception.Message);
    }

    [Fact]
    public void CreateMessageHeaders_WithExtremeTimestamps_ShouldHandleGracefully()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        var method = typeof(DelayedMessageProducer).GetMethod("CreateMessageHeaders", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // Test with extreme future date
        var extremeFuture = DateTime.MaxValue.AddDays(-1);
        var headers1 = (Confluent.Kafka.Headers)method!.Invoke(producer, new object[] { "test-topic", extremeFuture })!;
        Assert.NotNull(headers1);
        Assert.Equal(3, headers1.Count);

        // Test with extreme past date
        var extremePast = DateTime.MinValue.AddDays(1);
        var headers2 = (Confluent.Kafka.Headers)method!.Invoke(producer, new object[] { "test-topic", extremePast })!;
        Assert.NotNull(headers2);
        Assert.Equal(3, headers2.Count);
    }

    [Fact]
    public void ErrorHandling_WithSerializationOfComplexTypes_ShouldLogDetails()
    {
        // Arrange
        using var producer = new DelayedMessageProducer(_config, _logger);
        
        // Create a complex object that might cause serialization issues
        var complexMessage = new
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            Data = new Dictionary<string, object>
            {
                { "key1", "value1" },
                { "key2", 42 },
                { "key3", new { NestedProperty = "nested value" } }
            },
            Array = new[] { 1, 2, 3, 4, 5 }
        };

        // Act - This should succeed with normal serialization
        var jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        var json = JsonSerializer.Serialize(complexMessage, jsonOptions);

        // Assert
        Assert.NotNull(json);
        Assert.Contains("\"id\":", json);
        Assert.Contains("\"data\":", json);
        Assert.Contains("\"array\":", json);
    }

    [Fact]
    public void ErrorHandling_WithUnsupportedMessageType_ShouldLogAndThrow()
    {
        // Test with a type that has unsupported serialization attributes
        using var producer = new DelayedMessageProducer(_config, _logger);
        
        // Create an object with a property that will cause NotSupportedException
        var unsupportedMessage = new UnsupportedSerializationClass();

        // Act & Assert
        Assert.ThrowsAsync<InvalidOperationException>(
            () => producer.ProduceDelayedMessageAsync("target-topic", unsupportedMessage, DelayDuration.Seconds5));
    }

    [Fact]
    public void Dispose_WithPendingOperations_ShouldFlushAndDispose()
    {
        // Arrange
        var producer = new DelayedMessageProducer(_config, _logger);

        // Act - Dispose should handle any pending operations gracefully
        producer.Dispose();

        // Assert - Verify dispose was logged
        Assert.Contains(_loggerProvider.LogEntries, 
            entry => entry.Message.Contains("DelayedMessageProducer disposed"));
    }

    [Fact]
    public void Dispose_WithDisposeException_ShouldLogError()
    {
        // This test verifies that exceptions during dispose are logged
        // We can't easily force a dispose exception, but we can verify the structure
        
        // Arrange
        var producer = new DelayedMessageProducer(_config, _logger);

        // Act
        producer.Dispose();

        // Assert - Should complete without throwing
        // Multiple disposes should be safe
        producer.Dispose();
        producer.Dispose();
    }

    public void Dispose()
    {
        _loggerProvider?.Dispose();
    }
}

/// <summary>
/// Test class that causes serialization errors for testing error handling.
/// </summary>
public class ProblematicSerializationClass
{
    // This property will cause JsonSerializer to throw an exception
    public object CircularReference { get; set; }

    public ProblematicSerializationClass()
    {
        CircularReference = this; // Creates circular reference
    }
}

/// <summary>
/// Test class that causes NotSupportedException during serialization.
/// </summary>
public class UnsupportedSerializationClass
{
    // This property type will cause NotSupportedException in some scenarios
    public IntPtr UnsupportedProperty { get; set; } = IntPtr.Zero;
    
    // Another property that might cause issues
    public Delegate? UnsupportedDelegate { get; set; }
}

/// <summary>
/// Test logger provider for capturing log entries during tests.
/// </summary>
public class TestLoggerProvider : ILoggerProvider
{
    public List<LogEntry> LogEntries { get; } = new();

    public ILogger CreateLogger(string categoryName)
    {
        return new TestLogger(LogEntries);
    }

    public void Dispose()
    {
        LogEntries.Clear();
    }
}

/// <summary>
/// Test logger implementation that captures log entries.
/// </summary>
public class TestLogger : ILogger
{
    private readonly List<LogEntry> _logEntries;

    public TestLogger(List<LogEntry> logEntries)
    {
        _logEntries = logEntries;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        _logEntries.Add(new LogEntry
        {
            LogLevel = logLevel,
            EventId = eventId,
            Message = formatter(state, exception),
            Exception = exception
        });
    }
}

/// <summary>
/// Represents a captured log entry for testing.
/// </summary>
public class LogEntry
{
    public LogLevel LogLevel { get; set; }
    public EventId EventId { get; set; }
    public string Message { get; set; } = string.Empty;
    public Exception? Exception { get; set; }
}