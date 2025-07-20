using Confluent.Kafka;
using KafkaDelayedMessaging.Interfaces;
using KafkaDelayedMessaging.Models;
using KafkaDelayedMessaging.Helpers;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.Json;
using System.Diagnostics;

namespace KafkaDelayedMessaging.Services;

/// <summary>
/// Implementation of IDelayedMessageProducer that produces messages to Kafka delay topics.
/// Handles topic selection, header setting, and JSON serialization of message payloads.
/// </summary>
public class DelayedMessageProducer : IDelayedMessageProducer
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaDelayConfig _config;
    private readonly ILogger<DelayedMessageProducer> _logger;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly PerformanceTracker _performanceTracker;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of DelayedMessageProducer.
    /// </summary>
    /// <param name="config">Kafka delay configuration</param>
    /// <param name="logger">Logger instance</param>
    public DelayedMessageProducer(KafkaDelayConfig config, ILogger<DelayedMessageProducer> logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _performanceTracker = new PerformanceTracker();
        
        // Validate configuration
        _config.Validate();

        // Configure JSON serialization options
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        // Create Kafka producer with configuration
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _config.BootstrapServers,
            Acks = Enum.Parse<Acks>(_config.Acks, true),
            MessageSendMaxRetries = _config.Retries,
            EnableIdempotence = true,
            MessageTimeoutMs = _config.MessageTimeoutMs,
            RequestTimeoutMs = _config.RequestTimeoutMs
        };

        _producer = new ProducerBuilder<string, string>(producerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka producer error: {Error}", e.Reason))
            .Build();

        _logger.LogInformation("DelayedMessageProducer initialized with bootstrap servers: {BootstrapServers}", 
            _config.BootstrapServers);
    }

    /// <inheritdoc />
    public async Task ProduceDelayedMessageAsync<T>(
        string targetTopic, 
        T message, 
        DelayDuration delayDuration, 
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(targetTopic))
            throw new ArgumentException("Target topic cannot be null or empty", nameof(targetTopic));
        
        if (message == null)
            throw new ArgumentNullException(nameof(message));

        // Calculate processing timestamp based on delay duration
        var processAt = DateTime.UtcNow.AddSeconds((int)delayDuration);
        
        await ProduceDelayedMessageInternalAsync(targetTopic, message, delayDuration, processAt, cancellationToken);
    }

    /// <inheritdoc />
    public async Task ProduceDelayedMessageAsync<T>(
        string targetTopic, 
        T message, 
        DateTime processAt, 
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(targetTopic))
            throw new ArgumentException("Target topic cannot be null or empty", nameof(targetTopic));
        
        if (message == null)
            throw new ArgumentNullException(nameof(message));

        if (processAt <= DateTime.UtcNow)
            throw new ArgumentException("Process time must be in the future", nameof(processAt));

        // Find the appropriate DelayDuration for the given processAt time
        var delaySeconds = (int)(processAt - DateTime.UtcNow).TotalSeconds;
        var delayDuration = SelectDelayDuration(delaySeconds);
        
        await ProduceDelayedMessageInternalAsync(targetTopic, message, delayDuration, processAt, cancellationToken);
    }

    /// <summary>
    /// Internal method that handles the actual message production logic.
    /// </summary>
    private async Task ProduceDelayedMessageInternalAsync<T>(
        string targetTopic,
        T message,
        DelayDuration delayDuration,
        DateTime processAt,
        CancellationToken cancellationToken)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DelayedMessageProducer));

        var delayTopic = string.Empty;
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            // Select appropriate delay topic
            delayTopic = _config.GetDelayTopicName(delayDuration);
            
            _logger.LogDebug("Producing delayed message to topic {DelayTopic} for target {TargetTopic}, delay duration {DelayDuration}",
                delayTopic, targetTopic, delayDuration);
            
            // Serialize message payload to JSON
            string jsonPayload;
            try
            {
                jsonPayload = JsonSerializer.Serialize(message, _jsonOptions);
            }
            catch (JsonException ex)
            {
                _logger.LogError(LogEvents.SerializationError, ex,
                    "Failed to serialize message payload for target topic {TargetTopic}: {Error}",
                    targetTopic, ex.Message);
                throw new InvalidOperationException($"Message serialization failed for target topic '{targetTopic}'", ex);
            }
            catch (NotSupportedException ex)
            {
                _logger.LogError(LogEvents.SerializationError, ex,
                    "Message type {MessageType} is not supported for serialization to target topic {TargetTopic}",
                    typeof(T).Name, targetTopic);
                throw new InvalidOperationException($"Message type '{typeof(T).Name}' is not supported for serialization", ex);
            }
            
            // Create message with headers
            Headers messageHeaders;
            try
            {
                messageHeaders = CreateMessageHeaders(targetTopic, processAt);
            }
            catch (Exception ex)
            {
                _logger.LogError(LogEvents.ProcessingError, ex,
                    "Failed to create message headers for target topic {TargetTopic}: {Error}",
                    targetTopic, ex.Message);
                throw new InvalidOperationException($"Failed to create message headers for target topic '{targetTopic}'", ex);
            }

            var kafkaMessage = new Message<string, string>
            {
                Key = Guid.NewGuid().ToString(), // Use random key for even distribution
                Value = jsonPayload,
                Headers = messageHeaders
            };

            // Produce message to delay topic with retry logic
            DeliveryResult<string, string> deliveryResult;
            try
            {
                deliveryResult = await _producer.ProduceAsync(delayTopic, kafkaMessage, cancellationToken);
            }
            catch (ProduceException<string, string> ex)
            {
                _logger.LogError(LogEvents.KafkaConnectionError, ex,
                    "Failed to produce delayed message to topic {DelayTopic} for target {TargetTopic}: {Error}",
                    delayTopic, targetTopic, ex.Error.Reason);
                
                // Check if this is a critical error that should fail fast
                if (IsCriticalKafkaError(ex.Error))
                {
                    _logger.LogCritical(LogEvents.CriticalError,
                        "Critical Kafka error detected, failing fast: {Error}", ex.Error.Reason);
                    throw new InvalidOperationException($"Critical Kafka error: {ex.Error.Reason}", ex);
                }
                
                throw new InvalidOperationException($"Message production failed for delay topic '{delayTopic}': {ex.Error.Reason}", ex);
            }
            catch (KafkaException ex)
            {
                _logger.LogError(LogEvents.KafkaConnectionError, ex,
                    "Kafka error occurred while producing to topic {DelayTopic}: {Error}",
                    delayTopic, ex.Error.Reason);
                throw new InvalidOperationException($"Kafka error: {ex.Error.Reason}", ex);
            }
            
            stopwatch.Stop();
            
            // Record performance metrics
            _performanceTracker.RecordMessageProduced();
            
            // Log performance metrics
            _logger.LogDebug(LogEvents.LatencyMeasurement,
                "Message production latency: {LatencyMs}ms for topic {DelayTopic}",
                stopwatch.ElapsedMilliseconds, delayTopic);
            
            _logger.LogInformation(LogEvents.MessageProduced, 
                "Delayed message produced to topic {DelayTopic} for target {TargetTopic}, " +
                "process at {ProcessAt}, partition {Partition}, offset {Offset}, latency {LatencyMs}ms",
                delayTopic, targetTopic, processAt, deliveryResult.Partition.Value, deliveryResult.Offset.Value, stopwatch.ElapsedMilliseconds);
        }
        catch (OperationCanceledException)
        {
            _performanceTracker.RecordError();
            _logger.LogWarning("Delayed message production was cancelled for topic {DelayTopic}", delayTopic);
            throw;
        }
        catch (Exception ex) when (!(ex is InvalidOperationException))
        {
            _performanceTracker.RecordError();
            _logger.LogError(LogEvents.CriticalError, ex,
                "Unexpected error occurred during message production to topic {DelayTopic}: {Error}",
                delayTopic, ex.Message);
            throw new InvalidOperationException($"Unexpected error during message production: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Creates Kafka message headers with delay metadata.
    /// </summary>
    private Headers CreateMessageHeaders(string targetTopic, DateTime processAt)
    {
        var headers = new Headers
        {
            { DelayHeaders.TargetTopic, Encoding.UTF8.GetBytes(targetTopic ?? string.Empty) },
            { DelayHeaders.ProcessAt, Encoding.UTF8.GetBytes(processAt.ToUnixTimeMilliseconds().ToString()) },
            { DelayHeaders.OriginalTimestamp, Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString()) }
        };

        return headers;
    }

    /// <summary>
    /// Selects the most appropriate DelayDuration for a given delay in seconds.
    /// Chooses the smallest duration that is greater than or equal to the requested delay.
    /// </summary>
    private static DelayDuration SelectDelayDuration(int delaySeconds)
    {
        // Get all DelayDuration values and sort them
        var durations = Enum.GetValues<DelayDuration>()
            .OrderBy(d => (int)d)
            .ToArray();

        // Find the first duration that is >= the requested delay
        foreach (var duration in durations)
        {
            if ((int)duration >= delaySeconds)
            {
                return duration;
            }
        }

        // If no duration is large enough, use the largest available
        return durations.Last();
    }

    /// <summary>
    /// Determines if a Kafka error is critical and requires fail-fast behavior.
    /// </summary>
    /// <param name="error">The Kafka error to evaluate</param>
    /// <returns>True if the error is critical, false otherwise</returns>
    private static bool IsCriticalKafkaError(Error error)
    {
        return error.Code switch
        {
            ErrorCode.BrokerNotAvailable => true,
            ErrorCode.NetworkException => true,
            ErrorCode.SaslAuthenticationFailed => true,
            ErrorCode.TopicAuthorizationFailed => true,
            ErrorCode.GroupAuthorizationFailed => true,
            ErrorCode.ClusterAuthorizationFailed => true,
            ErrorCode.InvalidConfig => true,
            ErrorCode.Local_Fatal => true,
            ErrorCode.Local_Authentication => true,
            ErrorCode.Local_AllBrokersDown => true,
            _ => false
        };
    }

    /// <summary>
    /// Gets current performance metrics for the producer.
    /// </summary>
    /// <returns>Current performance metrics</returns>
    public PerformanceMetrics GetPerformanceMetrics()
    {
        return _performanceTracker.GetMetrics();
    }

    /// <summary>
    /// Disposes the Kafka producer and releases resources.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        try
        {
            // Log final performance metrics before disposal
            var finalMetrics = _performanceTracker.GetMetrics();
            _logger.LogInformation(LogEvents.PerformanceMetrics,
                "Final producer metrics - Messages produced: {MessagesProduced}, Errors: {Errors}, " +
                "Throughput: {Throughput:F2} msg/s, Peak memory: {PeakMemoryMB:F2} MB, Uptime: {Uptime}",
                finalMetrics.TotalMessagesProduced, finalMetrics.TotalErrors,
                finalMetrics.MessagesPerSecond, finalMetrics.PeakMemoryUsageBytes / 1024.0 / 1024.0,
                finalMetrics.Uptime);

            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
            
            // Dispose performance tracker
            _performanceTracker?.Dispose();
            
            _logger.LogInformation("DelayedMessageProducer disposed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing DelayedMessageProducer");
        }
        finally
        {
            _disposed = true;
        }
    }
}