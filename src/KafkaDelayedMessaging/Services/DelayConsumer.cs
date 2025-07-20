using System.Text;
using System.Text.Json;
using System.Diagnostics;
using Confluent.Kafka;
using KafkaDelayedMessaging.Interfaces;
using KafkaDelayedMessaging.Models;
using Microsoft.Extensions.Logging;

namespace KafkaDelayedMessaging.Services;

/// <summary>
/// Implementation of IDelayConsumer that processes delayed messages from Kafka delay topics.
/// Consumes messages, checks their processing timestamps, and forwards them to target topics when ready.
/// </summary>
public class DelayConsumer : IDelayConsumer
{
    private readonly KafkaDelayConfig _config;
    private readonly ILogger<DelayConsumer> _logger;
    private readonly PerformanceTracker _performanceTracker;
    private readonly Timer _metricsTimer;
    private IConsumer<string, string>? _consumer;
    private IProducer<string, string>? _producer;
    private CancellationTokenSource? _cancellationTokenSource;
    private Task? _consumeTask;
    private bool _disposed;

    public DelayConsumer(KafkaDelayConfig config, ILogger<DelayConsumer> logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _performanceTracker = new PerformanceTracker();
        
        // Initialize metrics timer to log performance metrics every 30 seconds
        _metricsTimer = new Timer(LogPerformanceMetrics, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
        
        // Validate configuration
        _config.Validate();
    }

    /// <inheritdoc />
    public async Task StartAsync(DelayDuration delayDuration, CancellationToken cancellationToken)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DelayConsumer));

        if (_consumeTask != null)
            throw new InvalidOperationException("Consumer is already running");

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        
        var delayTopic = _config.GetDelayTopicName(delayDuration);
        var consumerGroup = _config.GetConsumerGroupName(delayDuration);

        _logger.LogInformation(LogEvents.ConsumerStarted, 
            "Starting delay consumer for topic {DelayTopic} with consumer group {ConsumerGroup}", 
            delayTopic, consumerGroup);

        // Initialize consumer
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _config.BootstrapServers,
            GroupId = consumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = _config.EnableAutoCommit,
            AutoCommitIntervalMs = _config.AutoCommitIntervalMs,
            SessionTimeoutMs = _config.SessionTimeoutMs,
            MaxPollIntervalMs = _config.MaxPollIntervalMs,
            HeartbeatIntervalMs = _config.HeartbeatIntervalMs,
            FetchMinBytes = _config.FetchMinBytes,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            EnablePartitionEof = false,
            IsolationLevel = IsolationLevel.ReadCommitted
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(Deserializers.Utf8)
            .SetErrorHandler((_, e) => _logger.LogError(LogEvents.KafkaConnectionError, 
                "Consumer error: {Error}", e.Reason))
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogInformation(LogEvents.PartitionsAssigned,
                    "Consumer group {ConsumerGroup} assigned partitions: {Partitions}",
                    consumerGroup, string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                _logger.LogInformation(LogEvents.PartitionsRevoked,
                    "Consumer group {ConsumerGroup} revoked partitions: {Partitions}",
                    consumerGroup, string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .SetPartitionsLostHandler((c, partitions) =>
            {
                _logger.LogWarning(LogEvents.PartitionsLost,
                    "Consumer group {ConsumerGroup} lost partitions: {Partitions}",
                    consumerGroup, string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .Build();

        // Initialize producer for forwarding messages
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _config.BootstrapServers,
            Acks = Acks.All,
            RequestTimeoutMs = _config.RequestTimeoutMs,
            MessageTimeoutMs = _config.MessageTimeoutMs
        };

        _producer = new ProducerBuilder<string, string>(producerConfig)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(Serializers.Utf8)
            .SetErrorHandler((_, e) => _logger.LogError(LogEvents.KafkaConnectionError, 
                "Producer error: {Error}", e.Reason))
            .Build();

        // Subscribe to delay topic
        _consumer.Subscribe(delayTopic);

        // Start consumption task
        _consumeTask = Task.Run(() => ConsumeLoop(_cancellationTokenSource.Token), _cancellationTokenSource.Token);

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync()
    {
        if (_disposed || _consumeTask == null)
            return;

        _logger.LogInformation(LogEvents.ConsumerStopped, "Stopping delay consumer");

        _cancellationTokenSource?.Cancel();

        try
        {
            await _consumeTask;
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }

        _consumer?.Close();
        _consumer?.Dispose();
        _producer?.Dispose();

        _consumeTask = null;
        _cancellationTokenSource?.Dispose();
        _cancellationTokenSource = null;
    }

    private async Task ConsumeLoop(CancellationToken cancellationToken)
    {
        var consecutiveErrors = 0;
        const int maxConsecutiveErrors = 5;
        
        try
        {
            _logger.LogInformation("Starting consume loop for delay consumer");
            
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer!.Consume(TimeSpan.FromSeconds(1));
                    
                    if (consumeResult?.Message != null)
                    {
                        await ProcessMessage(consumeResult, cancellationToken);
                        consecutiveErrors = 0; // Reset error counter on successful processing
                    }
                }
                catch (ConsumeException ex)
                {
                    consecutiveErrors++;
                    _logger.LogError(LogEvents.ProcessingError, ex, 
                        "Error consuming message (consecutive errors: {ConsecutiveErrors}): {Error}", 
                        consecutiveErrors, ex.Error.Reason);
                    
                    // Check if this is a critical Kafka error
                    if (IsCriticalKafkaError(ex.Error))
                    {
                        _logger.LogCritical(LogEvents.CriticalError,
                            "Critical Kafka consume error detected, failing fast: {Error}", ex.Error.Reason);
                        throw new InvalidOperationException($"Critical Kafka consume error: {ex.Error.Reason}", ex);
                    }
                    
                    // If too many consecutive errors, fail fast
                    if (consecutiveErrors >= maxConsecutiveErrors)
                    {
                        _logger.LogCritical(LogEvents.CriticalError,
                            "Too many consecutive consume errors ({ConsecutiveErrors}), failing fast", consecutiveErrors);
                        throw new InvalidOperationException($"Too many consecutive consume errors: {consecutiveErrors}", ex);
                    }
                    
                    // Wait before retrying to avoid tight error loops
                    await Task.Delay(TimeSpan.FromSeconds(Math.Min(consecutiveErrors, 10)), cancellationToken);
                }
                catch (KafkaException ex)
                {
                    consecutiveErrors++;
                    _logger.LogError(LogEvents.KafkaConnectionError, ex,
                        "Kafka error in consume loop (consecutive errors: {ConsecutiveErrors}): {Error}",
                        consecutiveErrors, ex.Error.Reason);
                    
                    if (IsCriticalKafkaError(ex.Error) || consecutiveErrors >= maxConsecutiveErrors)
                    {
                        _logger.LogCritical(LogEvents.CriticalError,
                            "Critical Kafka error or too many consecutive errors, failing fast: {Error}", ex.Error.Reason);
                        throw new InvalidOperationException($"Critical Kafka error: {ex.Error.Reason}", ex);
                    }
                    
                    await Task.Delay(TimeSpan.FromSeconds(Math.Min(consecutiveErrors, 10)), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Consume loop cancelled");
                    break;
                }
                catch (Exception ex)
                {
                    consecutiveErrors++;
                    _logger.LogError(LogEvents.CriticalError, ex, 
                        "Unexpected error in consume loop (consecutive errors: {ConsecutiveErrors}): {Error}",
                        consecutiveErrors, ex.Message);
                    
                    // For unexpected errors, fail fast after a few attempts
                    if (consecutiveErrors >= 3)
                    {
                        _logger.LogCritical(LogEvents.CriticalError,
                            "Too many consecutive unexpected errors, failing fast");
                        throw;
                    }
                    
                    await Task.Delay(TimeSpan.FromSeconds(consecutiveErrors * 2), cancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consume loop terminated due to cancellation");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(LogEvents.CriticalError, ex, 
                "Fatal error in consume loop, consumer will stop: {Error}", ex.Message);
            throw;
        }
        finally
        {
            _logger.LogInformation("Consume loop ended");
        }
    }

    private async Task ProcessMessage(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
    {
        var messageOffset = consumeResult.Offset.Value;
        var messagePartition = consumeResult.Partition.Value;
        var processingStopwatch = Stopwatch.StartNew();
        var expectedProcessTime = DateTimeOffset.MinValue;
        var actualProcessTime = DateTimeOffset.UtcNow;
        
        try
        {
            _logger.LogDebug(LogEvents.MessageConsumed, 
                "Consumed message from partition {Partition} at offset {Offset}", 
                messagePartition, messageOffset);

            // Validate message structure
            if (consumeResult.Message?.Headers == null)
            {
                _logger.LogWarning(LogEvents.HeaderMalformed,
                    "Message at offset {Offset} has no headers, skipping", messageOffset);
                await CommitOffsetSafely(consumeResult, cancellationToken);
                return;
            }

            if (string.IsNullOrEmpty(consumeResult.Message.Value))
            {
                _logger.LogWarning(LogEvents.ProcessingError,
                    "Message at offset {Offset} has empty or null value, skipping", messageOffset);
                await CommitOffsetSafely(consumeResult, cancellationToken);
                return;
            }

            // Parse headers to extract target topic and processing timestamp
            var (targetTopic, processAt) = ParseMessageHeaders(consumeResult.Message.Headers);
            expectedProcessTime = processAt ?? DateTimeOffset.MinValue;
            
            if (string.IsNullOrEmpty(targetTopic))
            {
                _performanceTracker.RecordError();
                _logger.LogWarning(LogEvents.HeaderMalformed, 
                    "Message at offset {Offset} has missing or invalid target topic header, skipping", 
                    messageOffset);
                await CommitOffsetSafely(consumeResult, cancellationToken);
                return;
            }

            if (processAt == null)
            {
                _performanceTracker.RecordError();
                _logger.LogWarning(LogEvents.HeaderMalformed, 
                    "Message at offset {Offset} has missing or invalid process-at timestamp header, skipping", 
                    messageOffset);
                await CommitOffsetSafely(consumeResult, cancellationToken);
                return;
            }

            // Validate timestamp is reasonable (not too far in the past or future)
            var now = DateTimeOffset.UtcNow;
            var timeDifference = Math.Abs((processAt.Value - now).TotalDays);
            if (timeDifference > 30) // More than 30 days difference
            {
                _logger.LogWarning(LogEvents.HeaderMalformed,
                    "Message at offset {Offset} has unreasonable process-at timestamp {ProcessAt}, skipping",
                    messageOffset, processAt.Value);
                await CommitOffsetSafely(consumeResult, cancellationToken);
                return;
            }

            // Calculate delay until message should be processed
            var delay = processAt.Value - now;
            
            // Optimize sleep calculations to account for processing overhead
            const int processingOverheadMs = 5; // Account for ~5ms processing overhead
            if (delay > TimeSpan.FromMilliseconds(processingOverheadMs))
            {
                var optimizedDelay = delay.Subtract(TimeSpan.FromMilliseconds(processingOverheadMs));
                
                _logger.LogDebug(LogEvents.ConsumerSleeping, 
                    "Message not ready for processing, sleeping for {DelayMs}ms (optimized from {OriginalDelayMs}ms) until {ProcessAt}", 
                    optimizedDelay.TotalMilliseconds, delay.TotalMilliseconds, processAt.Value);

                try
                {
                    await Task.Delay(optimizedDelay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogDebug("Sleep interrupted by cancellation for message at offset {Offset}", messageOffset);
                    throw;
                }
            }

            // Record actual processing time for precision measurement
            actualProcessTime = DateTimeOffset.UtcNow;
            var delayPrecisionMs = (actualProcessTime - processAt.Value).TotalMilliseconds;

            // Forward message to target topic
            var forwardingStopwatch = Stopwatch.StartNew();
            try
            {
                await ForwardMessage(targetTopic, consumeResult.Message.Value, consumeResult.Message.Headers, cancellationToken);
                forwardingStopwatch.Stop();
            }
            catch (ProduceException<string, string> ex) when (IsRecoverableProduceError(ex.Error))
            {
                _logger.LogWarning(LogEvents.TargetTopicUnreachable,
                    "Recoverable error forwarding message to {TargetTopic}, will retry: {Error}",
                    targetTopic, ex.Error.Reason);
                
                // Wait a bit and retry once for recoverable errors
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
                await ForwardMessage(targetTopic, consumeResult.Message.Value, consumeResult.Message.Headers, cancellationToken);
                forwardingStopwatch.Stop();
            }

            // Commit offset after successful message forwarding
            await CommitOffset(consumeResult, cancellationToken);
            
            processingStopwatch.Stop();
            
            // Record performance metrics
            _performanceTracker.RecordMessageProcessed(processingStopwatch.Elapsed.TotalMilliseconds);
            _performanceTracker.RecordMessageForwarded(delayPrecisionMs);
            
            // Log performance metrics for this message
            _logger.LogDebug(LogEvents.LatencyMeasurement,
                "Message processing latency: {ProcessingLatencyMs}ms, forwarding latency: {ForwardingLatencyMs}ms",
                processingStopwatch.ElapsedMilliseconds, forwardingStopwatch.ElapsedMilliseconds);
                
            _logger.LogDebug(LogEvents.DelayPrecision,
                "Delay precision: {DelayPrecisionMs}ms (expected: {ExpectedTime}, actual: {ActualTime})",
                delayPrecisionMs, processAt.Value, actualProcessTime);

            _logger.LogInformation(LogEvents.MessageForwarded, 
                "Successfully forwarded message from offset {Offset} to target topic {TargetTopic}, " +
                "processing latency: {ProcessingLatencyMs}ms, delay precision: {DelayPrecisionMs}ms", 
                messageOffset, targetTopic, processingStopwatch.ElapsedMilliseconds, Math.Abs(delayPrecisionMs));
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Message processing cancelled for offset {Offset}", messageOffset);
            throw;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(LogEvents.TargetTopicUnreachable, ex,
                "Failed to forward message at offset {Offset} to target topic after retry: {Error}",
                messageOffset, ex.Error.Reason);
            
            // For produce errors, we still commit the offset to avoid reprocessing
            await CommitOffsetSafely(consumeResult, cancellationToken);
        }
        catch (JsonException ex)
        {
            _logger.LogError(LogEvents.DeserializationError, ex,
                "Failed to deserialize message at offset {Offset}: {Error}",
                messageOffset, ex.Message);
            
            // Skip malformed messages and commit offset
            await CommitOffsetSafely(consumeResult, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(LogEvents.ProcessingError, ex, 
                "Unexpected error processing message at offset {Offset}: {Error}", 
                messageOffset, ex.Message);
            
            // For unexpected errors, still commit offset to avoid infinite reprocessing
            await CommitOffsetSafely(consumeResult, cancellationToken);
        }
    }

    private (string? targetTopic, DateTimeOffset? processAt) ParseMessageHeaders(Headers headers)
    {
        string? targetTopic = null;
        DateTimeOffset? processAt = null;

        try
        {
            // Extract target topic
            if (headers.TryGetLastBytes(DelayHeaders.TargetTopic, out var targetTopicBytes))
            {
                targetTopic = Encoding.UTF8.GetString(targetTopicBytes);
            }

            // Extract processing timestamp
            if (headers.TryGetLastBytes(DelayHeaders.ProcessAt, out var processAtBytes))
            {
                var processAtString = Encoding.UTF8.GetString(processAtBytes);
                if (long.TryParse(processAtString, out var unixTimestamp))
                {
                    processAt = DateTimeOffset.FromUnixTimeMilliseconds(unixTimestamp);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(LogEvents.HeaderMalformed, ex, 
                "Error parsing message headers");
        }

        return (targetTopic, processAt);
    }

    private async Task ForwardMessage(string targetTopic, string messageValue, Headers originalHeaders, CancellationToken cancellationToken)
    {
        try
        {
            // Create new headers without delay-specific headers
            var forwardHeaders = new Headers();
            
            foreach (var header in originalHeaders)
            {
                // Skip delay-specific headers when forwarding
                if (header.Key != DelayHeaders.TargetTopic && 
                    header.Key != DelayHeaders.ProcessAt)
                {
                    forwardHeaders.Add(header.Key, header.GetValueBytes());
                }
            }

            var message = new Message<string, string>
            {
                Value = messageValue,
                Headers = forwardHeaders
            };

            var deliveryResult = await _producer!.ProduceAsync(targetTopic, message, cancellationToken);
            
            _logger.LogDebug(LogEvents.MessageForwarded, 
                "Message forwarded to {TargetTopic} at partition {Partition} offset {Offset}", 
                targetTopic, deliveryResult.Partition.Value, deliveryResult.Offset.Value);
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(LogEvents.TargetTopicUnreachable, ex, 
                "Failed to forward message to target topic {TargetTopic}: {Error}", 
                targetTopic, ex.Error.Reason);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(LogEvents.ProcessingError, ex, 
                "Unexpected error forwarding message to target topic {TargetTopic}", targetTopic);
            throw;
        }
    }

    private async Task CommitOffset(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
    {
        try
        {
            // Only commit if auto-commit is disabled and consumer is initialized
            if (!_config.EnableAutoCommit && _consumer != null)
            {
                var topicPartitionOffset = new TopicPartitionOffset(
                    consumeResult.TopicPartition, 
                    consumeResult.Offset + 1); // Commit the next offset

                _consumer.Commit(new[] { topicPartitionOffset });
                
                _logger.LogDebug("Committed offset {Offset} for partition {Partition}", 
                    topicPartitionOffset.Offset.Value, topicPartitionOffset.Partition.Value);
            }
            
            await Task.CompletedTask;
        }
        catch (KafkaException ex)
        {
            _logger.LogError(LogEvents.OffsetCommitError, ex, 
                "Failed to commit offset {Offset} for partition {Partition}: {Error}", 
                consumeResult.Offset.Value, consumeResult.Partition.Value, ex.Error.Reason);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(LogEvents.OffsetCommitError, ex, 
                "Unexpected error committing offset {Offset} for partition {Partition}", 
                consumeResult.Offset.Value, consumeResult.Partition.Value);
            throw;
        }
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
            ErrorCode.GroupCoordinatorNotAvailable => true,
            _ => false
        };
    }

    /// <summary>
    /// Determines if a produce error is recoverable and worth retrying.
    /// </summary>
    /// <param name="error">The Kafka error to evaluate</param>
    /// <returns>True if the error is recoverable, false otherwise</returns>
    private static bool IsRecoverableProduceError(Error error)
    {
        return error.Code switch
        {
            ErrorCode.RequestTimedOut => true,
            ErrorCode.NotEnoughReplicas => true,
            ErrorCode.NotEnoughReplicasAfterAppend => true,
            ErrorCode.LeaderNotAvailable => true,
            ErrorCode.NotLeaderForPartition => true,
            ErrorCode.NetworkException => true,
            _ => false
        };
    }

    /// <summary>
    /// Safely commits offset without throwing exceptions that would disrupt message processing.
    /// </summary>
    /// <param name="consumeResult">The consume result to commit</param>
    /// <param name="cancellationToken">Cancellation token</param>
    private async Task CommitOffsetSafely(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
    {
        try
        {
            await CommitOffset(consumeResult, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(LogEvents.OffsetCommitError, ex,
                "Failed to commit offset {Offset} for partition {Partition}, continuing processing: {Error}",
                consumeResult.Offset.Value, consumeResult.Partition.Value, ex.Message);
            
            // Don't rethrow - we want to continue processing even if offset commit fails
        }
    }

    /// <summary>
    /// Logs performance metrics periodically for monitoring long-running consumers.
    /// </summary>
    /// <param name="state">Timer state (not used)</param>
    private void LogPerformanceMetrics(object? state)
    {
        try
        {
            var metrics = _performanceTracker.GetMetrics();
            
            _logger.LogInformation(LogEvents.PerformanceMetrics,
                "Consumer performance metrics - Messages processed: {MessagesProcessed}, " +
                "Messages forwarded: {MessagesForwarded}, Errors: {Errors}, " +
                "Throughput: {Throughput:F2} msg/s, Avg processing latency: {AvgLatencyMs:F2}ms, " +
                "Avg delay precision: {AvgPrecisionMs:F2}ms, Memory usage: {MemoryMB:F2}MB, " +
                "Peak memory: {PeakMemoryMB:F2}MB, Uptime: {Uptime}",
                metrics.TotalMessagesProcessed, metrics.TotalMessagesForwarded, metrics.TotalErrors,
                metrics.MessagesPerSecond, metrics.AverageProcessingLatencyMs, metrics.AverageDelayPrecisionMs,
                metrics.MemoryUsageBytes / 1024.0 / 1024.0, metrics.PeakMemoryUsageBytes / 1024.0 / 1024.0,
                metrics.Uptime);

            // Log memory usage separately for monitoring
            _logger.LogDebug(LogEvents.MemoryUsage,
                "Memory usage: {CurrentMemoryMB:F2}MB, Peak: {PeakMemoryMB:F2}MB, GC Gen0: {Gen0}, Gen1: {Gen1}, Gen2: {Gen2}",
                metrics.MemoryUsageBytes / 1024.0 / 1024.0, metrics.PeakMemoryUsageBytes / 1024.0 / 1024.0,
                GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));

            // Log throughput measurement
            _logger.LogDebug(LogEvents.ThroughputMeasurement,
                "Throughput: {Throughput:F2} messages/second over {Uptime}",
                metrics.MessagesPerSecond, metrics.Uptime);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Error logging performance metrics: {Error}", ex.Message);
        }
    }

    /// <summary>
    /// Gets current performance metrics for the consumer.
    /// </summary>
    /// <returns>Current performance metrics</returns>
    public PerformanceMetrics GetPerformanceMetrics()
    {
        return _performanceTracker.GetMetrics();
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        try
        {
            // Log final performance metrics before disposal
            var finalMetrics = _performanceTracker.GetMetrics();
            _logger.LogInformation(LogEvents.PerformanceMetrics,
                "Final consumer metrics - Messages processed: {MessagesProcessed}, " +
                "Messages forwarded: {MessagesForwarded}, Errors: {Errors}, " +
                "Throughput: {Throughput:F2} msg/s, Avg processing latency: {AvgLatencyMs:F2}ms, " +
                "Avg delay precision: {AvgPrecisionMs:F2}ms, Peak memory: {PeakMemoryMB:F2}MB, " +
                "Uptime: {Uptime}",
                finalMetrics.TotalMessagesProcessed, finalMetrics.TotalMessagesForwarded, 
                finalMetrics.TotalErrors, finalMetrics.MessagesPerSecond, 
                finalMetrics.AverageProcessingLatencyMs, finalMetrics.AverageDelayPrecisionMs,
                finalMetrics.PeakMemoryUsageBytes / 1024.0 / 1024.0, finalMetrics.Uptime);

            StopAsync().GetAwaiter().GetResult();
            
            // Dispose metrics timer
            _metricsTimer?.Dispose();
            
            // Dispose performance tracker
            _performanceTracker?.Dispose();
            
            _logger.LogInformation("DelayConsumer disposed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing DelayConsumer");
        }
        finally
        {
            _disposed = true;
        }
    }
}