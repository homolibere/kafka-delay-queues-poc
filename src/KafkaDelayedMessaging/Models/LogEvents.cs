using Microsoft.Extensions.Logging;

namespace KafkaDelayedMessaging.Models;

/// <summary>
/// Static class containing predefined event IDs for structured logging in the delayed messaging system.
/// Event IDs are organized by category: 1xxx for informational events, 2xxx for warnings, 3xxx for errors.
/// </summary>
public static class LogEvents
{
    // Informational Events (1xxx)
    
    /// <summary>
    /// Event logged when a message is successfully produced to a delay topic.
    /// </summary>
    public static readonly EventId MessageProduced = new(1001, "MessageProduced");

    /// <summary>
    /// Event logged when a message is consumed from a delay topic.
    /// </summary>
    public static readonly EventId MessageConsumed = new(1002, "MessageConsumed");

    /// <summary>
    /// Event logged when a message is successfully forwarded to its target topic.
    /// </summary>
    public static readonly EventId MessageForwarded = new(1003, "MessageForwarded");

    /// <summary>
    /// Event logged when a delay consumer starts processing.
    /// </summary>
    public static readonly EventId ConsumerStarted = new(1004, "ConsumerStarted");

    /// <summary>
    /// Event logged when a delay consumer stops processing.
    /// </summary>
    public static readonly EventId ConsumerStopped = new(1005, "ConsumerStopped");

    /// <summary>
    /// Event logged when a consumer sleeps waiting for message processing time.
    /// </summary>
    public static readonly EventId ConsumerSleeping = new(1006, "ConsumerSleeping");

    /// <summary>
    /// Event logged when configuration is successfully validated.
    /// </summary>
    public static readonly EventId ConfigurationValidated = new(1007, "ConfigurationValidated");

    /// <summary>
    /// Event logged when partitions are assigned to a consumer.
    /// </summary>
    public static readonly EventId PartitionsAssigned = new(1008, "PartitionsAssigned");

    /// <summary>
    /// Event logged when partitions are revoked from a consumer.
    /// </summary>
    public static readonly EventId PartitionsRevoked = new(1009, "PartitionsRevoked");

    /// <summary>
    /// Event logged when partitions are lost by a consumer.
    /// </summary>
    public static readonly EventId PartitionsLost = new(2005, "PartitionsLost");

    // Warning Events (2xxx)

    /// <summary>
    /// Event logged when a message has malformed headers but processing continues.
    /// </summary>
    public static readonly EventId HeaderMalformed = new(2001, "HeaderMalformed");

    /// <summary>
    /// Event logged when a message is skipped due to processing errors.
    /// </summary>
    public static readonly EventId MessageSkipped = new(2002, "MessageSkipped");

    /// <summary>
    /// Event logged when a retry attempt is made for a failed operation.
    /// </summary>
    public static readonly EventId RetryAttempt = new(2003, "RetryAttempt");

    /// <summary>
    /// Event logged when consumer lag is detected.
    /// </summary>
    public static readonly EventId ConsumerLag = new(2004, "ConsumerLag");

    // Error Events (3xxx)

    /// <summary>
    /// Event logged when a general processing error occurs.
    /// </summary>
    public static readonly EventId ProcessingError = new(3001, "ProcessingError");

    /// <summary>
    /// Event logged when Kafka connection fails.
    /// </summary>
    public static readonly EventId KafkaConnectionError = new(3002, "KafkaConnectionError");

    /// <summary>
    /// Event logged when target topic is unreachable during message forwarding.
    /// </summary>
    public static readonly EventId TargetTopicUnreachable = new(3003, "TargetTopicUnreachable");

    /// <summary>
    /// Event logged when message serialization fails.
    /// </summary>
    public static readonly EventId SerializationError = new(3004, "SerializationError");

    /// <summary>
    /// Event logged when message deserialization fails.
    /// </summary>
    public static readonly EventId DeserializationError = new(3005, "DeserializationError");

    /// <summary>
    /// Event logged when configuration validation fails.
    /// </summary>
    public static readonly EventId ConfigurationError = new(3006, "ConfigurationError");

    /// <summary>
    /// Event logged when a critical error occurs that requires fail-fast behavior.
    /// </summary>
    public static readonly EventId CriticalError = new(3007, "CriticalError");

    /// <summary>
    /// Event logged when consumer group rebalancing fails.
    /// </summary>
    public static readonly EventId RebalanceError = new(3008, "RebalanceError");

    /// <summary>
    /// Event logged when offset commit fails.
    /// </summary>
    public static readonly EventId OffsetCommitError = new(3009, "OffsetCommitError");

    // Performance Monitoring Events (4xxx)

    /// <summary>
    /// Event logged for performance metrics reporting.
    /// </summary>
    public static readonly EventId PerformanceMetrics = new(4001, "PerformanceMetrics");

    /// <summary>
    /// Event logged for throughput measurements.
    /// </summary>
    public static readonly EventId ThroughputMeasurement = new(4002, "ThroughputMeasurement");

    /// <summary>
    /// Event logged for latency measurements.
    /// </summary>
    public static readonly EventId LatencyMeasurement = new(4003, "LatencyMeasurement");

    /// <summary>
    /// Event logged for memory usage monitoring.
    /// </summary>
    public static readonly EventId MemoryUsage = new(4004, "MemoryUsage");

    /// <summary>
    /// Event logged for delay precision measurements.
    /// </summary>
    public static readonly EventId DelayPrecision = new(4005, "DelayPrecision");
}