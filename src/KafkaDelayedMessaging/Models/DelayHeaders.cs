namespace KafkaDelayedMessaging.Models;

/// <summary>
/// Static class containing header constant definitions for delayed messaging system.
/// These headers are used to store metadata in Kafka messages for delay processing.
/// </summary>
public static class DelayHeaders
{
    /// <summary>
    /// Header key for the target topic where the message should be delivered after delay.
    /// </summary>
    public const string TargetTopic = "x-target-topic";

    /// <summary>
    /// Header key for the Unix timestamp (milliseconds) when the message should be processed.
    /// </summary>
    public const string ProcessAt = "x-process-at";

    /// <summary>
    /// Header key for the original message timestamp for tracking purposes.
    /// </summary>
    public const string OriginalTimestamp = "x-original-timestamp";
}