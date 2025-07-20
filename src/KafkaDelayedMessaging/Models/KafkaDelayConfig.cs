using System.ComponentModel.DataAnnotations;

namespace KafkaDelayedMessaging.Models;

/// <summary>
/// Configuration class for Kafka delayed messaging system.
/// Contains all necessary settings for Kafka connection and delay topic management.
/// </summary>
public class KafkaDelayConfig
{
    /// <summary>
    /// Kafka bootstrap servers connection string.
    /// </summary>
    [Required]
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Prefix used for delay topic naming convention.
    /// Default: "delay-" (results in topics like "delay-5s", "delay-10s")
    /// </summary>
    public string DelayTopicPrefix { get; set; } = "delay-";

    /// <summary>
    /// Maximum time between polls for consumer.
    /// Default: 5 minutes (300000 ms)
    /// </summary>
    public int MaxPollIntervalMs { get; set; } = 300000;

    /// <summary>
    /// Consumer session timeout in milliseconds.
    /// Default: 30 seconds (30000 ms)
    /// </summary>
    public int SessionTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Base consumer group ID. Actual consumer groups will be suffixed with delay topic name.
    /// </summary>
    [Required]
    public string ConsumerGroupId { get; set; } = string.Empty;

    /// <summary>
    /// Producer acknowledgment setting.
    /// Default: "all" for maximum durability
    /// </summary>
    public string Acks { get; set; } = "all";

    /// <summary>
    /// Producer retry attempts for transient failures.
    /// Default: 3
    /// </summary>
    public int Retries { get; set; } = 3;

    /// <summary>
    /// Enable auto-commit for consumers.
    /// Default: true
    /// </summary>
    public bool EnableAutoCommit { get; set; } = true;

    /// <summary>
    /// Auto-commit interval in milliseconds.
    /// Default: 5 seconds (5000 ms)
    /// </summary>
    public int AutoCommitIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Producer request timeout in milliseconds.
    /// Default: 30 seconds (30000 ms)
    /// </summary>
    public int RequestTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Producer message timeout in milliseconds.
    /// Default: 30 seconds (30000 ms)
    /// </summary>
    public int MessageTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Consumer heartbeat interval in milliseconds.
    /// Default: 3 seconds (3000 ms)
    /// </summary>
    public int HeartbeatIntervalMs { get; set; } = 3000;

    /// <summary>
    /// Consumer fetch minimum bytes.
    /// Default: 1 byte
    /// </summary>
    public int FetchMinBytes { get; set; } = 1;

    /// <summary>
    /// Validates the configuration and throws ValidationException if invalid.
    /// </summary>
    /// <exception cref="ValidationException">Thrown when configuration is invalid</exception>
    public void Validate()
    {
        var validationResults = new List<ValidationResult>();
        var validationContext = new ValidationContext(this);

        if (!Validator.TryValidateObject(this, validationContext, validationResults, true))
        {
            var errors = string.Join("; ", validationResults.Select(r => r.ErrorMessage));
            throw new ValidationException($"Invalid KafkaDelayConfig: {errors}");
        }

        // Additional custom validations
        if (string.IsNullOrWhiteSpace(BootstrapServers))
        {
            throw new ValidationException("BootstrapServers cannot be null or empty");
        }

        if (string.IsNullOrWhiteSpace(ConsumerGroupId))
        {
            throw new ValidationException("ConsumerGroupId cannot be null or empty");
        }

        if (MaxPollIntervalMs <= 0)
        {
            throw new ValidationException("MaxPollIntervalMs must be greater than 0");
        }

        if (SessionTimeoutMs <= 0)
        {
            throw new ValidationException("SessionTimeoutMs must be greater than 0");
        }

        if (Retries < 0)
        {
            throw new ValidationException("Retries cannot be negative");
        }

        if (AutoCommitIntervalMs <= 0)
        {
            throw new ValidationException("AutoCommitIntervalMs must be greater than 0");
        }

        if (string.IsNullOrWhiteSpace(DelayTopicPrefix))
        {
            throw new ValidationException("DelayTopicPrefix cannot be null or empty");
        }

        if (RequestTimeoutMs <= 0)
        {
            throw new ValidationException("RequestTimeoutMs must be greater than 0");
        }

        if (MessageTimeoutMs <= 0)
        {
            throw new ValidationException("MessageTimeoutMs must be greater than 0");
        }

        if (HeartbeatIntervalMs <= 0)
        {
            throw new ValidationException("HeartbeatIntervalMs must be greater than 0");
        }

        if (FetchMinBytes < 0)
        {
            throw new ValidationException("FetchMinBytes cannot be negative");
        }

        // Validate heartbeat interval is less than session timeout
        if (HeartbeatIntervalMs >= SessionTimeoutMs)
        {
            throw new ValidationException("HeartbeatIntervalMs must be less than SessionTimeoutMs");
        }
    }

    /// <summary>
    /// Gets the full delay topic name for a given delay duration.
    /// </summary>
    /// <param name="delayDuration">The delay duration</param>
    /// <returns>Full topic name (e.g., "delay-5s")</returns>
    public string GetDelayTopicName(DelayDuration delayDuration)
    {
        return $"{DelayTopicPrefix}{(int)delayDuration}s";
    }

    /// <summary>
    /// Gets the consumer group name for a specific delay topic.
    /// </summary>
    /// <param name="delayDuration">The delay duration</param>
    /// <returns>Consumer group name (e.g., "my-app-delay-5s")</returns>
    public string GetConsumerGroupName(DelayDuration delayDuration)
    {
        return $"{ConsumerGroupId}-{GetDelayTopicName(delayDuration)}";
    }
}