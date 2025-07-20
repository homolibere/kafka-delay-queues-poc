using KafkaDelayedMessaging.Models;

namespace KafkaDelayedMessaging.Interfaces;

/// <summary>
/// Interface for producing delayed messages to Kafka delay topics.
/// Messages are routed to appropriate delay topics based on delay duration
/// and contain metadata in headers for processing by delay consumers.
/// </summary>
public interface IDelayedMessageProducer : IDisposable
{
    /// <summary>
    /// Produces a delayed message using a predefined DelayDuration enum value.
    /// </summary>
    /// <typeparam name="T">Type of the message payload</typeparam>
    /// <param name="targetTopic">The topic where the message should be delivered after delay</param>
    /// <param name="message">The message payload to be delayed</param>
    /// <param name="delayDuration">Predefined delay duration from enum</param>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>Task representing the async operation</returns>
    Task ProduceDelayedMessageAsync<T>(
        string targetTopic, 
        T message, 
        DelayDuration delayDuration, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Produces a delayed message with a specific processing timestamp.
    /// </summary>
    /// <typeparam name="T">Type of the message payload</typeparam>
    /// <param name="targetTopic">The topic where the message should be delivered after delay</param>
    /// <param name="message">The message payload to be delayed</param>
    /// <param name="processAt">Exact timestamp when the message should be processed</param>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>Task representing the async operation</returns>
    Task ProduceDelayedMessageAsync<T>(
        string targetTopic, 
        T message, 
        DateTime processAt, 
        CancellationToken cancellationToken = default);
}