using KafkaDelayedMessaging.Models;

namespace KafkaDelayedMessaging.Interfaces;

/// <summary>
/// Interface for consuming delayed messages from Kafka delay topics.
/// Consumers process messages and forward them to target topics when ready.
/// </summary>
public interface IDelayConsumer : IDisposable
{
    /// <summary>
    /// Starts the delay consumer for a specific delay duration.
    /// The consumer will continuously process messages from the corresponding delay topic.
    /// </summary>
    /// <param name="delayDuration">The delay duration this consumer should handle</param>
    /// <param name="cancellationToken">Cancellation token to stop the consumer</param>
    /// <returns>Task representing the async operation</returns>
    Task StartAsync(DelayDuration delayDuration, CancellationToken cancellationToken);

    /// <summary>
    /// Stops the delay consumer gracefully.
    /// </summary>
    /// <returns>Task representing the async operation</returns>
    Task StopAsync();
}