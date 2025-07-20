namespace KafkaDelayedMessaging.Models;

/// <summary>
/// Represents an HTTP request that needs to be retried with delay.
/// Contains all necessary information to recreate the HTTP request.
/// </summary>
public class HttpRetryRequest
{
    /// <summary>
    /// The HTTP method (GET, POST, PUT, DELETE, etc.)
    /// </summary>
    public string Method { get; set; } = string.Empty;
    
    /// <summary>
    /// The target URL for the HTTP request
    /// </summary>
    public string Url { get; set; } = string.Empty;
    
    /// <summary>
    /// HTTP headers to include in the request
    /// </summary>
    public Dictionary<string, string> Headers { get; set; } = new();
    
    /// <summary>
    /// Request body content (for POST, PUT requests)
    /// </summary>
    public string? Body { get; set; }
    
    /// <summary>
    /// Content type for the request body
    /// </summary>
    public string? ContentType { get; set; }
    
    /// <summary>
    /// Current retry attempt number (starts at 0)
    /// </summary>
    public int RetryAttempt { get; set; }
    
    /// <summary>
    /// Maximum number of retry attempts allowed
    /// </summary>
    public int MaxRetries { get; set; } = 5;
    
    /// <summary>
    /// Original request timestamp for tracking
    /// </summary>
    public DateTime OriginalTimestamp { get; set; } = DateTime.UtcNow;
    
    /// <summary>
    /// Unique identifier for tracking this request across retries
    /// </summary>
    public string RequestId { get; set; } = Guid.NewGuid().ToString();
}