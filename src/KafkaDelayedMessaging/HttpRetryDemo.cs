using KafkaDelayedMessaging.Interfaces;
using KafkaDelayedMessaging.Models;
using KafkaDelayedMessaging.Services;
using Microsoft.Extensions.Logging;

namespace KafkaDelayedMessaging;

/// <summary>
/// Standalone demonstration of HTTP retry functionality using the delayed messaging system.
/// This class shows how HTTP failures trigger retry messages with progressive backoff delays.
/// </summary>
public class HttpRetryDemo
{
    private readonly ILogger<HttpRetryDemo> _logger;

    public HttpRetryDemo(ILogger<HttpRetryDemo> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Runs the HTTP retry demonstration with various failure scenarios.
    /// </summary>
    public async Task RunDemoAsync()
    {
        _logger.LogInformation("=== HTTP Retry with Delayed Messaging Demo ===");
        _logger.LogInformation("This demo shows how HTTP failures trigger delayed retry messages");
        _logger.LogInformation("");

        // Create a demo producer that logs what would be sent to Kafka
        var demoProducer = new DemoDelayedMessageProducer(_logger);
        
        // Create HTTP client with short timeout for demo purposes
        using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        
        // Create HTTP retry service
        var httpRetryLogger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<HttpRetryService>();
        var httpRetryService = new HttpRetryService(httpClient, demoProducer, httpRetryLogger);

        // Demo scenarios
        await DemoSuccessfulRequest(httpRetryService);
        await DemoServerErrorWithRetries(httpRetryService);
        await DemoClientErrorNoRetries(httpRetryService);
        await DemoNetworkTimeoutWithRetries(httpRetryService);
        await DemoPostRequestWithRetries(httpRetryService);
        await DemoProgressiveBackoff(httpRetryService);

        _logger.LogInformation("");
        _logger.LogInformation("=== Demo Complete ===");
        _logger.LogInformation("In a real system:");
        _logger.LogInformation("1. Failed HTTP requests would produce messages to delay topics");
        _logger.LogInformation("2. Delay consumers would process these messages after the specified delay");
        _logger.LogInformation("3. The HTTP requests would be retried with progressive backoff");
        _logger.LogInformation("4. This provides resilient HTTP communication with automatic retries");
    }

    private async Task DemoSuccessfulRequest(HttpRetryService httpRetryService)
    {
        _logger.LogInformation("--- Demo 1: Successful HTTP Request (No Retries) ---");
        try
        {
            var result = await httpRetryService.GetWithRetryAsync("https://httpbin.org/status/200");
            _logger.LogInformation("✅ Request succeeded - no retry messages produced");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Request failed: {Error}", ex.Message);
        }
        _logger.LogInformation("");
    }

    private async Task DemoServerErrorWithRetries(HttpRetryService httpRetryService)
    {
        _logger.LogInformation("--- Demo 2: Server Error (500) - Triggers Retries ---");
        try
        {
            var result = await httpRetryService.GetWithRetryAsync("https://httpbin.org/status/500");
            _logger.LogInformation("❌ Request failed as expected - retry message should be produced");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Request failed: {Error}", ex.Message);
        }
        _logger.LogInformation("");
    }

    private async Task DemoClientErrorNoRetries(HttpRetryService httpRetryService)
    {
        _logger.LogInformation("--- Demo 3: Client Error (404) - No Retries ---");
        try
        {
            var result = await httpRetryService.GetWithRetryAsync("https://httpbin.org/status/404");
            _logger.LogInformation("❌ Request failed as expected - no retry message (client errors aren't retried)");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Request failed: {Error}", ex.Message);
        }
        _logger.LogInformation("");
    }

    private async Task DemoNetworkTimeoutWithRetries(HttpRetryService httpRetryService)
    {
        _logger.LogInformation("--- Demo 4: Network Timeout - Triggers Retries ---");
        try
        {
            var result = await httpRetryService.GetWithRetryAsync("https://httpbin.org/delay/10"); // Will timeout
            _logger.LogInformation("❌ Request timed out as expected - retry message should be produced");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Request failed: {Error}", ex.Message);
        }
        _logger.LogInformation("");
    }

    private async Task DemoPostRequestWithRetries(HttpRetryService httpRetryService)
    {
        _logger.LogInformation("--- Demo 5: POST Request with Server Error - Triggers Retries ---");
        try
        {
            var postData = """{"demo": "data", "timestamp": "2024-01-01T00:00:00Z"}""";
            var result = await httpRetryService.PostWithRetryAsync(
                "https://httpbin.org/status/503", 
                postData,
                "application/json");
            _logger.LogInformation("❌ POST request failed as expected - retry message with body should be produced");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Request failed: {Error}", ex.Message);
        }
        _logger.LogInformation("");
    }

    private async Task DemoProgressiveBackoff(HttpRetryService httpRetryService)
    {
        _logger.LogInformation("--- Demo 6: Progressive Backoff Pattern ---");
        _logger.LogInformation("When HTTP requests fail, retries are scheduled with these delays:");
        _logger.LogInformation("  Attempt 1: 5 seconds (DelayDuration.Seconds5)");
        _logger.LogInformation("  Attempt 2: 10 seconds (DelayDuration.Seconds10)");
        _logger.LogInformation("  Attempt 3: 20 seconds (DelayDuration.Seconds20)");
        _logger.LogInformation("  Attempt 4: 40 seconds (DelayDuration.Seconds40)");
        _logger.LogInformation("  Attempt 5: ~1 minute (DelayDuration.Minutes1)");
        _logger.LogInformation("");
        
        // Simulate processing a retry request to show the progression
        var retryRequest = new HttpRetryRequest
        {
            Method = "GET",
            Url = "https://httpbin.org/status/502",
            RetryAttempt = 2, // This would be the 3rd attempt
            MaxRetries = 5,
            RequestId = Guid.NewGuid().ToString()
        };

        try
        {
            var result = await httpRetryService.ProcessRetryRequestAsync(retryRequest);
            _logger.LogInformation("❌ Retry attempt failed - next retry would be scheduled with 40-second delay");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Retry request failed: {Error}", ex.Message);
        }
        _logger.LogInformation("");
    }
}

/// <summary>
/// Demo implementation of IDelayedMessageProducer that logs what would be sent to Kafka.
/// </summary>
public class DemoDelayedMessageProducer : IDelayedMessageProducer
{
    private readonly ILogger _logger;

    public DemoDelayedMessageProducer(ILogger logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task ProduceDelayedMessageAsync<T>(
        string targetTopic, 
        T message, 
        DelayDuration delayDuration, 
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("📨 DELAYED MESSAGE PRODUCED:");
        _logger.LogInformation("   Target Topic: {TargetTopic}", targetTopic);
        _logger.LogInformation("   Delay Duration: {DelayDuration} ({DelaySeconds} seconds)", 
            delayDuration, (int)delayDuration);
        _logger.LogInformation("   Delay Topic: delay-{DelaySeconds}s", (int)delayDuration);
        
        if (message is HttpRetryRequest retryRequest)
        {
            _logger.LogInformation("   HTTP Method: {Method}", retryRequest.Method);
            _logger.LogInformation("   URL: {Url}", retryRequest.Url);
            _logger.LogInformation("   Retry Attempt: {RetryAttempt}/{MaxRetries}", 
                retryRequest.RetryAttempt, retryRequest.MaxRetries);
            _logger.LogInformation("   Request ID: {RequestId}", retryRequest.RequestId);
            
            if (!string.IsNullOrEmpty(retryRequest.Body))
            {
                _logger.LogInformation("   Body: {Body}", retryRequest.Body);
            }
        }
        
        return Task.CompletedTask;
    }

    public Task ProduceDelayedMessageAsync<T>(
        string targetTopic, 
        T message, 
        DateTime processAt, 
        CancellationToken cancellationToken = default)
    {
        var delay = processAt - DateTime.UtcNow;
        _logger.LogInformation("📨 DELAYED MESSAGE PRODUCED:");
        _logger.LogInformation("   Target Topic: {TargetTopic}", targetTopic);
        _logger.LogInformation("   Process At: {ProcessAt}", processAt);
        _logger.LogInformation("   Delay: {DelaySeconds} seconds", (int)delay.TotalSeconds);
        
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        // No resources to dispose in demo implementation
    }
}