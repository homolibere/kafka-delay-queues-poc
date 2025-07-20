using KafkaDelayedMessaging;
using KafkaDelayedMessaging.Helpers;
using KafkaDelayedMessaging.Models;
using KafkaDelayedMessaging.Services;
using KafkaDelayedMessaging.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

// Create logger factory
using var loggerFactory = ConfigurationHelper.CreateLoggerFactory(LogLevel.Information);
var logger = loggerFactory.CreateLogger<Program>();

logger.LogInformation("Kafka Delayed Messaging System - HTTP Retry Demo");

// Check command line arguments
var commandArgs = Environment.GetCommandLineArgs();
var runDemo = commandArgs.Length > 1 && commandArgs[1].Equals("--demo", StringComparison.OrdinalIgnoreCase);

if (runDemo)
{
    // Run standalone demo without requiring Kafka
    logger.LogInformation("Running standalone HTTP retry demo (no Kafka required)");
    var demoLogger = loggerFactory.CreateLogger<HttpRetryDemo>();
    var demo = new HttpRetryDemo(demoLogger);
    await demo.RunDemoAsync();
    return;
}

// Run full integration with Kafka
logger.LogInformation("Running HTTP retry integration with Kafka");

try
{
    // Create configuration - use the directory where the executable is located
    var basePath = AppContext.BaseDirectory;
    var configuration = ConfigurationHelper.CreateConfiguration(basePath);
    
    // Load and validate configuration
    var kafkaConfig = ConfigurationHelper.LoadAndValidateConfig(configuration, logger: logger);
    
    logger.LogInformation("Configuration loaded successfully");
    logger.LogInformation("Bootstrap Servers: {BootstrapServers}", kafkaConfig.BootstrapServers);
    
    // Create HTTP client with timeout
    using var httpClient = new HttpClient
    {
        Timeout = TimeSpan.FromSeconds(10)
    };
    
    // Create delayed message producer
    var producerLogger = loggerFactory.CreateLogger<DelayedMessageProducer>();
    using var delayedMessageProducer = new DelayedMessageProducer(kafkaConfig, producerLogger);
    
    // Create HTTP retry service
    var httpRetryLogger = loggerFactory.CreateLogger<HttpRetryService>();
    var httpRetryService = new HttpRetryService(httpClient, delayedMessageProducer, httpRetryLogger);
    
    // Demonstrate HTTP retry functionality
    await DemonstrateHttpRetryAsync(httpRetryService, logger);
    
    logger.LogInformation("HTTP Retry demonstration completed");
}
catch (Exception ex)
{
    logger.LogError(LogEvents.CriticalError, ex, "Failed to run HTTP Retry demonstration");
    logger.LogInformation("Tip: Run with --demo flag to see HTTP retry logic without requiring Kafka");
    Environment.Exit(1);
}

/// <summary>
/// Demonstrates HTTP retry functionality with various scenarios.
/// </summary>
static async Task DemonstrateHttpRetryAsync(HttpRetryService httpRetryService, ILogger logger)
{
    logger.LogInformation("=== HTTP Retry Demonstration ===");
    
    // Scenario 1: Successful request (no retry needed)
    logger.LogInformation("\n--- Scenario 1: Successful HTTP Request ---");
    try
    {
        var result1 = await httpRetryService.GetWithRetryAsync("https://httpbin.org/status/200");
        logger.LogInformation("Successful request result: {Result}", result1?.Substring(0, Math.Min(100, result1?.Length ?? 0)));
    }
    catch (Exception ex)
    {
        logger.LogWarning("Scenario 1 failed: {Error}", ex.Message);
    }
    
    // Scenario 2: Server error that should trigger retries
    logger.LogInformation("\n--- Scenario 2: Server Error (500) - Will Trigger Retries ---");
    try
    {
        var result2 = await httpRetryService.GetWithRetryAsync("https://httpbin.org/status/500");
        logger.LogInformation("Server error request result: {Result}", result2 ?? "null (expected)");
    }
    catch (Exception ex)
    {
        logger.LogWarning("Scenario 2 failed: {Error}", ex.Message);
    }
    
    // Scenario 3: Rate limiting that should trigger retries
    logger.LogInformation("\n--- Scenario 3: Rate Limiting (429) - Will Trigger Retries ---");
    try
    {
        var result3 = await httpRetryService.GetWithRetryAsync("https://httpbin.org/status/429");
        logger.LogInformation("Rate limited request result: {Result}", result3 ?? "null (expected)");
    }
    catch (Exception ex)
    {
        logger.LogWarning("Scenario 3 failed: {Error}", ex.Message);
    }
    
    // Scenario 4: Client error that should NOT trigger retries
    logger.LogInformation("\n--- Scenario 4: Client Error (404) - Will NOT Trigger Retries ---");
    try
    {
        var result4 = await httpRetryService.GetWithRetryAsync("https://httpbin.org/status/404");
        logger.LogInformation("Not found request result: {Result}", result4 ?? "null (expected)");
    }
    catch (Exception ex)
    {
        logger.LogWarning("Scenario 4 failed: {Error}", ex.Message);
    }
    
    // Scenario 5: POST request with retry
    logger.LogInformation("\n--- Scenario 5: POST Request with Server Error - Will Trigger Retries ---");
    try
    {
        var postData = """{"message": "test data", "timestamp": "2024-01-01T00:00:00Z"}""";
        var result5 = await httpRetryService.PostWithRetryAsync(
            "https://httpbin.org/status/503", 
            postData,
            "application/json");
        logger.LogInformation("POST request result: {Result}", result5 ?? "null (expected)");
    }
    catch (Exception ex)
    {
        logger.LogWarning("Scenario 5 failed: {Error}", ex.Message);
    }
    
    // Scenario 6: Network timeout/unreachable host
    logger.LogInformation("\n--- Scenario 6: Network Timeout - Will Trigger Retries ---");
    try
    {
        var result6 = await httpRetryService.GetWithRetryAsync("https://httpbin.org/delay/15"); // Will timeout
        logger.LogInformation("Timeout request result: {Result}", result6 ?? "null (expected)");
    }
    catch (Exception ex)
    {
        logger.LogWarning("Scenario 6 failed: {Error}", ex.Message);
    }
    
    // Scenario 7: Demonstrate progressive backoff delays
    logger.LogInformation("\n--- Scenario 7: Progressive Backoff Demonstration ---");
    logger.LogInformation("The following delays will be used for retries:");
    logger.LogInformation("  Retry 1: 5 seconds");
    logger.LogInformation("  Retry 2: 10 seconds");
    logger.LogInformation("  Retry 3: 20 seconds");
    logger.LogInformation("  Retry 4: 40 seconds");
    logger.LogInformation("  Retry 5: ~1 minute");
    
    try
    {
        var result7 = await httpRetryService.GetWithRetryAsync("https://httpbin.org/status/502");
        logger.LogInformation("Progressive backoff result: {Result}", result7 ?? "null (expected)");
    }
    catch (Exception ex)
    {
        logger.LogWarning("Scenario 7 failed: {Error}", ex.Message);
    }
    
    logger.LogInformation("\n=== Demonstration Complete ===");
    logger.LogInformation("Check the logs above to see retry messages being produced to delay topics.");
    logger.LogInformation("In a real system, delay consumers would process these messages and retry the HTTP calls.");
}
