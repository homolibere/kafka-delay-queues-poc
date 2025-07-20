using KafkaDelayedMessaging.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.ComponentModel.DataAnnotations;

namespace KafkaDelayedMessaging.Helpers;

/// <summary>
/// Helper class for configuration management and logging setup in the delayed messaging system.
/// </summary>
public static class ConfigurationHelper
{
    /// <summary>
    /// Creates and configures a logger factory with console logging.
    /// </summary>
    /// <param name="logLevel">Minimum log level to capture</param>
    /// <returns>Configured ILoggerFactory instance</returns>
    public static ILoggerFactory CreateLoggerFactory(LogLevel logLevel = LogLevel.Information)
    {
        return LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(logLevel)
                .AddConsole();
        });
    }

    /// <summary>
    /// Loads KafkaDelayConfig from configuration sources with validation.
    /// </summary>
    /// <param name="configuration">Configuration instance</param>
    /// <param name="sectionName">Configuration section name (default: "KafkaDelay")</param>
    /// <param name="logger">Optional logger for validation messages</param>
    /// <returns>Validated KafkaDelayConfig instance</returns>
    /// <exception cref="ValidationException">Thrown when configuration is invalid</exception>
    public static KafkaDelayConfig LoadAndValidateConfig(
        IConfiguration configuration, 
        string sectionName = "KafkaDelay",
        ILogger? logger = null)
    {
        var config = new KafkaDelayConfig();
        configuration.GetSection(sectionName).Bind(config);

        try
        {
            config.Validate();
            logger?.LogInformation(LogEvents.ConfigurationValidated, 
                "KafkaDelayConfig validated successfully. BootstrapServers: {BootstrapServers}, ConsumerGroupId: {ConsumerGroupId}",
                config.BootstrapServers, config.ConsumerGroupId);
            
            return config;
        }
        catch (ValidationException ex)
        {
            logger?.LogError(LogEvents.ConfigurationError, ex,
                "Configuration validation failed: {ErrorMessage}", ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Creates a default configuration for development/testing purposes.
    /// </summary>
    /// <param name="bootstrapServers">Kafka bootstrap servers</param>
    /// <param name="consumerGroupId">Consumer group ID</param>
    /// <returns>KafkaDelayConfig with default values</returns>
    public static KafkaDelayConfig CreateDefaultConfig(
        string bootstrapServers = "localhost:9092", 
        string consumerGroupId = "kafka-delay-consumer")
    {
        return new KafkaDelayConfig
        {
            BootstrapServers = bootstrapServers,
            ConsumerGroupId = consumerGroupId,
            DelayTopicPrefix = "delay-",
            MaxPollIntervalMs = 300000,
            SessionTimeoutMs = 30000,
            Acks = "all",
            Retries = 3,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 5000
        };
    }

    /// <summary>
    /// Validates configuration and logs the result.
    /// </summary>
    /// <param name="config">Configuration to validate</param>
    /// <param name="logger">Logger instance</param>
    /// <returns>True if valid, false otherwise</returns>
    public static bool TryValidateConfig(KafkaDelayConfig config, ILogger logger)
    {
        try
        {
            config.Validate();
            logger.LogInformation(LogEvents.ConfigurationValidated,
                "Configuration validation successful for ConsumerGroupId: {ConsumerGroupId}",
                config.ConsumerGroupId);
            return true;
        }
        catch (ValidationException ex)
        {
            logger.LogError(LogEvents.ConfigurationError, ex,
                "Configuration validation failed: {ErrorMessage}", ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Creates an IConfiguration instance from appsettings.json and environment variables.
    /// </summary>
    /// <param name="basePath">Base path for configuration files</param>
    /// <returns>Configured IConfiguration instance</returns>
    public static IConfiguration CreateConfiguration(string? basePath = null)
    {
        var builder = new ConfigurationBuilder();
        
        if (!string.IsNullOrEmpty(basePath))
        {
            builder.SetBasePath(basePath);
        }

        return builder
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production"}.json", 
                optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();
    }
}