using System.Text.Json;
using KafkaDelayedMessaging.Interfaces;
using KafkaDelayedMessaging.Models;
using KafkaDelayedMessaging.Services;
using Microsoft.Extensions.Logging;
using Testcontainers.Kafka;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaDelayedMessaging.Tests;

/// <summary>
/// Basic integration tests for the Kafka delayed messaging system using TestContainers.
/// Focuses on core functionality with simplified test scenarios.
/// </summary>
public class BasicIntegrationTests : IAsyncLifetime
{
    private KafkaContainer? _kafkaContainer;
    private string _bootstrapServers = string.Empty;
    private ILoggerFactory? _loggerFactory;
    private KafkaDelayConfig? _config;

    public async Task InitializeAsync()
    {
        // Start Kafka container
        _kafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.4.0")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .WithEnvironment("KAFKA_NUM_PARTITIONS", "3")
            .WithEnvironment("KAFKA_DEFAULT_REPLICATION_FACTOR", "1")
            .Build();

        await _kafkaContainer.StartAsync();
        _bootstrapServers = _kafkaContainer.GetBootstrapAddress();

        // Set up logging
        _loggerFactory = LoggerFactory.Create(builder =>
            builder.AddConsole().SetMinimumLevel(LogLevel.Information));

        // Create configuration
        _config = new KafkaDelayConfig
        {
            BootstrapServers = _bootstrapServers,
            ConsumerGroupId = "basic-test-consumer-group",
            DelayTopicPrefix = "basic-delay-",
            SessionTimeoutMs = 10000,
            MaxPollIntervalMs = 60000,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1000
        };

        // Create delay topics
        await CreateDelayTopics();
    }

    public async Task DisposeAsync()
    {
        _loggerFactory?.Dispose();
        
        if (_kafkaContainer != null)
        {
            await _kafkaContainer.DisposeAsync();
        }
    }

    private async Task CreateDelayTopics()
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        var topicSpecs = new List<TopicSpecification>
        {
            new()
            {
                Name = _config!.GetDelayTopicName(DelayDuration.Seconds5),
                NumPartitions = 3,
                ReplicationFactor = 1
            },
            new()
            {
                Name = "basic-target-topic",
                NumPartitions = 3,
                ReplicationFactor = 1
            }
        };

        try
        {
            await adminClient.CreateTopicsAsync(topicSpecs);
            await Task.Delay(2000); // Wait for topics to be created
        }
        catch (CreateTopicsException ex)
        {
            // Ignore if topics already exist
            if (!ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
            {
                throw;
            }
        }
    }

    [Fact]
    public async Task BasicDelayedMessageFlow_ShouldWork()
    {
        // Arrange
        var targetTopic = "basic-target-topic";
        var testMessage = new { Id = 1, Message = "Basic test message" };
        var delayDuration = DelayDuration.Seconds5;
        
        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();

        using var producer = new DelayedMessageProducer(_config!, producerLogger);
        using var delayConsumer = new DelayConsumer(_config!, consumerLogger);

        // Set up target topic consumer
        var receivedMessage = string.Empty;
        var messageReceived = false;
        
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "basic-target-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var targetConsumer = new ConsumerBuilder<string, string>(targetConsumerConfig).Build();
        targetConsumer.Subscribe(targetTopic);

        var targetConsumerCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var targetConsumerTask = Task.Run(async () =>
        {
            while (!targetConsumerCts.Token.IsCancellationRequested && !messageReceived)
            {
                try
                {
                    var result = targetConsumer.Consume(TimeSpan.FromSeconds(1));
                    if (result?.Message?.Value != null)
                    {
                        receivedMessage = result.Message.Value;
                        messageReceived = true;
                        break;
                    }
                }
                catch (ConsumeException)
                {
                    // Ignore consume exceptions in test
                }
            }
        });

        // Act
        var startTime = DateTime.UtcNow;
        
        // Start delay consumer
        var delayConsumerCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var delayConsumerTask = delayConsumer.StartAsync(delayDuration, delayConsumerCts.Token);

        // Produce delayed message
        await producer.ProduceDelayedMessageAsync(targetTopic, testMessage, delayDuration);

        // Wait for message to be processed
        await targetConsumerTask;

        var endTime = DateTime.UtcNow;
        var actualDelay = endTime - startTime;

        // Assert
        Assert.True(messageReceived, "Message should have been received");
        Assert.NotEmpty(receivedMessage);
        
        var deserializedMessage = JsonSerializer.Deserialize<JsonElement>(receivedMessage);
        Assert.Equal(testMessage.Id, deserializedMessage.GetProperty("id").GetInt32());
        Assert.Equal(testMessage.Message, deserializedMessage.GetProperty("message").GetString());

        // Verify timing (should be at least 4 seconds, allowing for some variance)
        Assert.True(actualDelay.TotalSeconds >= 4, $"Message delivered too early: {actualDelay.TotalSeconds}s");
        Assert.True(actualDelay.TotalSeconds <= 8, $"Message delivered too late: {actualDelay.TotalSeconds}s");

        // Cleanup
        delayConsumerCts.Cancel();
        
        try
        {
            await delayConsumerTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
    }

    [Fact]
    public async Task ProducerOnly_ShouldProduceMessageToDelayTopic()
    {
        // Arrange
        var targetTopic = "basic-target-topic";
        var testMessage = new { Id = 2, Message = "Producer only test" };
        var delayDuration = DelayDuration.Seconds5;
        
        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        using var producer = new DelayedMessageProducer(_config!, producerLogger);

        // Act & Assert - Should not throw
        await producer.ProduceDelayedMessageAsync(targetTopic, testMessage, delayDuration);
        
        // Verify message was produced to delay topic by consuming directly
        var delayTopic = _config!.GetDelayTopicName(delayDuration);
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "producer-test-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(delayTopic);

        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));
        
        Assert.NotNull(consumeResult);
        Assert.NotNull(consumeResult.Message);
        Assert.NotEmpty(consumeResult.Message.Value);
        
        // Verify headers
        Assert.NotNull(consumeResult.Message.Headers);
        Assert.True(consumeResult.Message.Headers.Any(h => h.Key == DelayHeaders.TargetTopic));
        Assert.True(consumeResult.Message.Headers.Any(h => h.Key == DelayHeaders.ProcessAt));
    }

    [Fact]
    public async Task MalformedMessage_ShouldBeSkipped()
    {
        // Arrange
        var delayTopic = _config!.GetDelayTopicName(DelayDuration.Seconds5);
        var targetTopic = "basic-target-topic";
        
        var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();
        using var delayConsumer = new DelayConsumer(_config!, consumerLogger);

        // Produce a malformed message directly to delay topic
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var rawProducer = new ProducerBuilder<string, string>(producerConfig).Build();

        var receivedMessages = new List<string>();
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "malformed-test-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var targetConsumer = new ConsumerBuilder<string, string>(targetConsumerConfig).Build();
        targetConsumer.Subscribe(targetTopic);

        var targetConsumerCts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var targetConsumerTask = Task.Run(async () =>
        {
            while (!targetConsumerCts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = targetConsumer.Consume(TimeSpan.FromSeconds(1));
                    if (result?.Message?.Value != null)
                    {
                        receivedMessages.Add(result.Message.Value);
                    }
                }
                catch (ConsumeException)
                {
                    // Ignore consume exceptions in test
                }
            }
        });

        // Act
        var delayConsumerCts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var delayConsumerTask = delayConsumer.StartAsync(DelayDuration.Seconds5, delayConsumerCts.Token);

        // Produce malformed message (missing headers)
        await rawProducer.ProduceAsync(delayTopic, new Message<string, string>
        {
            Value = JsonSerializer.Serialize(new { Id = 1, Message = "Malformed message" })
        });

        // Produce valid message after malformed one
        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        using var validProducer = new DelayedMessageProducer(_config!, producerLogger);
        await validProducer.ProduceDelayedMessageAsync(targetTopic, 
            new { Id = 2, Message = "Valid message" }, DelayDuration.Seconds5);

        // Wait for processing
        await targetConsumerTask;

        // Assert
        // Should only receive the valid message, malformed message should be skipped
        Assert.Single(receivedMessages);
        
        var receivedMessage = JsonSerializer.Deserialize<JsonElement>(receivedMessages[0]);
        Assert.Equal(2, receivedMessage.GetProperty("id").GetInt32());
        Assert.Equal("Valid message", receivedMessage.GetProperty("message").GetString());

        // Cleanup
        delayConsumerCts.Cancel();
        
        try
        {
            await delayConsumerTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
    }
}