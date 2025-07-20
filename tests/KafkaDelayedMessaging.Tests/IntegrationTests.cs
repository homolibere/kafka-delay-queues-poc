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
/// Integration tests for the Kafka delayed messaging system using TestContainers.
/// Tests end-to-end message flow, timing precision, error scenarios, and scalability.
/// </summary>
public class IntegrationTests : IAsyncLifetime
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
            builder.AddConsole().SetMinimumLevel(LogLevel.Debug));

        // Create configuration
        _config = new KafkaDelayConfig
        {
            BootstrapServers = _bootstrapServers,
            ConsumerGroupId = "test-consumer-group",
            DelayTopicPrefix = "test-delay-",
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

        var delayDurations = new[] { DelayDuration.Seconds5, DelayDuration.Seconds10, DelayDuration.Seconds20 };
        var topicSpecs = delayDurations.Select(duration => new TopicSpecification
        {
            Name = _config!.GetDelayTopicName(duration),
            NumPartitions = 3,
            ReplicationFactor = 1
        }).ToList();

        // Add target topics for testing
        topicSpecs.Add(new TopicSpecification
        {
            Name = "test-target-topic",
            NumPartitions = 3,
            ReplicationFactor = 1
        });

        topicSpecs.Add(new TopicSpecification
        {
            Name = "test-target-topic-2",
            NumPartitions = 3,
            ReplicationFactor = 1
        });

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
    public async Task EndToEndDelayedMessageFlow_ShouldDeliverMessageAfterDelay()
    {
        // Arrange
        var targetTopic = "test-target-topic";
        var testMessage = new { Id = 1, Message = "Test delayed message", Timestamp = DateTime.UtcNow };
        var delayDuration = DelayDuration.Seconds5;
        
        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();

        using var producer = new DelayedMessageProducer(_config!, producerLogger);
        using var delayConsumer = new DelayConsumer(_config!, consumerLogger);

        // Set up target topic consumer to verify message delivery
        var targetMessages = new List<string>();
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-target-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var targetConsumer = new ConsumerBuilder<string, string>(targetConsumerConfig).Build();
        targetConsumer.Subscribe(targetTopic);

        var targetConsumerCts = new CancellationTokenSource();
        var targetConsumerTask = Task.Run(async () =>
        {
            while (!targetConsumerCts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = targetConsumer.Consume(TimeSpan.FromSeconds(1));
                    if (result?.Message?.Value != null)
                    {
                        targetMessages.Add(result.Message.Value);
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
        var delayConsumerCts = new CancellationTokenSource();
        var delayConsumerTask = delayConsumer.StartAsync(delayDuration, delayConsumerCts.Token);

        // Produce delayed message
        await producer.ProduceDelayedMessageAsync(targetTopic, testMessage, delayDuration);

        // Wait for message to be processed (with some buffer)
        await Task.Delay(TimeSpan.FromSeconds(7));

        // Assert
        var endTime = DateTime.UtcNow;
        var actualDelay = endTime - startTime;

        Assert.Single(targetMessages);
        
        var receivedMessage = JsonSerializer.Deserialize<JsonElement>(targetMessages[0]);
        Assert.Equal(testMessage.Id, receivedMessage.GetProperty("id").GetInt32());
        Assert.Equal(testMessage.Message, receivedMessage.GetProperty("message").GetString());

        // Verify timing precision (should be close to 5 seconds, allowing for processing overhead)
        Assert.True(actualDelay.TotalSeconds >= 4.5, $"Message delivered too early: {actualDelay.TotalSeconds}s");
        Assert.True(actualDelay.TotalSeconds <= 6.5, $"Message delivered too late: {actualDelay.TotalSeconds}s");

        // Cleanup
        delayConsumerCts.Cancel();
        targetConsumerCts.Cancel();
        
        try
        {
            await delayConsumerTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        
        await targetConsumerTask;
    }

    [Fact]
    public async Task MultipleDelayDurations_ShouldProcessCorrectly()
    {
        // Arrange
        var targetTopic = "test-target-topic-2";
        var testMessages = new[]
        {
            new { Id = 1, DelayDuration = DelayDuration.Seconds5, Message = "5 second delay" },
            new { Id = 2, DelayDuration = DelayDuration.Seconds10, Message = "10 second delay" },
            new { Id = 3, DelayDuration = DelayDuration.Seconds5, Message = "Another 5 second delay" }
        };

        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        using var producer = new DelayedMessageProducer(_config!, producerLogger);

        // Start consumers for different delay durations
        var consumers = new List<DelayConsumer>();
        var consumerTasks = new List<Task>();
        var cancellationTokenSources = new List<CancellationTokenSource>();

        foreach (var delayDuration in new[] { DelayDuration.Seconds5, DelayDuration.Seconds10 })
        {
            var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();
            var consumer = new DelayConsumer(_config!, consumerLogger);
            var cts = new CancellationTokenSource();
            
            consumers.Add(consumer);
            cancellationTokenSources.Add(cts);
            consumerTasks.Add(consumer.StartAsync(delayDuration, cts.Token));
        }

        // Set up target topic consumer
        var targetMessages = new List<(string Message, DateTime ReceivedAt)>();
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-target-consumer-2",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var targetConsumer = new ConsumerBuilder<string, string>(targetConsumerConfig).Build();
        targetConsumer.Subscribe(targetTopic);

        var targetConsumerCts = new CancellationTokenSource();
        var targetConsumerTask = Task.Run(async () =>
        {
            while (!targetConsumerCts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = targetConsumer.Consume(TimeSpan.FromSeconds(1));
                    if (result?.Message?.Value != null)
                    {
                        targetMessages.Add((result.Message.Value, DateTime.UtcNow));
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
        
        // Produce messages with different delays
        foreach (var testMessage in testMessages)
        {
            await producer.ProduceDelayedMessageAsync(targetTopic, testMessage, testMessage.DelayDuration);
        }

        // Wait for all messages to be processed
        await Task.Delay(TimeSpan.FromSeconds(12));

        // Assert
        Assert.Equal(3, targetMessages.Count);

        // Verify messages arrived in correct order (5s messages first, then 10s message)
        var orderedMessages = targetMessages.OrderBy(m => m.ReceivedAt).ToList();
        
        var firstMessage = JsonSerializer.Deserialize<JsonElement>(orderedMessages[0].Message);
        var secondMessage = JsonSerializer.Deserialize<JsonElement>(orderedMessages[1].Message);
        var thirdMessage = JsonSerializer.Deserialize<JsonElement>(orderedMessages[2].Message);

        // First two messages should be the 5-second delay messages
        Assert.True(firstMessage.GetProperty("id").GetInt32() == 1 || firstMessage.GetProperty("id").GetInt32() == 3);
        Assert.True(secondMessage.GetProperty("id").GetInt32() == 1 || secondMessage.GetProperty("id").GetInt32() == 3);
        Assert.NotEqual(firstMessage.GetProperty("id").GetInt32(), secondMessage.GetProperty("id").GetInt32());

        // Third message should be the 10-second delay message
        Assert.Equal(2, thirdMessage.GetProperty("id").GetInt32());

        // Verify timing - first two messages should arrive around 5s, third around 10s
        var firstDelay = orderedMessages[0].ReceivedAt - startTime;
        var thirdDelay = orderedMessages[2].ReceivedAt - startTime;

        Assert.True(firstDelay.TotalSeconds >= 4.5 && firstDelay.TotalSeconds <= 6.5);
        Assert.True(thirdDelay.TotalSeconds >= 9.5 && thirdDelay.TotalSeconds <= 11.5);

        // Cleanup
        foreach (var cts in cancellationTokenSources)
        {
            cts.Cancel();
        }
        targetConsumerCts.Cancel();

        foreach (var consumer in consumers)
        {
            consumer.Dispose();
        }

        await targetConsumerTask;
    }

    [Fact]
    public async Task MalformedHeaders_ShouldSkipMessageAndContinueProcessing()
    {
        // Arrange
        var delayTopic = _config!.GetDelayTopicName(DelayDuration.Seconds5);
        var targetTopic = "test-target-topic";
        
        var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();
        using var delayConsumer = new DelayConsumer(_config!, consumerLogger);

        // Produce a message with malformed headers directly to delay topic
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var rawProducer = new ProducerBuilder<string, string>(producerConfig).Build();

        // Set up target topic consumer
        var targetMessages = new List<string>();
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-malformed-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var targetConsumer = new ConsumerBuilder<string, string>(targetConsumerConfig).Build();
        targetConsumer.Subscribe(targetTopic);

        var targetConsumerCts = new CancellationTokenSource();
        var targetConsumerTask = Task.Run(async () =>
        {
            while (!targetConsumerCts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = targetConsumer.Consume(TimeSpan.FromSeconds(1));
                    if (result?.Message?.Value != null)
                    {
                        targetMessages.Add(result.Message.Value);
                    }
                }
                catch (ConsumeException)
                {
                    // Ignore consume exceptions in test
                }
            }
        });

        // Act
        var delayConsumerCts = new CancellationTokenSource();
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
        await Task.Delay(TimeSpan.FromSeconds(7));

        // Assert
        // Should only receive the valid message, malformed message should be skipped
        Assert.Single(targetMessages);
        
        var receivedMessage = JsonSerializer.Deserialize<JsonElement>(targetMessages[0]);
        Assert.Equal(2, receivedMessage.GetProperty("id").GetInt32());
        Assert.Equal("Valid message", receivedMessage.GetProperty("message").GetString());

        // Cleanup
        delayConsumerCts.Cancel();
        targetConsumerCts.Cancel();
        
        try
        {
            await delayConsumerTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        
        await targetConsumerTask;
    }

    [Fact]
    public async Task ConsumerScalability_MultipleConsumersShouldProcessMessages()
    {
        // Arrange
        var targetTopic = "test-target-topic";
        var messageCount = 10;
        var delayDuration = DelayDuration.Seconds5;

        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        using var producer = new DelayedMessageProducer(_config!, producerLogger);

        // Start multiple consumers for the same delay duration
        var consumers = new List<DelayConsumer>();
        var consumerTasks = new List<Task>();
        var cancellationTokenSources = new List<CancellationTokenSource>();

        for (int i = 0; i < 3; i++) // 3 consumers
        {
            var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();
            var consumer = new DelayConsumer(_config!, consumerLogger);
            var cts = new CancellationTokenSource();
            
            consumers.Add(consumer);
            cancellationTokenSources.Add(cts);
            consumerTasks.Add(consumer.StartAsync(delayDuration, cts.Token));
        }

        // Set up target topic consumer
        var targetMessages = new List<string>();
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-scalability-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var targetConsumer = new ConsumerBuilder<string, string>(targetConsumerConfig).Build();
        targetConsumer.Subscribe(targetTopic);

        var targetConsumerCts = new CancellationTokenSource();
        var targetConsumerTask = Task.Run(async () =>
        {
            while (!targetConsumerCts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = targetConsumer.Consume(TimeSpan.FromSeconds(1));
                    if (result?.Message?.Value != null)
                    {
                        targetMessages.Add(result.Message.Value);
                    }
                }
                catch (ConsumeException)
                {
                    // Ignore consume exceptions in test
                }
            }
        });

        // Act
        // Produce multiple messages
        var produceTasks = new List<Task>();
        for (int i = 0; i < messageCount; i++)
        {
            var messageId = i;
            produceTasks.Add(producer.ProduceDelayedMessageAsync(targetTopic, 
                new { Id = messageId, Message = $"Scalability test message {messageId}" }, delayDuration));
        }

        await Task.WhenAll(produceTasks);

        // Wait for all messages to be processed
        await Task.Delay(TimeSpan.FromSeconds(8));

        // Assert
        Assert.Equal(messageCount, targetMessages.Count);

        // Verify all messages were received
        var receivedIds = new HashSet<int>();
        foreach (var messageJson in targetMessages)
        {
            var message = JsonSerializer.Deserialize<JsonElement>(messageJson);
            receivedIds.Add(message.GetProperty("id").GetInt32());
        }

        Assert.Equal(messageCount, receivedIds.Count);
        for (int i = 0; i < messageCount; i++)
        {
            Assert.Contains(i, receivedIds);
        }

        // Cleanup
        foreach (var cts in cancellationTokenSources)
        {
            cts.Cancel();
        }
        targetConsumerCts.Cancel();

        foreach (var consumer in consumers)
        {
            consumer.Dispose();
        }

        await targetConsumerTask;
    }

    [Fact]
    public async Task TimingPrecision_ShouldDeliverWithMillisecondPrecision()
    {
        // Arrange
        var targetTopic = "test-target-topic";
        var testMessage = new { Id = 1, Message = "Precision test" };
        var processAt = DateTime.UtcNow.AddSeconds(3); // 3 seconds from now

        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();

        using var producer = new DelayedMessageProducer(_config!, producerLogger);
        using var delayConsumer = new DelayConsumer(_config!, consumerLogger);

        // Set up target topic consumer with precise timing
        var messageReceivedAt = DateTime.MinValue;
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-precision-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var targetConsumer = new ConsumerBuilder<string, string>(targetConsumerConfig).Build();
        targetConsumer.Subscribe(targetTopic);

        var targetConsumerCts = new CancellationTokenSource();
        var targetConsumerTask = Task.Run(async () =>
        {
            while (!targetConsumerCts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = targetConsumer.Consume(TimeSpan.FromSeconds(1));
                    if (result?.Message?.Value != null)
                    {
                        messageReceivedAt = DateTime.UtcNow;
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
        var delayConsumerCts = new CancellationTokenSource();
        var delayConsumerTask = delayConsumer.StartAsync(DelayDuration.Seconds5, delayConsumerCts.Token);

        await producer.ProduceDelayedMessageAsync(targetTopic, testMessage, processAt);

        // Wait for message processing
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert
        Assert.NotEqual(DateTime.MinValue, messageReceivedAt);

        var actualDelay = messageReceivedAt - processAt;
        
        // Should be delivered within 500ms of the target time (allowing for processing overhead)
        Assert.True(Math.Abs(actualDelay.TotalMilliseconds) <= 500, 
            $"Message timing precision failed. Expected around {processAt}, got {messageReceivedAt}, difference: {actualDelay.TotalMilliseconds}ms");

        // Cleanup
        delayConsumerCts.Cancel();
        targetConsumerCts.Cancel();
        
        try
        {
            await delayConsumerTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        
        await targetConsumerTask;
    }

    [Fact]
    public async Task UnreachableTargetTopic_ShouldLogErrorAndContinueProcessing()
    {
        // Arrange
        var unreachableTargetTopic = "non-existent-topic";
        var reachableTargetTopic = "test-target-topic";
        var testMessage1 = new { Id = 1, Message = "Message to unreachable topic" };
        var testMessage2 = new { Id = 2, Message = "Message to reachable topic" };

        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();

        using var producer = new DelayedMessageProducer(_config!, producerLogger);
        using var delayConsumer = new DelayConsumer(_config!, consumerLogger);

        // Set up target topic consumer for reachable topic
        var targetMessages = new List<string>();
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-unreachable-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var targetConsumer = new ConsumerBuilder<string, string>(targetConsumerConfig).Build();
        targetConsumer.Subscribe(reachableTargetTopic);

        var targetConsumerCts = new CancellationTokenSource();
        var targetConsumerTask = Task.Run(async () =>
        {
            while (!targetConsumerCts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = targetConsumer.Consume(TimeSpan.FromSeconds(1));
                    if (result?.Message?.Value != null)
                    {
                        targetMessages.Add(result.Message.Value);
                    }
                }
                catch (ConsumeException)
                {
                    // Ignore consume exceptions in test
                }
            }
        });

        // Act
        var delayConsumerCts = new CancellationTokenSource();
        var delayConsumerTask = delayConsumer.StartAsync(DelayDuration.Seconds5, delayConsumerCts.Token);

        // Produce message to unreachable topic first
        await producer.ProduceDelayedMessageAsync(unreachableTargetTopic, testMessage1, DelayDuration.Seconds5);
        
        // Then produce message to reachable topic
        await producer.ProduceDelayedMessageAsync(reachableTargetTopic, testMessage2, DelayDuration.Seconds5);

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(7));

        // Assert
        // Should receive the message to the reachable topic despite the error with unreachable topic
        Assert.Single(targetMessages);
        
        var receivedMessage = JsonSerializer.Deserialize<JsonElement>(targetMessages[0]);
        Assert.Equal(2, receivedMessage.GetProperty("id").GetInt32());
        Assert.Equal("Message to reachable topic", receivedMessage.GetProperty("message").GetString());

        // Cleanup
        delayConsumerCts.Cancel();
        targetConsumerCts.Cancel();
        
        try
        {
            await delayConsumerTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        
        await targetConsumerTask;
    }
}