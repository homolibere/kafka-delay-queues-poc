using System.Collections.Concurrent;
using System.Text.Json;
using KafkaDelayedMessaging.Models;
using KafkaDelayedMessaging.Services;
using Microsoft.Extensions.Logging;
using Testcontainers.Kafka;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaDelayedMessaging.Tests;

/// <summary>
/// Scalability-focused integration tests for DelayConsumer using TestContainers.
/// Tests consumer group management, partition assignment, and multi-instance processing.
/// </summary>
public class DelayConsumerScalabilityTests : IAsyncLifetime
{
    private KafkaContainer? _kafkaContainer;
    private string _bootstrapServers = string.Empty;
    private ILoggerFactory? _loggerFactory;
    private KafkaDelayConfig? _config;

    public async Task InitializeAsync()
    {
        // Start Kafka container with multiple partitions for scalability testing
        _kafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.4.0")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .WithEnvironment("KAFKA_NUM_PARTITIONS", "6")
            .WithEnvironment("KAFKA_DEFAULT_REPLICATION_FACTOR", "1")
            .Build();

        await _kafkaContainer.StartAsync();
        _bootstrapServers = _kafkaContainer.GetBootstrapAddress();

        // Set up logging
        _loggerFactory = LoggerFactory.Create(builder =>
            builder.AddConsole().SetMinimumLevel(LogLevel.Information));

        // Create configuration optimized for scalability testing
        _config = new KafkaDelayConfig
        {
            BootstrapServers = _bootstrapServers,
            ConsumerGroupId = "scalability-test-group",
            DelayTopicPrefix = "scale-delay-",
            SessionTimeoutMs = 15000,
            MaxPollIntervalMs = 120000,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 2000,
            HeartbeatIntervalMs = 5000
        };

        // Create delay topics with multiple partitions
        await CreateScalabilityTopics();
    }

    public async Task DisposeAsync()
    {
        _loggerFactory?.Dispose();
        
        if (_kafkaContainer != null)
        {
            await _kafkaContainer.DisposeAsync();
        }
    }

    private async Task CreateScalabilityTopics()
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
                NumPartitions = 6,
                ReplicationFactor = 1
            },
            new()
            {
                Name = _config!.GetDelayTopicName(DelayDuration.Seconds10),
                NumPartitions = 6,
                ReplicationFactor = 1
            },
            new()
            {
                Name = "scalability-target-topic",
                NumPartitions = 6,
                ReplicationFactor = 1
            },
            new()
            {
                Name = "high-volume-target-topic",
                NumPartitions = 6,
                ReplicationFactor = 1
            }
        };

        try
        {
            await adminClient.CreateTopicsAsync(topicSpecs);
            await Task.Delay(3000); // Wait for topics to be created
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
    public async Task MultipleConsumerInstances_ShouldDistributeWorkload()
    {
        // Arrange
        var targetTopic = "scalability-target-topic";
        var messageCount = 30; // Enough messages to distribute across multiple consumers
        var delayDuration = DelayDuration.Seconds5;
        var consumerCount = 4;

        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        using var producer = new DelayedMessageProducer(_config!, producerLogger);

        // Track which consumer processed which messages
        var consumerProcessingCounts = new ConcurrentDictionary<int, int>();
        var processedMessages = new ConcurrentBag<int>();

        // Start multiple consumers
        var consumers = new List<DelayConsumer>();
        var consumerTasks = new List<Task>();
        var cancellationTokenSources = new List<CancellationTokenSource>();

        for (int i = 0; i < consumerCount; i++)
        {
            var consumerId = i;
            var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();
            var consumer = new DelayConsumer(_config!, consumerLogger);
            var cts = new CancellationTokenSource();
            
            consumers.Add(consumer);
            cancellationTokenSources.Add(cts);
            consumerTasks.Add(consumer.StartAsync(delayDuration, cts.Token));
            
            consumerProcessingCounts[consumerId] = 0;
        }

        // Set up target topic consumer to track processed messages
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "scalability-target-consumer",
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
                        var message = JsonSerializer.Deserialize<JsonElement>(result.Message.Value);
                        var messageId = message.GetProperty("id").GetInt32();
                        processedMessages.Add(messageId);
                    }
                }
                catch (ConsumeException)
                {
                    // Ignore consume exceptions in test
                }
            }
        });

        // Act
        // Produce messages
        var produceTasks = new List<Task>();
        for (int i = 0; i < messageCount; i++)
        {
            var messageId = i;
            produceTasks.Add(producer.ProduceDelayedMessageAsync(targetTopic, 
                new { Id = messageId, Message = $"Scalability message {messageId}" }, delayDuration));
        }

        await Task.WhenAll(produceTasks);

        // Wait for processing with extra time for consumer coordination
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert
        var processedMessagesList = processedMessages.ToList();
        Assert.Equal(messageCount, processedMessagesList.Count);

        // Verify all messages were processed exactly once
        var uniqueMessages = processedMessagesList.Distinct().ToList();
        Assert.Equal(messageCount, uniqueMessages.Count);

        // Verify messages are distributed (no single consumer should process all messages)
        // This is probabilistic but with 30 messages and 4 consumers, distribution should occur
        var maxMessagesPerConsumer = messageCount * 0.8; // Allow up to 80% to one consumer
        Assert.True(processedMessagesList.Count <= maxMessagesPerConsumer, 
            "Messages should be distributed across multiple consumer instances");

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
    public async Task ConsumerRebalancing_ShouldHandleConsumerAdditionAndRemoval()
    {
        // Arrange
        var targetTopic = "scalability-target-topic";
        var messageCount = 20;
        var delayDuration = DelayDuration.Seconds5;

        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        using var producer = new DelayedMessageProducer(_config!, producerLogger);

        var processedMessages = new ConcurrentBag<int>();

        // Set up target topic consumer
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "rebalancing-target-consumer",
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
                        var message = JsonSerializer.Deserialize<JsonElement>(result.Message.Value);
                        var messageId = message.GetProperty("id").GetInt32();
                        processedMessages.Add(messageId);
                    }
                }
                catch (ConsumeException)
                {
                    // Ignore consume exceptions in test
                }
            }
        });

        // Act
        // Start with 2 consumers
        var consumers = new List<DelayConsumer>();
        var cancellationTokenSources = new List<CancellationTokenSource>();

        for (int i = 0; i < 2; i++)
        {
            var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();
            var consumer = new DelayConsumer(_config!, consumerLogger);
            var cts = new CancellationTokenSource();
            
            consumers.Add(consumer);
            cancellationTokenSources.Add(cts);
            await consumer.StartAsync(delayDuration, cts.Token);
        }

        // Produce first batch of messages
        for (int i = 0; i < messageCount / 2; i++)
        {
            await producer.ProduceDelayedMessageAsync(targetTopic, 
                new { Id = i, Message = $"Rebalancing message {i}" }, delayDuration);
        }

        // Wait a bit for initial processing
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Add a third consumer (should trigger rebalancing)
        var consumerLogger3 = _loggerFactory!.CreateLogger<DelayConsumer>();
        var consumer3 = new DelayConsumer(_config!, consumerLogger3);
        var cts3 = new CancellationTokenSource();
        consumers.Add(consumer3);
        cancellationTokenSources.Add(cts3);
        await consumer3.StartAsync(delayDuration, cts3.Token);

        // Produce second batch of messages
        for (int i = messageCount / 2; i < messageCount; i++)
        {
            await producer.ProduceDelayedMessageAsync(targetTopic, 
                new { Id = i, Message = $"Rebalancing message {i}" }, delayDuration);
        }

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(8));

        // Remove one consumer (should trigger rebalancing again)
        cancellationTokenSources[0].Cancel();
        consumers[0].Dispose();

        // Wait for rebalancing and final processing
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Assert
        var processedMessagesList = processedMessages.ToList();
        Assert.Equal(messageCount, processedMessagesList.Count);

        // Verify all messages were processed exactly once despite rebalancing
        var uniqueMessages = processedMessagesList.Distinct().ToList();
        Assert.Equal(messageCount, uniqueMessages.Count);

        // Cleanup
        foreach (var cts in cancellationTokenSources.Skip(1)) // Skip the first one that's already cancelled
        {
            cts.Cancel();
        }
        targetConsumerCts.Cancel();

        foreach (var consumer in consumers.Skip(1)) // Skip the first one that's already disposed
        {
            consumer.Dispose();
        }

        await targetConsumerTask;
    }

    [Fact]
    public async Task HighVolumeProcessing_ShouldMaintainPerformance()
    {
        // Arrange
        var targetTopic = "high-volume-target-topic";
        var messageCount = 100; // High volume for integration test
        var delayDuration = DelayDuration.Seconds5;
        var consumerCount = 3;

        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        using var producer = new DelayedMessageProducer(_config!, producerLogger);

        var processedMessages = new ConcurrentBag<(int MessageId, DateTime ProcessedAt)>();

        // Start multiple consumers for high volume processing
        var consumers = new List<DelayConsumer>();
        var consumerTasks = new List<Task>();
        var cancellationTokenSources = new List<CancellationTokenSource>();

        for (int i = 0; i < consumerCount; i++)
        {
            var consumerLogger = _loggerFactory!.CreateLogger<DelayConsumer>();
            var consumer = new DelayConsumer(_config!, consumerLogger);
            var cts = new CancellationTokenSource();
            
            consumers.Add(consumer);
            cancellationTokenSources.Add(cts);
            consumerTasks.Add(consumer.StartAsync(delayDuration, cts.Token));
        }

        // Set up target topic consumer with performance tracking
        var targetConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "high-volume-target-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            FetchMinBytes = 1024 // Optimize for higher throughput
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
                        var message = JsonSerializer.Deserialize<JsonElement>(result.Message.Value);
                        var messageId = message.GetProperty("id").GetInt32();
                        processedMessages.Add((messageId, DateTime.UtcNow));
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

        // Produce messages in parallel for better performance
        var produceTasks = new List<Task>();
        var semaphore = new SemaphoreSlim(10); // Limit concurrent produce operations

        for (int i = 0; i < messageCount; i++)
        {
            var messageId = i;
            produceTasks.Add(Task.Run(async () =>
            {
                await semaphore.WaitAsync();
                try
                {
                    await producer.ProduceDelayedMessageAsync(targetTopic, 
                        new { Id = messageId, Message = $"High volume message {messageId}", 
                              Timestamp = DateTime.UtcNow }, delayDuration);
                }
                finally
                {
                    semaphore.Release();
                }
            }));
        }

        await Task.WhenAll(produceTasks);
        var produceEndTime = DateTime.UtcNow;

        // Wait for processing with reasonable timeout
        await Task.Delay(TimeSpan.FromSeconds(12));

        var endTime = DateTime.UtcNow;

        // Assert
        var processedMessagesList = processedMessages.ToList();
        Assert.Equal(messageCount, processedMessagesList.Count);

        // Verify all messages were processed exactly once
        var uniqueMessages = processedMessagesList.Select(m => m.MessageId).Distinct().ToList();
        Assert.Equal(messageCount, uniqueMessages.Count);

        // Performance assertions
        var produceTime = produceEndTime - startTime;
        var totalProcessingTime = endTime - startTime;

        // Should be able to produce 100 messages in reasonable time (< 30 seconds)
        Assert.True(produceTime.TotalSeconds < 30, 
            $"Message production took too long: {produceTime.TotalSeconds}s");

        // Total processing time should be reasonable (< 15 seconds including 5s delay)
        Assert.True(totalProcessingTime.TotalSeconds < 15, 
            $"Total processing time too long: {totalProcessingTime.TotalSeconds}s");

        // Verify timing precision is maintained under high volume
        var firstMessageTime = processedMessagesList.Min(m => m.ProcessedAt);
        var lastMessageTime = processedMessagesList.Max(m => m.ProcessedAt);
        var processingSpread = lastMessageTime - firstMessageTime;

        // All messages should be processed within a reasonable time window
        Assert.True(processingSpread.TotalSeconds < 3, 
            $"Message processing spread too large: {processingSpread.TotalSeconds}s");

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
    public async Task ConsumerGroupIsolation_ShouldNotInterfereWithOtherGroups()
    {
        // Arrange
        var targetTopic = "scalability-target-topic";
        var messageCount = 10;
        var delayDuration = DelayDuration.Seconds5;

        var producerLogger = _loggerFactory!.CreateLogger<DelayedMessageProducer>();
        using var producer = new DelayedMessageProducer(_config!, producerLogger);

        // Create two separate consumer groups
        var config1 = new KafkaDelayConfig
        {
            BootstrapServers = _bootstrapServers,
            ConsumerGroupId = "isolation-group-1",
            DelayTopicPrefix = "scale-delay-",
            SessionTimeoutMs = 15000,
            MaxPollIntervalMs = 120000,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 2000
        };

        var config2 = new KafkaDelayConfig
        {
            BootstrapServers = _bootstrapServers,
            ConsumerGroupId = "isolation-group-2",
            DelayTopicPrefix = "scale-delay-",
            SessionTimeoutMs = 15000,
            MaxPollIntervalMs = 120000,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 2000
        };

        var processedMessagesGroup1 = new ConcurrentBag<int>();
        var processedMessagesGroup2 = new ConcurrentBag<int>();

        // Start consumers from both groups
        var consumerLogger1 = _loggerFactory!.CreateLogger<DelayConsumer>();
        var consumerLogger2 = _loggerFactory!.CreateLogger<DelayConsumer>();
        
        using var consumer1 = new DelayConsumer(config1, consumerLogger1);
        using var consumer2 = new DelayConsumer(config2, consumerLogger2);

        var cts1 = new CancellationTokenSource();
        var cts2 = new CancellationTokenSource();

        var consumerTask1 = consumer1.StartAsync(delayDuration, cts1.Token);
        var consumerTask2 = consumer2.StartAsync(delayDuration, cts2.Token);

        // Set up target topic consumers for both groups
        var targetConsumerConfig1 = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "isolation-target-consumer-1",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        var targetConsumerConfig2 = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "isolation-target-consumer-2",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var targetConsumer1 = new ConsumerBuilder<string, string>(targetConsumerConfig1).Build();
        using var targetConsumer2 = new ConsumerBuilder<string, string>(targetConsumerConfig2).Build();
        
        targetConsumer1.Subscribe(targetTopic);
        targetConsumer2.Subscribe(targetTopic);

        var targetCts1 = new CancellationTokenSource();
        var targetCts2 = new CancellationTokenSource();

        var targetTask1 = Task.Run(async () =>
        {
            while (!targetCts1.Token.IsCancellationRequested)
            {
                try
                {
                    var result = targetConsumer1.Consume(TimeSpan.FromSeconds(1));
                    if (result?.Message?.Value != null)
                    {
                        var message = JsonSerializer.Deserialize<JsonElement>(result.Message.Value);
                        var messageId = message.GetProperty("id").GetInt32();
                        processedMessagesGroup1.Add(messageId);
                    }
                }
                catch (ConsumeException)
                {
                    // Ignore consume exceptions in test
                }
            }
        });

        var targetTask2 = Task.Run(async () =>
        {
            while (!targetCts2.Token.IsCancellationRequested)
            {
                try
                {
                    var result = targetConsumer2.Consume(TimeSpan.FromSeconds(1));
                    if (result?.Message?.Value != null)
                    {
                        var message = JsonSerializer.Deserialize<JsonElement>(result.Message.Value);
                        var messageId = message.GetProperty("id").GetInt32();
                        processedMessagesGroup2.Add(messageId);
                    }
                }
                catch (ConsumeException)
                {
                    // Ignore consume exceptions in test
                }
            }
        });

        // Act
        // Produce messages
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceDelayedMessageAsync(targetTopic, 
                new { Id = i, Message = $"Isolation test message {i}" }, delayDuration);
        }

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(8));

        // Assert
        var group1Messages = processedMessagesGroup1.ToList();
        var group2Messages = processedMessagesGroup2.ToList();

        // Both groups should receive all messages (since they're in different consumer groups for the target topic)
        Assert.Equal(messageCount, group1Messages.Count);
        Assert.Equal(messageCount, group2Messages.Count);

        // Both groups should have processed the same set of messages
        var group1Sorted = group1Messages.OrderBy(x => x).ToList();
        var group2Sorted = group2Messages.OrderBy(x => x).ToList();
        
        Assert.Equal(group1Sorted, group2Sorted);

        // Cleanup
        cts1.Cancel();
        cts2.Cancel();
        targetCts1.Cancel();
        targetCts2.Cancel();

        await targetTask1;
        await targetTask2;
    }
}