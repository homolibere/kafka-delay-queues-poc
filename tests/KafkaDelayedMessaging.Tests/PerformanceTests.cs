using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaDelayedMessaging.Models;
using KafkaDelayedMessaging.Services;
using Microsoft.Extensions.Logging;
using Testcontainers.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace KafkaDelayedMessaging.Tests;

/// <summary>
/// Performance tests to validate millisecond precision requirements and system performance characteristics.
/// These tests verify that the delayed messaging system meets performance requirements for throughput,
/// latency, and timing precision.
/// </summary>
public class PerformanceTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly ILogger<DelayedMessageProducer> _producerLogger;
    private readonly ILogger<DelayConsumer> _consumerLogger;
    private KafkaContainer? _kafkaContainer;
    private string _bootstrapServers = string.Empty;

    public PerformanceTests(ITestOutputHelper output)
    {
        _output = output;
        
        var loggerFactory = LoggerFactory.Create(builder =>
            builder.AddConsole().SetMinimumLevel(LogLevel.Information));
        
        _producerLogger = loggerFactory.CreateLogger<DelayedMessageProducer>();
        _consumerLogger = loggerFactory.CreateLogger<DelayConsumer>();
    }

    public async Task InitializeAsync()
    {
        _kafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.4.0")
            .Build();

        await _kafkaContainer.StartAsync();
        _bootstrapServers = _kafkaContainer.GetBootstrapAddress();
        
        // Create delay topics
        await CreateDelayTopics();
    }

    public async Task DisposeAsync()
    {
        if (_kafkaContainer != null)
        {
            await _kafkaContainer.DisposeAsync();
        }
    }

    /// <summary>
    /// Tests that delay precision meets millisecond-level requirements.
    /// Validates that messages are processed within acceptable timing tolerance.
    /// </summary>
    [Fact]
    public async Task DelayPrecision_ShouldMeetMillisecondRequirements()
    {
        // Arrange
        var config = CreateTestConfig();
        var producer = new DelayedMessageProducer(config, _producerLogger);
        var consumer = new DelayConsumer(config, _consumerLogger);
        
        var targetTopic = "test-target-precision";
        var testMessage = new { Id = 1, Content = "Precision test message" };
        var delayDuration = DelayDuration.Seconds5;
        
        var precisionMeasurements = new List<double>();
        var messageCount = 10;
        var maxAcceptablePrecisionMs = 100; // Allow up to 100ms deviation
        
        try
        {
            // Create target topic
            await CreateTopic(targetTopic);
            
            // Start consumer
            using var consumerCts = new CancellationTokenSource();
            var consumerTask = consumer.StartAsync(delayDuration, consumerCts.Token);
            
            // Allow consumer to initialize
            await Task.Delay(2000);
            
            // Produce test messages with precise timing
            var expectedProcessTimes = new List<DateTimeOffset>();
            for (int i = 0; i < messageCount; i++)
            {
                var expectedProcessTime = DateTimeOffset.UtcNow.AddSeconds(5);
                expectedProcessTimes.Add(expectedProcessTime);
                
                await producer.ProduceDelayedMessageAsync(
                    targetTopic, 
                    new { Id = i, Content = $"Precision test message {i}" }, 
                    delayDuration);
                
                // Small delay between messages to avoid overwhelming
                await Task.Delay(100);
            }
            
            // Monitor target topic for forwarded messages
            var targetConsumer = CreateTargetConsumer(targetTopic);
            var receivedMessages = new List<(DateTimeOffset ReceivedTime, int MessageId)>();
            
            var monitoringStopwatch = Stopwatch.StartNew();
            while (receivedMessages.Count < messageCount && monitoringStopwatch.Elapsed < TimeSpan.FromSeconds(30))
            {
                var result = targetConsumer.Consume(TimeSpan.FromSeconds(1));
                if (result?.Message != null)
                {
                    var receivedTime = DateTimeOffset.UtcNow;
                    // Extract message ID from JSON (simplified parsing)
                    var messageContent = result.Message.Value;
                    if (messageContent.Contains("\"id\":"))
                    {
                        var idStart = messageContent.IndexOf("\"id\":") + 5;
                        var idEnd = messageContent.IndexOf(",", idStart);
                        if (idEnd == -1) idEnd = messageContent.IndexOf("}", idStart);
                        
                        if (int.TryParse(messageContent.Substring(idStart, idEnd - idStart), out var messageId))
                        {
                            receivedMessages.Add((receivedTime, messageId));
                        }
                    }
                }
            }
            
            // Calculate precision measurements
            foreach (var (receivedTime, messageId) in receivedMessages)
            {
                if (messageId < expectedProcessTimes.Count)
                {
                    var expectedTime = expectedProcessTimes[messageId];
                    var precisionMs = Math.Abs((receivedTime - expectedTime).TotalMilliseconds);
                    precisionMeasurements.Add(precisionMs);
                    
                    _output.WriteLine($"Message {messageId}: Expected {expectedTime:HH:mm:ss.fff}, " +
                                    $"Received {receivedTime:HH:mm:ss.fff}, Precision: {precisionMs:F2}ms");
                }
            }
            
            // Stop consumer
            consumerCts.Cancel();
            targetConsumer.Close();
            
            // Assert precision requirements
            Assert.NotEmpty(precisionMeasurements);
            Assert.True(precisionMeasurements.Count >= messageCount * 0.8, 
                $"Should receive at least 80% of messages. Received: {precisionMeasurements.Count}/{messageCount}");
            
            var averagePrecision = precisionMeasurements.Average();
            var maxPrecision = precisionMeasurements.Max();
            
            _output.WriteLine($"Average precision: {averagePrecision:F2}ms");
            _output.WriteLine($"Max precision deviation: {maxPrecision:F2}ms");
            _output.WriteLine($"Precision measurements: [{string.Join(", ", precisionMeasurements.Select(p => $"{p:F1}ms"))}]");
            
            // Validate precision requirements
            Assert.True(averagePrecision <= maxAcceptablePrecisionMs, 
                $"Average precision {averagePrecision:F2}ms should be <= {maxAcceptablePrecisionMs}ms");
            Assert.True(maxPrecision <= maxAcceptablePrecisionMs * 2, 
                $"Max precision {maxPrecision:F2}ms should be <= {maxAcceptablePrecisionMs * 2}ms");
        }
        finally
        {
            producer.Dispose();
            consumer.Dispose();
        }
    }

    /// <summary>
    /// Tests throughput performance under load to validate system scalability.
    /// </summary>
    [Fact]
    public async Task Throughput_ShouldMeetPerformanceRequirements()
    {
        // Arrange
        var config = CreateTestConfig();
        var producer = new DelayedMessageProducer(config, _producerLogger);
        var consumer = new DelayConsumer(config, _consumerLogger);
        
        var targetTopic = "test-target-throughput";
        var delayDuration = DelayDuration.Seconds5;
        var messageCount = 100;
        var minExpectedThroughputMsgPerSec = 10; // Minimum acceptable throughput
        
        try
        {
            // Create target topic
            await CreateTopic(targetTopic);
            
            // Start consumer
            using var consumerCts = new CancellationTokenSource();
            var consumerTask = consumer.StartAsync(delayDuration, consumerCts.Token);
            
            // Allow consumer to initialize
            await Task.Delay(2000);
            
            // Measure production throughput
            var productionStopwatch = Stopwatch.StartNew();
            var productionTasks = new List<Task>();
            
            for (int i = 0; i < messageCount; i++)
            {
                var messageId = i;
                var task = producer.ProduceDelayedMessageAsync(
                    targetTopic,
                    new { Id = messageId, Content = $"Throughput test message {messageId}", Timestamp = DateTimeOffset.UtcNow },
                    delayDuration);
                productionTasks.Add(task);
            }
            
            await Task.WhenAll(productionTasks);
            productionStopwatch.Stop();
            
            var productionThroughput = messageCount / productionStopwatch.Elapsed.TotalSeconds;
            _output.WriteLine($"Production throughput: {productionThroughput:F2} messages/second");
            
            // Wait for messages to be processed and forwarded
            await Task.Delay(TimeSpan.FromSeconds((int)delayDuration + 10));
            
            // Get consumer performance metrics
            var consumerMetrics = consumer.GetPerformanceMetrics();
            var producerMetrics = producer.GetPerformanceMetrics();
            
            _output.WriteLine($"Consumer metrics:");
            _output.WriteLine($"  Messages processed: {consumerMetrics.TotalMessagesProcessed}");
            _output.WriteLine($"  Messages forwarded: {consumerMetrics.TotalMessagesForwarded}");
            _output.WriteLine($"  Throughput: {consumerMetrics.MessagesPerSecond:F2} msg/s");
            _output.WriteLine($"  Avg processing latency: {consumerMetrics.AverageProcessingLatencyMs:F2}ms");
            _output.WriteLine($"  Memory usage: {consumerMetrics.MemoryUsageBytes / 1024.0 / 1024.0:F2}MB");
            
            _output.WriteLine($"Producer metrics:");
            _output.WriteLine($"  Messages produced: {producerMetrics.TotalMessagesProduced}");
            _output.WriteLine($"  Throughput: {producerMetrics.MessagesPerSecond:F2} msg/s");
            _output.WriteLine($"  Memory usage: {producerMetrics.MemoryUsageBytes / 1024.0 / 1024.0:F2}MB");
            
            // Stop consumer
            consumerCts.Cancel();
            
            // Assert throughput requirements
            Assert.True(productionThroughput >= minExpectedThroughputMsgPerSec,
                $"Production throughput {productionThroughput:F2} should be >= {minExpectedThroughputMsgPerSec} msg/s");
            
            Assert.True(consumerMetrics.TotalMessagesProcessed >= messageCount * 0.9,
                $"Should process at least 90% of messages. Processed: {consumerMetrics.TotalMessagesProcessed}/{messageCount}");
        }
        finally
        {
            producer.Dispose();
            consumer.Dispose();
        }
    }

    /// <summary>
    /// Tests memory usage characteristics for long-running consumers.
    /// </summary>
    [Fact]
    public async Task MemoryUsage_ShouldRemainStableUnderLoad()
    {
        // Arrange
        var config = CreateTestConfig();
        var consumer = new DelayConsumer(config, _consumerLogger);
        var producer = new DelayedMessageProducer(config, _producerLogger);
        
        var targetTopic = "test-target-memory";
        var delayDuration = DelayDuration.Seconds10;
        var maxAcceptableMemoryMB = 100; // Maximum acceptable memory usage
        var testDurationSeconds = 30;
        
        try
        {
            // Create target topic
            await CreateTopic(targetTopic);
            
            // Start consumer
            using var consumerCts = new CancellationTokenSource();
            var consumerTask = consumer.StartAsync(delayDuration, consumerCts.Token);
            
            // Allow consumer to initialize
            await Task.Delay(2000);
            
            // Continuously produce messages and monitor memory
            var memoryMeasurements = new List<double>();
            var testStopwatch = Stopwatch.StartNew();
            var messageCounter = 0;
            
            while (testStopwatch.Elapsed.TotalSeconds < testDurationSeconds)
            {
                // Produce a batch of messages
                var batchTasks = new List<Task>();
                for (int i = 0; i < 10; i++)
                {
                    var task = producer.ProduceDelayedMessageAsync(
                        targetTopic,
                        new { Id = messageCounter++, Content = $"Memory test message {messageCounter}", Data = new string('x', 1000) },
                        delayDuration);
                    batchTasks.Add(task);
                }
                
                await Task.WhenAll(batchTasks);
                
                // Measure memory usage
                var metrics = consumer.GetPerformanceMetrics();
                var memoryUsageMB = metrics.MemoryUsageBytes / 1024.0 / 1024.0;
                memoryMeasurements.Add(memoryUsageMB);
                
                _output.WriteLine($"Time: {testStopwatch.Elapsed.TotalSeconds:F1}s, " +
                                $"Memory: {memoryUsageMB:F2}MB, " +
                                $"Messages produced: {messageCounter}");
                
                await Task.Delay(1000); // Wait 1 second between batches
            }
            
            // Stop consumer
            consumerCts.Cancel();
            
            // Analyze memory usage
            var averageMemory = memoryMeasurements.Average();
            var maxMemory = memoryMeasurements.Max();
            var minMemory = memoryMeasurements.Min();
            var memoryGrowth = maxMemory - minMemory;
            
            _output.WriteLine($"Memory analysis:");
            _output.WriteLine($"  Average memory: {averageMemory:F2}MB");
            _output.WriteLine($"  Max memory: {maxMemory:F2}MB");
            _output.WriteLine($"  Min memory: {minMemory:F2}MB");
            _output.WriteLine($"  Memory growth: {memoryGrowth:F2}MB");
            _output.WriteLine($"  Messages produced: {messageCounter}");
            
            // Assert memory requirements
            Assert.True(maxMemory <= maxAcceptableMemoryMB,
                $"Max memory usage {maxMemory:F2}MB should be <= {maxAcceptableMemoryMB}MB");
            
            Assert.True(memoryGrowth <= maxAcceptableMemoryMB * 0.5,
                $"Memory growth {memoryGrowth:F2}MB should be <= {maxAcceptableMemoryMB * 0.5}MB");
        }
        finally
        {
            producer.Dispose();
            consumer.Dispose();
        }
    }

    /// <summary>
    /// Tests processing latency under various load conditions.
    /// </summary>
    [Fact]
    public async Task ProcessingLatency_ShouldMeetPerformanceRequirements()
    {
        // Arrange
        var config = CreateTestConfig();
        var consumer = new DelayConsumer(config, _consumerLogger);
        var producer = new DelayedMessageProducer(config, _producerLogger);
        
        var targetTopic = "test-target-latency";
        var delayDuration = DelayDuration.Seconds5;
        var messageCount = 50;
        var maxAcceptableLatencyMs = 1000; // Maximum acceptable processing latency
        
        try
        {
            // Create target topic
            await CreateTopic(targetTopic);
            
            // Start consumer
            using var consumerCts = new CancellationTokenSource();
            var consumerTask = consumer.StartAsync(delayDuration, consumerCts.Token);
            
            // Allow consumer to initialize
            await Task.Delay(2000);
            
            // Produce messages with varying sizes to test latency
            var messageSizes = new[] { 100, 1000, 10000, 50000 }; // Different message sizes in bytes
            
            foreach (var messageSize in messageSizes)
            {
                _output.WriteLine($"Testing latency with message size: {messageSize} bytes");
                
                var largeBatch = new List<Task>();
                for (int i = 0; i < messageCount / messageSizes.Length; i++)
                {
                    var largeContent = new string('x', messageSize);
                    var task = producer.ProduceDelayedMessageAsync(
                        targetTopic,
                        new { Id = i, Content = largeContent, Size = messageSize },
                        delayDuration);
                    largeBatch.Add(task);
                }
                
                var batchStopwatch = Stopwatch.StartNew();
                await Task.WhenAll(largeBatch);
                batchStopwatch.Stop();
                
                var batchLatency = batchStopwatch.ElapsedMilliseconds / (double)(messageCount / messageSizes.Length);
                _output.WriteLine($"  Batch production latency: {batchLatency:F2}ms per message");
            }
            
            // Wait for processing
            await Task.Delay(TimeSpan.FromSeconds((int)delayDuration + 10));
            
            // Get final metrics
            var metrics = consumer.GetPerformanceMetrics();
            
            _output.WriteLine($"Final latency metrics:");
            _output.WriteLine($"  Average processing latency: {metrics.AverageProcessingLatencyMs:F2}ms");
            _output.WriteLine($"  Messages processed: {metrics.TotalMessagesProcessed}");
            _output.WriteLine($"  Average delay precision: {metrics.AverageDelayPrecisionMs:F2}ms");
            
            // Stop consumer
            consumerCts.Cancel();
            
            // Assert latency requirements
            Assert.True(metrics.AverageProcessingLatencyMs <= maxAcceptableLatencyMs,
                $"Average processing latency {metrics.AverageProcessingLatencyMs:F2}ms should be <= {maxAcceptableLatencyMs}ms");
            
            Assert.True(metrics.TotalMessagesProcessed > 0,
                "Should have processed at least some messages");
        }
        finally
        {
            producer.Dispose();
            consumer.Dispose();
        }
    }

    private KafkaDelayConfig CreateTestConfig()
    {
        return new KafkaDelayConfig
        {
            BootstrapServers = _bootstrapServers,
            DelayTopicPrefix = "test-delay-",
            ConsumerGroupId = $"test-group-{Guid.NewGuid():N}",
            EnableAutoCommit = false,
            SessionTimeoutMs = 10000,
            MaxPollIntervalMs = 30000
        };
    }

    private async Task CreateDelayTopics()
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = _bootstrapServers };
        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        var delayDurations = new[] { DelayDuration.Seconds5, DelayDuration.Seconds10 };
        var topicSpecs = delayDurations.Select(duration => new TopicSpecification
        {
            Name = $"test-delay-{(int)duration}s",
            NumPartitions = 3,
            ReplicationFactor = 1
        }).ToList();

        try
        {
            await adminClient.CreateTopicsAsync(topicSpecs);
            await Task.Delay(2000); // Wait for topics to be created
        }
        catch (CreateTopicsException ex)
        {
            // Ignore if topics already exist
            foreach (var result in ex.Results)
            {
                if (result.Error.Code != ErrorCode.TopicAlreadyExists)
                {
                    throw;
                }
            }
        }
    }

    private async Task CreateTopic(string topicName)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = _bootstrapServers };
        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        var topicSpec = new TopicSpecification
        {
            Name = topicName,
            NumPartitions = 3,
            ReplicationFactor = 1
        };

        try
        {
            await adminClient.CreateTopicsAsync(new[] { topicSpec });
            await Task.Delay(1000); // Wait for topic to be created
        }
        catch (CreateTopicsException ex)
        {
            // Ignore if topic already exists
            if (ex.Results.First().Error.Code != ErrorCode.TopicAlreadyExists)
            {
                throw;
            }
        }
    }

    private IConsumer<string, string> CreateTargetConsumer(string targetTopic)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"test-target-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(targetTopic);
        
        return consumer;
    }
}