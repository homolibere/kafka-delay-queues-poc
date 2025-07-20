This project is a POC for Kafka Delayed Messaging System

# Kafka Delayed Messaging System

A .NET 8 library that implements delayed message processing using Apache Kafka, designed for building resilient systems with automatic retry capabilities and progressive backoff patterns.

## Overview

This system enables you to schedule messages for future processing by routing them through delay-specific Kafka topics. It's particularly useful for implementing HTTP retry logic, job scheduling, and any scenario requiring time-based message processing with exponential backoff.

## Key Features

- **Progressive Backoff**: Built-in exponential backoff delays from 5 seconds to 5 days
- **HTTP Retry Integration**: Ready-to-use HTTP client with automatic retry scheduling
- **Flexible Delay Options**: Support for predefined durations or custom timestamps
- **Comprehensive Testing**: Full test suite with Kafka integration tests using Testcontainers
- **Production Ready**: Configurable Kafka settings with validation and error handling
- **Standalone Demo**: Run without Kafka to understand the retry patterns

### Running the Demo

To see the HTTP retry system in action without requiring Kafka:

```bash
dotnet run --project src/KafkaDelayedMessaging --demo
```

This demonstrates how HTTP failures trigger delayed retry messages with progressive backoff.

### Running with Kafka

1. Start a Kafka cluster (locally or use your existing cluster)
2. Update `appsettings.json` with your Kafka configuration
3. Run the full integration:

```bash
dotnet run --project src/KafkaDelayedMessaging
```

## Architecture

### Delay Duration System

The system uses predefined delay durations that follow an exponential backoff pattern:

```csharp
public enum DelayDuration
{
    Seconds5 = 5,      // 5 seconds
    Seconds10 = 10,    // 10 seconds  
    Seconds20 = 20,    // 20 seconds
    Seconds40 = 40,    // 40 seconds
    Minutes1 = 80,     // ~1.3 minutes
    Minutes3 = 160,    // ~2.7 minutes
    // ... up to Days5 = 432000 (5 days)
}
```

### Topic Structure

Messages are routed to delay-specific topics:
- `delay-5s` - Messages delayed by 5 seconds
- `delay-10s` - Messages delayed by 10 seconds
- `delay-20s` - Messages delayed by 20 seconds
- And so on...

### Core Components

1. **IDelayedMessageProducer** - Produces messages to delay topics
2. **IDelayConsumer** - Consumes messages from delay topics and forwards them when ready
3. **HttpRetryService** - HTTP client with automatic retry scheduling
4. **KafkaDelayConfig** - Configuration management with validation

## Usage Examples

### Basic Delayed Message

```csharp
// Create producer
var producer = new DelayedMessageProducer(kafkaConfig, logger);

// Schedule a message for 10 seconds from now
await producer.ProduceDelayedMessageAsync(
    "target-topic", 
    myMessage, 
    DelayDuration.Seconds10);
```

### HTTP Retry with Automatic Backoff

```csharp
var httpClient = new HttpClient();
var producer = new DelayedMessageProducer(kafkaConfig, logger);
var httpRetryService = new HttpRetryService(httpClient, producer, logger);

// This will automatically retry with progressive backoff on failures
var result = await httpRetryService.GetWithRetryAsync("https://api.example.com/data");
```

### Custom Processing Time

```csharp
// Schedule for specific timestamp
var processAt = DateTime.UtcNow.AddHours(2);
await producer.ProduceDelayedMessageAsync(
    "target-topic", 
    myMessage, 
    processAt);
```

## Configuration

Configure the system via `appsettings.json`:

```json
{
  "KafkaDelay": {
    "BootstrapServers": "localhost:9092",
    "ConsumerGroupId": "my-app-delay-consumer",
    "DelayTopicPrefix": "delay-",
    "MaxPollIntervalMs": 300000,
    "SessionTimeoutMs": 30000,
    "Acks": "all",
    "Retries": 3,
    "EnableAutoCommit": true,
    "AutoCommitIntervalMs": 5000
  }
}
```

### Key Configuration Options

- **BootstrapServers**: Kafka cluster connection string
- **ConsumerGroupId**: Base name for consumer groups (suffixed with delay topic)
- **DelayTopicPrefix**: Prefix for delay topic names (default: "delay-")
- **Acks**: Producer acknowledgment setting ("all" for maximum durability)
- **Retries**: Number of producer retry attempts

## HTTP Retry Patterns

The HTTP retry service implements intelligent retry logic:

### Retryable Conditions
- Server errors (5xx status codes)
- Rate limiting (429 status code)
- Network timeouts and connection issues
- DNS resolution failures

### Non-Retryable Conditions
- Client errors (4xx status codes except 429)
- Authentication/authorization failures
- Malformed requests

### Retry Schedule
1. First retry: 5 seconds
2. Second retry: 10 seconds  
3. Third retry: 20 seconds
4. Fourth retry: 40 seconds
5. Final retry: ~1 minute

## Testing

The project includes comprehensive tests using xUnit and Testcontainers for Kafka integration:

```bash
# Run all tests
dotnet test

# Run specific test categories
dotnet test --filter "Category=Unit"
dotnet test --filter "Category=Integration"
```

### Test Categories

- **Unit Tests**: Core logic without external dependencies
- **Integration Tests**: Full Kafka integration using Testcontainers
- **Performance Tests**: Load testing and scalability validation
- **Scalability Tests**: Consumer performance under high load

## Production Considerations

### Topic Management
- Pre-create delay topics with appropriate partition counts
- Configure retention policies based on maximum delay durations
- Monitor topic sizes and consumer lag

### Consumer Deployment
- Deploy separate consumer instances for each delay duration
- Use consumer groups for horizontal scaling
- Implement proper health checks and monitoring

### Error Handling
- Configure dead letter topics for failed messages
- Implement circuit breakers for downstream services
- Monitor retry exhaustion rates

### Performance Tuning
- Adjust batch sizes and fetch settings based on load
- Configure appropriate timeouts for your use case
- Monitor producer and consumer metrics

## Monitoring

Key metrics to monitor:

- **Producer Metrics**: Message send rate, failure rate, latency
- **Consumer Metrics**: Processing rate, lag, error rate
- **Topic Metrics**: Message count, size, retention
- **HTTP Retry Metrics**: Success rate after retries, retry exhaustion rate

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions, issues, or contributions:
- Open an issue on GitHub
- Check existing documentation and tests
- Review the demo code for usage examples