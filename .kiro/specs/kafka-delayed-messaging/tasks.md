# Implementation Plan

- [x] 1. Set up project structure and core models
  - Create .NET 8 console application project with central package management (Directory.Packages.props)
  - Add Confluent.Kafka and Microsoft.Extensions.Logging NuGet packages via central package management
  - Put sources in /src folder and tests to /tests folder
  - Define DelayDuration enum with predefined values (5s, 10s, 20s... up to 5 days)
  - Create DelayHeaders static class with header constant definitions
  - _Requirements: 4.1, 4.2, 1.2, 1.4_

- [x] 2. Implement configuration and logging infrastructure
  - Create KafkaDelayConfig class with Kafka connection settings
  - Set up structured logging with Microsoft.Extensions.Logging
  - Define LogEvents static class with event IDs for different scenarios
  - Create configuration validation methods
  - _Requirements: 4.1, 3.1, 3.2_

- [x] 3. Create delayed message producer implementation
  - Implement IDelayedMessageProducer interface with Confluent.Kafka producer
  - Add method to select appropriate delay topic based on DelayDuration enum
  - Implement JSON serialization for message payloads
  - Add logic to set message headers with target topic and processing timestamp
  - Create unit tests for producer functionality
  - _Requirements: 1.1, 1.3, 1.4, 1.5, 6.2_

- [x] 4. Implement delay consumer core logic
  - Create IDelayConsumer interface and implementation using Confluent.Kafka consumer
  - Add message consumption loop with proper error handling
  - Implement header parsing logic to extract target topic and processing timestamp
  - Add timing logic to calculate when message should be processed
  - Create unit tests for consumer message processing logic
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.3_

- [x] 5. Add message forwarding and sleep functionality
  - Implement message forwarding to target topics when processing time arrives
  - Add sleep logic using Task.Delay with CancellationToken support
  - Implement proper offset commit after successful message forwarding
  - Add error handling for target topic unreachability
  - Create unit tests for forwarding and timing functionality
  - _Requirements: 2.3, 2.4, 2.5, 3.4_

- [x] 6. Implement comprehensive error handling and logging
  - Add try-catch blocks for all critical operations with specific exception handling
  - Implement logging for message production, consumption, and forwarding events
  - Add error logging for malformed headers, serialization failures, and Kafka errors
  - Create fail-fast behavior for critical errors while continuing processing for recoverable errors
  - Write unit tests for error handling scenarios
  - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [x] 7. Create HTTP retry demonstration application
  - Build sample application that makes HTTP calls and handles failures
  - Implement retry logic using the delayed messaging system
  - Demonstrate producing messages to delay topics when HTTP calls fail
  - Show integration with different DelayDuration enum values for progressive backoff
  - _Requirements: 6.3, 6.4_

- [x] 8. Add consumer group management and scalability features
  - Implement consumer group naming strategy using DelayDuration
  - Add support for multiple consumer instances processing same delay topic
  - Configure auto-commit and proper partition assignment
  - Create integration tests for multiple consumer scenarios
  - _Requirements: 5.1, 5.2, 5.3_

- [x] 9. Create integration tests with TestContainers
  - Set up TestContainers for Kafka integration testing
  - Write end-to-end tests for complete delayed message flow
  - Test different DelayDuration values and verify timing precision
  - Add tests for error scenarios and consumer scalability
  - _Requirements: 1.6, 2.5, 5.1, 5.2_

- [x] 10. Add performance monitoring and final optimizations
  - Implement performance logging for throughput and latency metrics
  - Add memory usage monitoring for long-running consumers
  - Optimize sleep calculations to account for processing overhead
  - Create performance tests to validate millisecond precision requirements
  - _Requirements: 1.6, 2.5, 5.3_