using KafkaDelayedMessaging.Interfaces;
using KafkaDelayedMessaging.Models;
using KafkaDelayedMessaging.Services;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Text;

namespace KafkaDelayedMessaging.Tests;

public class HttpRetryServiceTests
{
    private readonly ILogger<HttpRetryService> _logger;
    private readonly TestDelayedMessageProducer _mockProducer;
    private readonly HttpClient _httpClient;
    private readonly HttpRetryService _httpRetryService;
    private readonly TestHttpMessageHandler _mockHttpHandler;

    public HttpRetryServiceTests()
    {
        _logger = new TestLogger<HttpRetryService>();
        _mockProducer = new TestDelayedMessageProducer();
        _mockHttpHandler = new TestHttpMessageHandler();
        _httpClient = new HttpClient(_mockHttpHandler);
        _httpRetryService = new HttpRetryService(_httpClient, _mockProducer, _logger);
    }

    [Fact]
    public async Task GetWithRetryAsync_SuccessfulRequest_ReturnsContent()
    {
        // Arrange
        var url = "https://test.example.com/success";
        var expectedContent = "Success response";
        _mockHttpHandler.SetResponse(url, HttpStatusCode.OK, expectedContent);

        // Act
        var result = await _httpRetryService.GetWithRetryAsync(url);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(expectedContent, result);
        Assert.Empty(_mockProducer.ProducedMessages); // No retries should be scheduled
    }

    [Fact]
    public async Task GetWithRetryAsync_ServerError_SchedulesRetry()
    {
        // Arrange
        var url = "https://test.example.com/server-error";
        _mockHttpHandler.SetResponse(url, HttpStatusCode.InternalServerError);

        // Act
        var result = await _httpRetryService.GetWithRetryAsync(url);

        // Assert
        Assert.Null(result); // Should return null for failed request
        Assert.Single(_mockProducer.ProducedMessages); // Should schedule one retry
        
        var retryMessage = _mockProducer.ProducedMessages.First();
        Assert.Equal("http-retry-requests", retryMessage.TargetTopic);
        Assert.Equal(DelayDuration.Seconds5, retryMessage.DelayDuration);
        
        var retryRequest = retryMessage.Message as HttpRetryRequest;
        Assert.NotNull(retryRequest);
        Assert.Equal("GET", retryRequest.Method);
        Assert.Equal(url, retryRequest.Url);
        Assert.Equal(1, retryRequest.RetryAttempt);
    }

    [Fact]
    public async Task GetWithRetryAsync_ClientError_DoesNotScheduleRetry()
    {
        // Arrange
        var url = "https://test.example.com/not-found";
        _mockHttpHandler.SetResponse(url, HttpStatusCode.NotFound);

        // Act
        var result = await _httpRetryService.GetWithRetryAsync(url);

        // Assert
        Assert.Null(result); // Should return null for failed request
        Assert.Empty(_mockProducer.ProducedMessages); // Should not schedule retry for client errors
    }

    [Fact]
    public async Task PostWithRetryAsync_WithBody_SchedulesRetryWithCorrectData()
    {
        // Arrange
        var url = "https://test.example.com/service-unavailable";
        var body = """{"test": "data"}""";
        var contentType = "application/json";
        _mockHttpHandler.SetResponse(url, HttpStatusCode.ServiceUnavailable);

        // Act
        var result = await _httpRetryService.PostWithRetryAsync(url, body, contentType);

        // Assert
        Assert.Null(result); // Should return null for failed request
        Assert.Single(_mockProducer.ProducedMessages); // Should schedule one retry
        
        var retryMessage = _mockProducer.ProducedMessages.First();
        var retryRequest = retryMessage.Message as HttpRetryRequest;
        Assert.NotNull(retryRequest);
        Assert.Equal("POST", retryRequest.Method);
        Assert.Equal(url, retryRequest.Url);
        Assert.Equal(body, retryRequest.Body);
        Assert.Equal(contentType, retryRequest.ContentType);
    }

    [Fact]
    public async Task ProcessRetryRequestAsync_ValidRequest_ExecutesRequest()
    {
        // Arrange
        var url = "https://test.example.com/retry-success";
        var expectedContent = "Retry success response";
        _mockHttpHandler.SetResponse(url, HttpStatusCode.OK, expectedContent);
        
        var retryRequest = new HttpRetryRequest
        {
            Method = "GET",
            Url = url,
            RetryAttempt = 1,
            MaxRetries = 5,
            RequestId = Guid.NewGuid().ToString()
        };

        // Act
        var result = await _httpRetryService.ProcessRetryRequestAsync(retryRequest);

        // Assert
        Assert.NotNull(result); // Should succeed for 200 status
        Assert.Equal(expectedContent, result);
    }

    [Fact]
    public void Constructor_NullArguments_ThrowsArgumentNullException()
    {
        // Test null httpClient
        Assert.Throws<ArgumentNullException>(() => 
            new HttpRetryService(null!, _mockProducer, _logger));

        // Test null delayedMessageProducer
        Assert.Throws<ArgumentNullException>(() => 
            new HttpRetryService(_httpClient, null!, _logger));

        // Test null logger
        Assert.Throws<ArgumentNullException>(() => 
            new HttpRetryService(_httpClient, _mockProducer, null!));
    }

    [Fact]
    public async Task ProcessRetryRequestAsync_NullRequest_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => 
            _httpRetryService.ProcessRetryRequestAsync(null!));
    }

    [Fact]
    public async Task GetWithRetryAsync_MaxRetriesExceeded_DoesNotScheduleMoreRetries()
    {
        // Arrange
        var url = "https://test.example.com/always-fails";
        _mockHttpHandler.SetResponse(url, HttpStatusCode.InternalServerError);

        var retryRequest = new HttpRetryRequest
        {
            Method = "GET",
            Url = url,
            RetryAttempt = 5, // Already at max retries
            MaxRetries = 5,
            RequestId = Guid.NewGuid().ToString()
        };

        // Act
        var result = await _httpRetryService.ProcessRetryRequestAsync(retryRequest);

        // Assert
        Assert.Null(result); // Should return null for failed request
        Assert.Empty(_mockProducer.ProducedMessages); // Should not schedule more retries
    }

    [Fact]
    public async Task PostWithRetryAsync_WithHeaders_PreservesHeaders()
    {
        // Arrange
        var url = "https://test.example.com/with-headers";
        var body = """{"data": "test"}""";
        var headers = new Dictionary<string, string>
        {
            { "Authorization", "Bearer token123" },
            { "X-Custom-Header", "custom-value" }
        };
        _mockHttpHandler.SetResponse(url, HttpStatusCode.BadGateway);

        // Act
        var result = await _httpRetryService.PostWithRetryAsync(url, body, "application/json", headers);

        // Assert
        Assert.Null(result);
        Assert.Single(_mockProducer.ProducedMessages);
        
        var retryMessage = _mockProducer.ProducedMessages.First();
        var retryRequest = retryMessage.Message as HttpRetryRequest;
        Assert.NotNull(retryRequest);
        Assert.Equal(headers.Count, retryRequest.Headers.Count);
        Assert.Equal("Bearer token123", retryRequest.Headers["Authorization"]);
        Assert.Equal("custom-value", retryRequest.Headers["X-Custom-Header"]);
    }

    [Fact]
    public async Task GetWithRetryAsync_TooManyRequests_SchedulesRetry()
    {
        // Arrange
        var url = "https://test.example.com/rate-limited";
        _mockHttpHandler.SetResponse(url, HttpStatusCode.TooManyRequests);

        // Act
        var result = await _httpRetryService.GetWithRetryAsync(url);

        // Assert
        Assert.Null(result);
        Assert.Single(_mockProducer.ProducedMessages); // Should schedule retry for rate limiting
        
        var retryMessage = _mockProducer.ProducedMessages.First();
        Assert.Equal("http-retry-requests", retryMessage.TargetTopic);
        Assert.Equal(DelayDuration.Seconds5, retryMessage.DelayDuration);
    }

    [Fact]
    public async Task ProcessRetryRequestAsync_WithCustomHeaders_IncludesHeaders()
    {
        // Arrange
        var url = "https://test.example.com/with-custom-headers";
        var expectedContent = "Success with headers";
        _mockHttpHandler.SetResponse(url, HttpStatusCode.OK, expectedContent);
        
        var retryRequest = new HttpRetryRequest
        {
            Method = "GET",
            Url = url,
            Headers = new Dictionary<string, string>
            {
                { "X-Request-ID", "test-123" },
                { "User-Agent", "TestAgent/1.0" }
            },
            RetryAttempt = 0,
            MaxRetries = 3,
            RequestId = Guid.NewGuid().ToString()
        };

        // Act
        var result = await _httpRetryService.ProcessRetryRequestAsync(retryRequest);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(expectedContent, result);
        
        // Verify the request was made with the correct headers
        var actualRequest = _mockHttpHandler.Requests.Last();
        Assert.Contains(actualRequest.Headers, h => h.Key == "X-Request-ID" && h.Value.Contains("test-123"));
        Assert.Contains(actualRequest.Headers, h => h.Key == "User-Agent" && h.Value.Contains("TestAgent/1.0"));
    }
}

/// <summary>
/// Test implementation of IDelayedMessageProducer for unit testing.
/// </summary>
public class TestDelayedMessageProducer : IDelayedMessageProducer
{
    public List<ProducedMessage> ProducedMessages { get; } = new();

    public Task ProduceDelayedMessageAsync<T>(
        string targetTopic, 
        T message, 
        DelayDuration delayDuration, 
        CancellationToken cancellationToken = default)
    {
        ProducedMessages.Add(new ProducedMessage
        {
            TargetTopic = targetTopic,
            Message = message,
            DelayDuration = delayDuration
        });
        return Task.CompletedTask;
    }

    public Task ProduceDelayedMessageAsync<T>(
        string targetTopic, 
        T message, 
        DateTime processAt, 
        CancellationToken cancellationToken = default)
    {
        // Calculate delay duration for testing
        var delaySeconds = (int)(processAt - DateTime.UtcNow).TotalSeconds;
        var delayDuration = DelayDuration.Seconds5; // Default for testing
        
        ProducedMessages.Add(new ProducedMessage
        {
            TargetTopic = targetTopic,
            Message = message,
            DelayDuration = delayDuration,
            ProcessAt = processAt
        });
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        // No resources to dispose in test implementation
    }
}

/// <summary>
/// Represents a message that was produced during testing.
/// </summary>
public class ProducedMessage
{
    public string TargetTopic { get; set; } = string.Empty;
    public object? Message { get; set; }
    public DelayDuration DelayDuration { get; set; }
    public DateTime? ProcessAt { get; set; }
}

/// <summary>
/// Test logger implementation that captures log messages.
/// </summary>
public class TestLogger<T> : ILogger<T>
{
    public List<string> LogMessages { get; } = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        var message = formatter(state, exception);
        LogMessages.Add($"[{logLevel}] {message}");
    }
}

/// <summary>
/// Test HTTP message handler that allows mocking HTTP responses.
/// </summary>
public class TestHttpMessageHandler : HttpMessageHandler
{
    private readonly Dictionary<string, HttpResponseMessage> _responses = new();
    private readonly List<HttpRequestMessage> _requests = new();

    public IReadOnlyList<HttpRequestMessage> Requests => _requests.AsReadOnly();

    public void SetResponse(string url, HttpStatusCode statusCode, string? content = null)
    {
        var response = new HttpResponseMessage(statusCode);
        if (content != null)
        {
            response.Content = new StringContent(content);
        }
        _responses[url] = response;
    }

    public void SetResponse(string url, HttpResponseMessage response)
    {
        _responses[url] = response;
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        _requests.Add(request);
        
        var url = request.RequestUri?.ToString() ?? string.Empty;
        
        if (_responses.TryGetValue(url, out var response))
        {
            return Task.FromResult(response);
        }

        // Default response for unmocked URLs
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound)
        {
            Content = new StringContent("Not Found")
        });
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            foreach (var response in _responses.Values)
            {
                response.Dispose();
            }
            _responses.Clear();
            
            foreach (var request in _requests)
            {
                request.Dispose();
            }
            _requests.Clear();
        }
        base.Dispose(disposing);
    }
}