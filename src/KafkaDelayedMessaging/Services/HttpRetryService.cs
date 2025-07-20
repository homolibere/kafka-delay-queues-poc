using KafkaDelayedMessaging.Interfaces;
using KafkaDelayedMessaging.Models;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace KafkaDelayedMessaging.Services;

/// <summary>
/// Service that demonstrates HTTP retry logic using the delayed messaging system.
/// Makes HTTP calls and schedules retries with progressive backoff when failures occur.
/// </summary>
public class HttpRetryService
{
    private readonly HttpClient _httpClient;
    private readonly IDelayedMessageProducer _delayedMessageProducer;
    private readonly ILogger<HttpRetryService> _logger;
    private readonly DelayDuration[] _retryDelays;

    /// <summary>
    /// Initializes a new instance of HttpRetryService.
    /// </summary>
    /// <param name="httpClient">HTTP client for making requests</param>
    /// <param name="delayedMessageProducer">Producer for delayed retry messages</param>
    /// <param name="logger">Logger instance</param>
    public HttpRetryService(
        HttpClient httpClient,
        IDelayedMessageProducer delayedMessageProducer,
        ILogger<HttpRetryService> logger)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _delayedMessageProducer = delayedMessageProducer ?? throw new ArgumentNullException(nameof(delayedMessageProducer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        
        // Define progressive backoff delays for retries
        _retryDelays = new[]
        {
            DelayDuration.Seconds5,   // First retry after 5 seconds
            DelayDuration.Seconds10,  // Second retry after 10 seconds
            DelayDuration.Seconds20,  // Third retry after 20 seconds
            DelayDuration.Seconds40,  // Fourth retry after 40 seconds
            DelayDuration.Minutes1    // Final retry after ~1 minute
        };
    }

    /// <summary>
    /// Makes an HTTP GET request with automatic retry logic.
    /// </summary>
    /// <param name="url">The URL to request</param>
    /// <param name="headers">Optional headers to include</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>HTTP response content or null if all retries failed</returns>
    public async Task<string?> GetWithRetryAsync(
        string url, 
        Dictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        var request = new HttpRetryRequest
        {
            Method = "GET",
            Url = url,
            Headers = headers ?? new Dictionary<string, string>(),
            MaxRetries = _retryDelays.Length
        };

        return await ExecuteHttpRequestAsync(request, cancellationToken);
    }

    /// <summary>
    /// Makes an HTTP POST request with automatic retry logic.
    /// </summary>
    /// <param name="url">The URL to request</param>
    /// <param name="body">Request body content</param>
    /// <param name="contentType">Content type (defaults to application/json)</param>
    /// <param name="headers">Optional headers to include</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>HTTP response content or null if all retries failed</returns>
    public async Task<string?> PostWithRetryAsync(
        string url,
        string body,
        string contentType = "application/json",
        Dictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        var request = new HttpRetryRequest
        {
            Method = "POST",
            Url = url,
            Body = body,
            ContentType = contentType,
            Headers = headers ?? new Dictionary<string, string>(),
            MaxRetries = _retryDelays.Length
        };

        return await ExecuteHttpRequestAsync(request, cancellationToken);
    }

    /// <summary>
    /// Processes a retry request that was delayed through the messaging system.
    /// This method would typically be called by a consumer processing retry messages.
    /// </summary>
    /// <param name="retryRequest">The retry request to process</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>HTTP response content or null if request failed</returns>
    public async Task<string?> ProcessRetryRequestAsync(
        HttpRetryRequest retryRequest,
        CancellationToken cancellationToken = default)
    {
        if (retryRequest == null)
            throw new ArgumentNullException(nameof(retryRequest));

        _logger.LogInformation("Processing retry request {RequestId}, attempt {RetryAttempt}/{MaxRetries} for {Method} {Url}",
            retryRequest.RequestId, retryRequest.RetryAttempt + 1, retryRequest.MaxRetries, 
            retryRequest.Method, retryRequest.Url);

        return await ExecuteHttpRequestAsync(retryRequest, cancellationToken);
    }

    /// <summary>
    /// Executes an HTTP request with retry logic using delayed messaging.
    /// </summary>
    private async Task<string?> ExecuteHttpRequestAsync(
        HttpRetryRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogDebug("Executing HTTP {Method} request to {Url}, attempt {Attempt}",
                request.Method, request.Url, request.RetryAttempt + 1);

            // Create HTTP request message
            using var httpRequest = CreateHttpRequestMessage(request);
            
            // Execute the HTTP request
            using var response = await _httpClient.SendAsync(httpRequest, cancellationToken);
            
            // Check if the response indicates success
            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync(cancellationToken);
                
                _logger.LogInformation("HTTP {Method} request to {Url} succeeded with status {StatusCode} after {Attempts} attempts",
                    request.Method, request.Url, response.StatusCode, request.RetryAttempt + 1);
                
                return content;
            }
            
            // Handle non-success status codes
            await HandleHttpFailureAsync(request, response, cancellationToken);
            return null;
        }
        catch (HttpRequestException ex)
        {
            _logger.LogWarning("HTTP request exception for {Method} {Url}: {Error}",
                request.Method, request.Url, ex.Message);
            
            await HandleHttpExceptionAsync(request, ex, cancellationToken);
            return null;
        }
        catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException)
        {
            _logger.LogWarning("HTTP request timeout for {Method} {Url}",
                request.Method, request.Url);
            
            await HandleHttpExceptionAsync(request, ex, cancellationToken);
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error during HTTP {Method} request to {Url}",
                request.Method, request.Url);
            
            await HandleHttpExceptionAsync(request, ex, cancellationToken);
            return null;
        }
    }

    /// <summary>
    /// Creates an HttpRequestMessage from an HttpRetryRequest.
    /// </summary>
    private HttpRequestMessage CreateHttpRequestMessage(HttpRetryRequest request)
    {
        var httpRequest = new HttpRequestMessage(new HttpMethod(request.Method), request.Url);
        
        // Add headers
        foreach (var header in request.Headers)
        {
            httpRequest.Headers.TryAddWithoutValidation(header.Key, header.Value);
        }
        
        // Add body for POST/PUT requests
        if (!string.IsNullOrEmpty(request.Body) && 
            (request.Method.Equals("POST", StringComparison.OrdinalIgnoreCase) ||
             request.Method.Equals("PUT", StringComparison.OrdinalIgnoreCase)))
        {
            httpRequest.Content = new StringContent(
                request.Body, 
                Encoding.UTF8, 
                request.ContentType ?? "application/json");
        }
        
        return httpRequest;
    }

    /// <summary>
    /// Handles HTTP failures (non-success status codes) and schedules retries.
    /// </summary>
    private async Task HandleHttpFailureAsync(
        HttpRetryRequest request,
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        var statusCode = response.StatusCode;
        var shouldRetry = ShouldRetryForStatusCode(statusCode);
        
        _logger.LogWarning("HTTP {Method} request to {Url} failed with status {StatusCode}, should retry: {ShouldRetry}",
            request.Method, request.Url, statusCode, shouldRetry);
        
        if (shouldRetry)
        {
            await ScheduleRetryAsync(request, $"HTTP {statusCode}", cancellationToken);
        }
        else
        {
            _logger.LogError("HTTP {Method} request to {Url} failed with non-retryable status {StatusCode}, giving up after {Attempts} attempts",
                request.Method, request.Url, statusCode, request.RetryAttempt + 1);
        }
    }

    /// <summary>
    /// Handles HTTP exceptions and schedules retries.
    /// </summary>
    private async Task HandleHttpExceptionAsync(
        HttpRetryRequest request,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var shouldRetry = ShouldRetryForException(exception);
        
        _logger.LogWarning("HTTP {Method} request to {Url} threw exception: {ExceptionType}: {Message}, should retry: {ShouldRetry}",
            request.Method, request.Url, exception.GetType().Name, exception.Message, shouldRetry);
        
        if (shouldRetry)
        {
            await ScheduleRetryAsync(request, exception.GetType().Name, cancellationToken);
        }
        else
        {
            _logger.LogError("HTTP {Method} request to {Url} failed with non-retryable exception {ExceptionType}, giving up after {Attempts} attempts",
                request.Method, request.Url, exception.GetType().Name, request.RetryAttempt + 1);
        }
    }

    /// <summary>
    /// Schedules a retry attempt using the delayed messaging system.
    /// </summary>
    private async Task ScheduleRetryAsync(
        HttpRetryRequest request,
        string failureReason,
        CancellationToken cancellationToken)
    {
        // Check if we have retries left
        if (request.RetryAttempt >= request.MaxRetries)
        {
            _logger.LogError("HTTP {Method} request to {Url} exhausted all {MaxRetries} retry attempts, giving up. Last failure: {FailureReason}",
                request.Method, request.Url, request.MaxRetries, failureReason);
            return;
        }

        // Get the delay duration for this retry attempt
        var delayDuration = _retryDelays[Math.Min(request.RetryAttempt, _retryDelays.Length - 1)];
        
        // Create a new retry request with incremented attempt counter
        var nextRetryRequest = new HttpRetryRequest
        {
            Method = request.Method,
            Url = request.Url,
            Headers = request.Headers,
            Body = request.Body,
            ContentType = request.ContentType,
            RetryAttempt = request.RetryAttempt + 1,
            MaxRetries = request.MaxRetries,
            OriginalTimestamp = request.OriginalTimestamp,
            RequestId = request.RequestId
        };

        try
        {
            // Schedule the retry using delayed messaging
            await _delayedMessageProducer.ProduceDelayedMessageAsync(
                "http-retry-requests", // Target topic for retry processing
                nextRetryRequest,
                delayDuration,
                cancellationToken);

            _logger.LogInformation("Scheduled retry {RetryAttempt}/{MaxRetries} for HTTP {Method} {Url} with {DelayDuration} delay due to: {FailureReason}",
                nextRetryRequest.RetryAttempt, request.MaxRetries, request.Method, request.Url, delayDuration, failureReason);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to schedule retry for HTTP {Method} request to {Url}: {Error}",
                request.Method, request.Url, ex.Message);
        }
    }

    /// <summary>
    /// Determines if a request should be retried based on HTTP status code.
    /// </summary>
    private static bool ShouldRetryForStatusCode(HttpStatusCode statusCode)
    {
        return statusCode switch
        {
            // Server errors - usually temporary
            HttpStatusCode.InternalServerError => true,
            HttpStatusCode.BadGateway => true,
            HttpStatusCode.ServiceUnavailable => true,
            HttpStatusCode.GatewayTimeout => true,
            HttpStatusCode.RequestTimeout => true,
            
            // Rate limiting - should retry with backoff
            HttpStatusCode.TooManyRequests => true,
            
            // Client errors - usually permanent, don't retry
            HttpStatusCode.BadRequest => false,
            HttpStatusCode.Unauthorized => false,
            HttpStatusCode.Forbidden => false,
            HttpStatusCode.NotFound => false,
            HttpStatusCode.MethodNotAllowed => false,
            HttpStatusCode.NotAcceptable => false,
            HttpStatusCode.Conflict => false,
            HttpStatusCode.Gone => false,
            HttpStatusCode.UnprocessableEntity => false,
            
            // Success codes - no retry needed
            _ when ((int)statusCode >= 200 && (int)statusCode < 300) => false,
            
            // Default: retry for unknown server errors, don't retry for client errors
            _ => (int)statusCode >= 500
        };
    }

    /// <summary>
    /// Determines if a request should be retried based on exception type.
    /// </summary>
    private static bool ShouldRetryForException(Exception exception)
    {
        return exception switch
        {
            HttpRequestException => true,  // Network issues, DNS failures, etc.
            TaskCanceledException => true, // Timeouts
            SocketException => true,       // Network connectivity issues
            _ => false                     // Other exceptions are usually not retryable
        };
    }
}