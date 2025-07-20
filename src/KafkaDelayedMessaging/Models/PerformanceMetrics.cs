using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace KafkaDelayedMessaging.Models;

/// <summary>
/// Performance metrics for monitoring delayed message processing throughput and latency.
/// </summary>
public class PerformanceMetrics
{
    /// <summary>
    /// Total number of messages processed.
    /// </summary>
    public long TotalMessagesProcessed { get; set; }

    /// <summary>
    /// Total number of messages produced to delay topics.
    /// </summary>
    public long TotalMessagesProduced { get; set; }

    /// <summary>
    /// Total number of messages forwarded to target topics.
    /// </summary>
    public long TotalMessagesForwarded { get; set; }

    /// <summary>
    /// Total number of processing errors encountered.
    /// </summary>
    public long TotalErrors { get; set; }

    /// <summary>
    /// Average processing latency in milliseconds.
    /// </summary>
    public double AverageProcessingLatencyMs { get; set; }

    /// <summary>
    /// Average delay precision (difference between expected and actual processing time) in milliseconds.
    /// </summary>
    public double AverageDelayPrecisionMs { get; set; }

    /// <summary>
    /// Messages processed per second (throughput).
    /// </summary>
    public double MessagesPerSecond { get; set; }

    /// <summary>
    /// Current memory usage in bytes.
    /// </summary>
    public long MemoryUsageBytes { get; set; }

    /// <summary>
    /// Peak memory usage in bytes since last reset.
    /// </summary>
    public long PeakMemoryUsageBytes { get; set; }

    /// <summary>
    /// Timestamp when metrics were last updated.
    /// </summary>
    public DateTimeOffset LastUpdated { get; set; }

    /// <summary>
    /// Time when metrics collection started.
    /// </summary>
    public DateTimeOffset StartTime { get; set; }

    /// <summary>
    /// Total uptime since metrics collection started.
    /// </summary>
    public TimeSpan Uptime => LastUpdated - StartTime;
}

/// <summary>
/// Performance tracker using System.Diagnostics.Metrics for collecting and calculating performance metrics.
/// </summary>
public class PerformanceTracker : IDisposable
{
    private static readonly Meter _meter = new("KafkaDelayedMessaging", "1.0.0");
    
    // Counters
    private static readonly Counter<long> _messagesProducedCounter = _meter.CreateCounter<long>(
        "kafka_delayed_messages_produced_total",
        "messages",
        "Total number of messages produced to delay topics");
    
    private static readonly Counter<long> _messagesProcessedCounter = _meter.CreateCounter<long>(
        "kafka_delayed_messages_processed_total", 
        "messages",
        "Total number of messages processed from delay topics");
    
    private static readonly Counter<long> _messagesForwardedCounter = _meter.CreateCounter<long>(
        "kafka_delayed_messages_forwarded_total",
        "messages", 
        "Total number of messages forwarded to target topics");
    
    private static readonly Counter<long> _errorsCounter = _meter.CreateCounter<long>(
        "kafka_delayed_messages_errors_total",
        "errors",
        "Total number of processing errors encountered");

    // Histograms
    private static readonly Histogram<double> _processingLatencyHistogram = _meter.CreateHistogram<double>(
        "kafka_delayed_message_processing_duration_ms",
        "milliseconds",
        "Processing latency for delayed messages");
    
    private static readonly Histogram<double> _delayPrecisionHistogram = _meter.CreateHistogram<double>(
        "kafka_delayed_message_precision_ms",
        "milliseconds", 
        "Delay precision (difference between expected and actual processing time)");

    // Gauges (using ObservableGauge)
    private static readonly ObservableGauge<long> _memoryUsageGauge = _meter.CreateObservableGauge<long>(
        "kafka_delayed_messaging_memory_usage_bytes",
        GetCurrentMemoryUsage,
        "bytes",
        "Current memory usage");

    // Internal tracking for compatibility with existing API
    private readonly object _lock = new();
    private readonly List<double> _processingLatencies = new();
    private readonly List<double> _delayPrecisions = new();
    private readonly Stopwatch _uptimeStopwatch = Stopwatch.StartNew();
    
    private long _totalMessagesProcessed;
    private long _totalMessagesProduced;
    private long _totalMessagesForwarded;
    private long _totalErrors;
    private long _peakMemoryUsage;
    private DateTimeOffset _startTime = DateTimeOffset.UtcNow;

    /// <summary>
    /// Records a message production event.
    /// </summary>
    public void RecordMessageProduced()
    {
        _messagesProducedCounter.Add(1);
        Interlocked.Increment(ref _totalMessagesProduced);
    }

    /// <summary>
    /// Records a message processing event with latency measurement.
    /// </summary>
    /// <param name="processingLatencyMs">Processing latency in milliseconds</param>
    public void RecordMessageProcessed(double processingLatencyMs)
    {
        _messagesProcessedCounter.Add(1);
        _processingLatencyHistogram.Record(processingLatencyMs);
        
        lock (_lock)
        {
            Interlocked.Increment(ref _totalMessagesProcessed);
            _processingLatencies.Add(processingLatencyMs);
            
            // Keep only the last 1000 measurements to prevent memory growth
            if (_processingLatencies.Count > 1000)
            {
                _processingLatencies.RemoveAt(0);
            }
        }
    }

    /// <summary>
    /// Records a message forwarding event with delay precision measurement.
    /// </summary>
    /// <param name="delayPrecisionMs">Delay precision in milliseconds (difference between expected and actual)</param>
    public void RecordMessageForwarded(double delayPrecisionMs)
    {
        _messagesForwardedCounter.Add(1);
        var absolutePrecision = Math.Abs(delayPrecisionMs);
        _delayPrecisionHistogram.Record(absolutePrecision);
        
        lock (_lock)
        {
            Interlocked.Increment(ref _totalMessagesForwarded);
            _delayPrecisions.Add(absolutePrecision);
            
            // Keep only the last 1000 measurements to prevent memory growth
            if (_delayPrecisions.Count > 1000)
            {
                _delayPrecisions.RemoveAt(0);
            }
        }
    }

    /// <summary>
    /// Records an error event.
    /// </summary>
    public void RecordError()
    {
        _errorsCounter.Add(1);
        Interlocked.Increment(ref _totalErrors);
    }

    /// <summary>
    /// Updates memory usage tracking.
    /// </summary>
    /// <param name="currentMemoryUsage">Current memory usage in bytes</param>
    public void UpdateMemoryUsage(long currentMemoryUsage)
    {
        lock (_lock)
        {
            if (currentMemoryUsage > _peakMemoryUsage)
            {
                _peakMemoryUsage = currentMemoryUsage;
            }
        }
    }

    /// <summary>
    /// Gets current performance metrics snapshot.
    /// </summary>
    /// <returns>Current performance metrics</returns>
    public PerformanceMetrics GetMetrics()
    {
        lock (_lock)
        {
            var currentMemory = GC.GetTotalMemory(false);
            UpdateMemoryUsage(currentMemory);
            
            var uptime = _uptimeStopwatch.Elapsed;
            var throughput = uptime.TotalSeconds > 0 ? _totalMessagesProcessed / uptime.TotalSeconds : 0;

            return new PerformanceMetrics
            {
                TotalMessagesProcessed = _totalMessagesProcessed,
                TotalMessagesProduced = _totalMessagesProduced,
                TotalMessagesForwarded = _totalMessagesForwarded,
                TotalErrors = _totalErrors,
                AverageProcessingLatencyMs = _processingLatencies.Count > 0 ? _processingLatencies.Average() : 0,
                AverageDelayPrecisionMs = _delayPrecisions.Count > 0 ? _delayPrecisions.Average() : 0,
                MessagesPerSecond = throughput,
                MemoryUsageBytes = currentMemory,
                PeakMemoryUsageBytes = _peakMemoryUsage,
                LastUpdated = DateTimeOffset.UtcNow,
                StartTime = _startTime
            };
        }
    }

    /// <summary>
    /// Resets all metrics to initial state.
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            _totalMessagesProcessed = 0;
            _totalMessagesProduced = 0;
            _totalMessagesForwarded = 0;
            _totalErrors = 0;
            _peakMemoryUsage = 0;
            _processingLatencies.Clear();
            _delayPrecisions.Clear();
            _startTime = DateTimeOffset.UtcNow;
            _uptimeStopwatch.Restart();
        }
    }

    /// <summary>
    /// Gets the current memory usage for the observable gauge.
    /// </summary>
    private static long GetCurrentMemoryUsage() => GC.GetTotalMemory(false);

    public void Dispose()
    {
        // The static meter will be disposed when the application shuts down
        // Individual instances don't need to dispose anything
    }
}