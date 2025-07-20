namespace KafkaDelayedMessaging.Models;

/// <summary>
/// Predefined delay durations for delayed messaging system.
/// Values represent seconds and are designed for exponential backoff patterns.
/// </summary>
public enum DelayDuration
{
    Seconds5 = 5,
    Seconds10 = 10,
    Seconds20 = 20,
    Seconds40 = 40,
    Minutes1 = 80,        // ~1.3 minutes
    Minutes3 = 160,       // ~2.7 minutes
    Minutes5 = 320,       // ~5.3 minutes
    Minutes11 = 640,      // ~10.7 minutes
    Minutes21 = 1280,     // ~21.3 minutes
    Minutes43 = 2560,     // ~42.7 minutes
    Hours1 = 5120,        // ~1.4 hours
    Hours3 = 10240,       // ~2.8 hours
    Hours6 = 20480,       // ~5.7 hours
    Hours11 = 40960,      // ~11.4 hours
    Hours23 = 81920,      // ~22.8 hours
    Days2 = 163840,       // ~1.9 days
    Days4 = 327680,       // ~3.8 days
    Days5 = 432000        // 5 days exactly
}