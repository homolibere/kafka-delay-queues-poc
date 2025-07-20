namespace KafkaDelayedMessaging.Helpers;

/// <summary>
/// Extension methods for DateTime operations in the delayed messaging system.
/// </summary>
public static class DateTimeExtensions
{
    /// <summary>
    /// Converts a DateTime to Unix timestamp in milliseconds.
    /// </summary>
    /// <param name="dateTime">The DateTime to convert</param>
    /// <returns>Unix timestamp in milliseconds</returns>
    public static long ToUnixTimeMilliseconds(this DateTime dateTime)
    {
        return ((DateTimeOffset)dateTime).ToUnixTimeMilliseconds();
    }
}