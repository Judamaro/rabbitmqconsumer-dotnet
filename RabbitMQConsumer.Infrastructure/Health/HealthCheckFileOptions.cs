namespace RabbitMQConsumer.Infrastructure.Health;

/// <summary>
/// Settings of the health status file, bound from the optional <c>HealthCheckFileOptions</c> configuration section.
/// </summary>
public record HealthCheckFileOptions
{
    /// <summary>
    /// Path of the file that exists and is refreshed while the service is healthy. The Docker healthcheck checks that it was updated recently.
    /// </summary>
    [Required]
    public string FilePath { get; init; } = "/tmp/healthy";
}
