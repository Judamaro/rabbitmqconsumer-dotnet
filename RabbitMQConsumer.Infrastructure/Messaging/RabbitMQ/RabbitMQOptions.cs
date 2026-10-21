namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

/// <summary>
/// RabbitMQ connection and consumer settings, bound from the <c>RabbitMQOptions</c> configuration section.
/// </summary>
public record RabbitMQOptions
{
    [Required]
    public required string Name { get; init; }
    [Required]
    public required string Host { get; init; }
    [Required]
    public required string User { get; init; }
    [Required]
    public required string Pwd { get; init; }
    [Required]
    public required string Queue { get; init; }
    [Range(1, 65535)]
    public int Port { get; init; }
    [Required]
    public required string VirtualHost { get; init; }
    [Range(1, ushort.MaxValue)]
    public ushort PrefetchCount { get; init; }
    [Range(1, int.MaxValue)]
    public int HeartbeatSeconds { get; init; }
    [Range(1, int.MaxValue)]
    public int NetworkRecoveryIntervalSeconds { get; init; }
    [Range(1, int.MaxValue)]
    public int MaxConnectRetryDelaySeconds { get; init; }
}
