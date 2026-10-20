namespace RabbitMQConsumer.Shared.OptionsValues;

public record RabbitMQOptions
{
    public required string Name { get; init; }
    public required string Host { get; init; }
    public required string User { get; init; }
    public required string Pwd { get; init; }
    public required string Queue { get; init; }
    public int Port { get; init; }
    public required string VirtualHost { get; init; }
    public ushort PrefetchCount { get; init; }
    public int HeartbeatSeconds { get; init; }
    public int NetworkRecoveryIntervalSeconds { get; init; }
    public int MaxConnectRetryDelaySeconds { get; init; }
}
