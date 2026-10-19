namespace RabbitMQConsumer.Shared.OptionsValues;

public record RabbitMQOptions
{
    public required string Name { get; init; }
    public required string Host { get; init; }
    public required string User { get; init; }
    public required string Pwd { get; init; }
}