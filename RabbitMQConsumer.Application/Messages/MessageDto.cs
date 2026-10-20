namespace RabbitMQConsumer.Application.Messages;

/// <summary>
/// Raw message as received from the broker, before applying the business rules.
/// </summary>
public record MessageDto
{
    public int Id { get; init; }
    public string? Payload { get; init; }
}
