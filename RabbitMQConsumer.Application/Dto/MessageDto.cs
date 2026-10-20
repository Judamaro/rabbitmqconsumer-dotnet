namespace RabbitMQConsumer.Application.Dto;

public record MessageDto
{
    public int Id { get; init; }
    public required string Payload { get; init; }
}
