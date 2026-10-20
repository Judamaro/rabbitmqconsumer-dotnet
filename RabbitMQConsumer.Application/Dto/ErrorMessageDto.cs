namespace RabbitMQConsumer.Application.Dto;

public record ErrorMessageDto
{
    public int Id { get; init; }
    public required string Text { get; init; }
}
