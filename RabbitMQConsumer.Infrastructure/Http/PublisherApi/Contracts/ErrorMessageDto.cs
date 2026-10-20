namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi.Contracts;

public record ErrorMessageDto
{
    public int Id { get; init; }
    public required string Text { get; init; }
}
