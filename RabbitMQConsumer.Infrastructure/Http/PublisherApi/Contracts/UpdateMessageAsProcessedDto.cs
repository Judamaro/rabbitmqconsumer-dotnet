namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi.Contracts;

/// <summary>
/// Request body sent to the publisher API <c>UpdateMessageAsProcessed</c> endpoint.
/// </summary>
public record UpdateMessageAsProcessedDto
{
    public int Id { get; init; }
}
