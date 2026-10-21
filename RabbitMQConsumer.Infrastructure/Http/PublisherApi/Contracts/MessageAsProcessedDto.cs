namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi.Contracts;

/// <summary>
/// Request body sent to the publisher API <c>MarkAsProcessed</c> endpoint.
/// </summary>
public record MessageAsProcessedDto
{
    public int Id { get; init; }
}
