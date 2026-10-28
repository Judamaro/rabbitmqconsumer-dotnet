namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi.Contracts;

/// <summary>
/// Request body sent to the publisher API <c>AddErrorText</c> endpoint.
/// </summary>
public record AddErrorTextDto
{
    public int Id { get; init; }
    public required string Text { get; init; }
}
