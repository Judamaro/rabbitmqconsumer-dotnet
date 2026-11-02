namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi.Contracts;

/// <summary>
/// Request body sent to the publisher API <c>AddErrorText</c> endpoint, matching its <c>AddErrorRequest</c>.
/// </summary>
public record AddErrorTextDto
{
    public int MessageId { get; init; }
    public required string Text { get; init; }
}
