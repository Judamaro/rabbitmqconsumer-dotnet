namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi.Contracts;

/// <summary>
/// Request body sent to the publisher API <c>AddErrorMessage</c> endpoint.
/// </summary>
public record ErrorMessageDto
{
    public int Id { get; init; }
    public required string Text { get; init; }
}
