namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi.Contracts;

public record PetitionResponseDto
{
    public required string Message { get; init; }
    public required string Module { get; init; }
    public object? Result { get; init; }
    public int StatusCode { get; init; }
    public bool Success { get; init; }
}
