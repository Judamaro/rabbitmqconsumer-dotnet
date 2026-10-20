namespace RabbitMQConsumer.Application.Abstractions;

/// <summary>
/// Port to the publisher API that owns the messages.
/// </summary>
public interface IPublisherApiClient
{
    /// <exception cref="HttpRequestException">The publisher API did not accept the request.</exception>
    Task MarkAsProcessedAsync(int id, CancellationToken cancellationToken = default);

    /// <exception cref="HttpRequestException">The publisher API did not accept the request.</exception>
    Task AddErrorMessageAsync(int id, string errorMessage, CancellationToken cancellationToken = default);
}
