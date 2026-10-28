namespace RabbitMQConsumer.Application.Abstractions;

/// <summary>
/// Port to the publisher API that owns the messages.
/// </summary>
public interface IPublisherApiClient
{
    /// <summary>
    /// Notifies the publisher API that the message has been processed successfully.
    /// </summary>
    /// <param name="id">Identifier of the processed message.</param>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <exception cref="HttpRequestException">The publisher API did not accept the request.</exception>
    Task UpdateMessageAsProcessedAsync(int id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reports to the publisher API the error that prevented a message from being processed.
    /// </summary>
    /// <param name="id">Identifier of the message that failed.</param>
    /// <param name="errorText">Text describing the error.</param>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <exception cref="HttpRequestException">The publisher API did not accept the request.</exception>
    Task AddErrorTextAsync(int id, string errorText, CancellationToken cancellationToken = default);
}
