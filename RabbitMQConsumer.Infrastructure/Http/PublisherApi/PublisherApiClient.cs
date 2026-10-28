namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi;

/// <summary>
/// Adapter that implements <see cref="IPublisherApiClient"/> by calling the publisher API endpoints configured in <see cref="UrlOptions"/>.
/// </summary>
/// <param name="_urlOptions">URLs of the publisher API endpoints.</param>
/// <param name="_jsonHttpClient">Client used to send the JSON requests.</param>
public class PublisherApiClient(IOptions<UrlOptions> _urlOptions, IJsonHttpClient _jsonHttpClient) : IPublisherApiClient
{
    /// <inheritdoc/>
    public Task AddErrorTextAsync(int id, string errorText, CancellationToken cancellationToken = default)
        => SendPostAsync(_urlOptions.Value.PublisherApi.AddErrorText, new AddErrorTextDto() { Id = id, Text = errorText }, cancellationToken);

    /// <inheritdoc/>
    public Task UpdateMessageAsProcessedAsync(int id, CancellationToken cancellationToken = default)
        => SendPostAsync(_urlOptions.Value.PublisherApi.UpdateMessageAsProcessed, new UpdateMessageAsProcessedDto() { Id = id }, cancellationToken);

    /// <summary>
    /// Sends the request and disposes the response, throwing when the publisher API does not return a success status code.
    /// </summary>
    /// <param name="url">Absolute URL of the endpoint.</param>
    /// <param name="body">Object to serialize as the request body.</param>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <exception cref="HttpRequestException">The response status code is not successful; the message includes the status code and the response body.</exception>
    private async Task SendPostAsync(string url, object body, CancellationToken cancellationToken)
    {
        using HttpResponseMessage httpResponseMessage = await _jsonHttpClient.SendPostAsync(url, body, cancellationToken);
        if (httpResponseMessage.IsSuccessStatusCode)
            return;

        string responseBody = await httpResponseMessage.Content.ReadAsStringAsync(cancellationToken);
        throw new HttpRequestException($"POST {url} returned status code {(int)httpResponseMessage.StatusCode}: {responseBody}", null, httpResponseMessage.StatusCode);
    }
}
