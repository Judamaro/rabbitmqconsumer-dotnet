namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi;

/// <summary>
/// Adapter that implements <see cref="IPublisherApiClient"/> by calling the publisher API endpoints configured in <see cref="UrlOptions"/>.
/// </summary>
/// <param name="_urlOptions">URLs of the publisher API endpoints.</param>
/// <param name="_jsonHttpClient">Client used to send the requests.</param>
/// <param name="_logger">Logger for messages that were already processed.</param>
public class PublisherApiClient(IOptions<UrlOptions> _urlOptions, IJsonHttpClient _jsonHttpClient, ILogger<PublisherApiClient> _logger) : IPublisherApiClient
{
    /// <inheritdoc/>
    public async Task AddErrorTextAsync(int id, string errorText, CancellationToken cancellationToken = default)
    {
        string url = _urlOptions.Value.PublisherApi.AddErrorText;
        using HttpResponseMessage httpResponseMessage = await _jsonHttpClient.SendPostAsync(url, new AddErrorTextDto() { MessageId = id, Text = errorText }, cancellationToken);
        await EnsureSuccessAsync(httpResponseMessage, "POST", url, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task UpdateMessageAsProcessedAsync(int id, CancellationToken cancellationToken = default)
    {
        string url = _urlOptions.Value.PublisherApi.UpdateMessageAsProcessed.Replace(UrlOptions.PublisherApiOptions.MessageIdPlaceholder, id.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);
        using HttpResponseMessage httpResponseMessage = await _jsonHttpClient.SendPatchAsync(url, cancellationToken);

        // 409 Conflict means the message is already processed, e.g. it was redelivered after the previous ack was lost
        if (httpResponseMessage.StatusCode == HttpStatusCode.Conflict)
        {
            _logger.LogInformation("Message {MessageId} was already marked as processed", id);
            return;
        }

        await EnsureSuccessAsync(httpResponseMessage, "PATCH", url, cancellationToken);
    }

    /// <summary>
    /// Throws when the publisher API does not return a success status code.
    /// </summary>
    /// <param name="httpResponseMessage">Response received.</param>
    /// <param name="method">HTTP method used, for the error message.</param>
    /// <param name="url">Absolute URL of the endpoint, for the error message.</param>
    /// <param name="cancellationToken">Token to cancel reading the response body.</param>
    /// <exception cref="HttpRequestException">The response status code is not successful; the message includes the status code and the response body.</exception>
    private static async Task EnsureSuccessAsync(HttpResponseMessage httpResponseMessage, string method, string url, CancellationToken cancellationToken)
    {
        if (httpResponseMessage.IsSuccessStatusCode)
            return;

        string responseBody = await httpResponseMessage.Content.ReadAsStringAsync(cancellationToken);
        throw new HttpRequestException($"{method} {url} returned status code {(int)httpResponseMessage.StatusCode}: {responseBody}", null, httpResponseMessage.StatusCode);
    }
}
