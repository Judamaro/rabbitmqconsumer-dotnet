namespace RabbitMQConsumer.Infrastructure.Http;

/// <summary>
/// Typed <see cref="HttpClient"/> that sends JSON requests serialized with System.Text.Json.
/// </summary>
/// <param name="_httpClient">HTTP client provided by <see cref="IHttpClientFactory"/>, already wrapped with the resilience pipeline.</param>
/// <param name="_logger">Logger for non-success responses.</param>
public sealed class JsonHttpClient(HttpClient _httpClient, ILogger<JsonHttpClient> _logger) : IJsonHttpClient
{
    /// <inheritdoc/>
    public async Task<HttpResponseMessage> SendPostAsync(string url, object body, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(url);
        ArgumentNullException.ThrowIfNull(body);

        using JsonContent content = JsonContent.Create(body, body.GetType());
        await content.LoadIntoBufferAsync(cancellationToken);
        HttpResponseMessage response = await _httpClient.PostAsync(url, content, cancellationToken);

        if (!response.IsSuccessStatusCode)
            _logger.LogWarning("POST {Url} failed with status code {StatusCode}", url, (int)response.StatusCode);

        return response;
    }
}
