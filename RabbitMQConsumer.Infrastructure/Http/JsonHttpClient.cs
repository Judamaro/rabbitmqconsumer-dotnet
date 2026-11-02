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
        using HttpRequestMessage request = new(HttpMethod.Post, url) { Content = content };

        return await SendAsync(request, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<HttpResponseMessage> SendPatchAsync(string url, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(url);

        using HttpRequestMessage request = new(HttpMethod.Patch, url);

        return await SendAsync(request, cancellationToken);
    }

    /// <summary>
    /// Sends the request and logs a warning when the response status code is not successful.
    /// </summary>
    /// <param name="request">Request to send.</param>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <returns>The response received, which may have a non-success status code.</returns>
    private async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        HttpResponseMessage response = await _httpClient.SendAsync(request, cancellationToken);

        if (!response.IsSuccessStatusCode)
            _logger.LogWarning("{Method} {Url} failed with status code {StatusCode}", request.Method, request.RequestUri, (int)response.StatusCode);

        return response;
    }
}
