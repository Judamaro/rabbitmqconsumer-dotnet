namespace RabbitMQConsumer.Infraestructure.Interface.Repositories;

public sealed class HttpClientRepository(HttpClient _httpClient, ILogger<HttpClientRepository> _logger) : IHttpClientRepository
{
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
