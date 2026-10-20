namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi;

public class PublisherApiClient(IOptions<UrlOptions> _urlOptions, IJsonHttpClient _jsonHttpClient) : IPublisherApiClient
{
    public Task AddErrorMessageAsync(int id, string errorMessage, CancellationToken cancellationToken = default)
        => SendPostAsync(_urlOptions.Value.PublisherApi.AddErrorMessage, new ErrorMessageDto() { Id = id, Text = errorMessage }, cancellationToken);

    public Task MarkAsProcessedAsync(int id, CancellationToken cancellationToken = default)
        => SendPostAsync(_urlOptions.Value.PublisherApi.MarkAsProcessed, new MessageAsProcessedDto() { Id = id }, cancellationToken);

    private async Task SendPostAsync(string url, object body, CancellationToken cancellationToken)
    {
        using HttpResponseMessage httpResponseMessage = await _jsonHttpClient.SendPostAsync(url, body, cancellationToken);
        if (httpResponseMessage.IsSuccessStatusCode)
            return;

        string responseBody = await httpResponseMessage.Content.ReadAsStringAsync(cancellationToken);
        throw new HttpRequestException($"POST {url} returned status code {(int)httpResponseMessage.StatusCode}: {responseBody}", null, httpResponseMessage.StatusCode);
    }
}
