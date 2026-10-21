namespace RabbitMQConsumer.Infrastructure.Http;

/// <summary>
/// HTTP client that sends JSON requests through the resilience pipeline (retries, timeouts and circuit breaker).
/// </summary>
public interface IJsonHttpClient
{
    /// <summary>
    /// Sends <paramref name="body"/> serialized as JSON to <paramref name="url"/> using POST.
    /// The caller owns the returned response and must dispose it.
    /// </summary>
    /// <param name="url">Absolute URL of the endpoint.</param>
    /// <param name="body">Object to serialize as the request body.</param>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <returns>The response received, which may have a non-success status code.</returns>
    public Task<HttpResponseMessage> SendPostAsync(string url, object body, CancellationToken cancellationToken = default);
}
