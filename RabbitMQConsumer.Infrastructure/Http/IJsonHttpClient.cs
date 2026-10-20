namespace RabbitMQConsumer.Infrastructure.Http;

public interface IJsonHttpClient
{
    public Task<HttpResponseMessage> SendPostAsync(string url, object body, CancellationToken cancellationToken = default);
}
