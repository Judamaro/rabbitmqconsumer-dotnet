namespace RabbitMQConsumer.Application.Interfaces.Repositories;

public interface IHttpClientRepository
{
    public Task<HttpResponseMessage> SendPostAsync(string url, object body, CancellationToken cancellationToken = default);
}
