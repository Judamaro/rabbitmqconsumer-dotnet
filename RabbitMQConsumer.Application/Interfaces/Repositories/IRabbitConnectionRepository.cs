namespace RabbitMQConsumer.Application.Interfaces.Repositories;

public interface IRabbitConnectionRepository
{
    public Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken);
}
