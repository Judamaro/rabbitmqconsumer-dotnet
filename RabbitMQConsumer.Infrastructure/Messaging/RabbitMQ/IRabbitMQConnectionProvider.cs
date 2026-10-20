namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

public interface IRabbitMQConnectionProvider
{
    public Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken);
}
