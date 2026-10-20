namespace RabbitMQConsumer.Application.Interfaces.Repositories;

public interface IPublisherApiRepository
{
    Task MarkAsProcessedAsync(int id, CancellationToken cancellationToken = default);

    Task AddErrorMessageAsync(int id, string errorMessage, CancellationToken cancellationToken = default);
}
