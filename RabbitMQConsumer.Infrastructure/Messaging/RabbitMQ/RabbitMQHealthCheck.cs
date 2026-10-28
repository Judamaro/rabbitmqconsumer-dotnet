namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

/// <summary>
/// Reports the consumer as healthy while the RabbitMQ connection is open and the queue is being consumed.
/// </summary>
/// <param name="_rabbitMQOptions">Broker settings used to name the queue in the result.</param>
/// <param name="_rabbitMQConnectionProvider">Provider that owns the shared connection.</param>
/// <param name="_consumerStatus">Status shared by the hosted consumer.</param>
public sealed class RabbitMQHealthCheck(IOptions<RabbitMQOptions> _rabbitMQOptions, IRabbitMQConnectionProvider _rabbitMQConnectionProvider, RabbitMQConsumerStatus _consumerStatus) : IHealthCheck
{
    /// <inheritdoc/>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!_rabbitMQConnectionProvider.IsConnected)
            return Task.FromResult(HealthCheckResult.Unhealthy("RabbitMQ connection is not open"));

        if (!_consumerStatus.IsConsuming)
            return Task.FromResult(HealthCheckResult.Unhealthy($"Consumer is not consuming from queue {_rabbitMQOptions.Value.Queue}"));

        return Task.FromResult(HealthCheckResult.Healthy($"Consuming from queue {_rabbitMQOptions.Value.Queue}"));
    }
}
