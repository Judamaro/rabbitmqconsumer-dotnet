namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

/// <summary>
/// Shares whether <see cref="RabbitMQMessageConsumer"/> is currently consuming the queue, so the health check can report it.
/// </summary>
public sealed class RabbitMQConsumerStatus
{
    private volatile bool _isConsuming;

    /// <summary>
    /// Whether the consumer is registered on an open channel and receiving deliveries from the queue.
    /// </summary>
    public bool IsConsuming => _isConsuming;

    /// <summary>
    /// Marks the consumer as registered on the queue.
    /// </summary>
    public void MarkConsuming() => _isConsuming = true;

    /// <summary>
    /// Marks the consumer as no longer consuming, e.g. after the channel was closed or the host is stopping.
    /// </summary>
    public void MarkStopped() => _isConsuming = false;
}
