namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

/// <summary>
/// Provides channels on a single RabbitMQ connection shared by the whole process.
/// </summary>
public interface IRabbitMQConnectionProvider
{
    /// <summary>
    /// Whether the shared connection is currently open; <see langword="false"/> before the first connection and while automatic recovery is in progress.
    /// </summary>
    public bool IsConnected { get; }


    /// <summary>
    /// Creates a new channel on the shared RabbitMQ connection, connecting first if needed.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the connection or channel creation.</param>
    /// <returns>A new channel that the caller owns and must dispose.</returns>
    public Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken);
}
