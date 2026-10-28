namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

/// <summary>
/// Owns the shared RabbitMQ connection: opens it lazily with retries, relies on automatic recovery to keep it alive and closes it on dispose.
/// </summary>
/// <param name="_rabbitMQOptions">Broker settings used for logging and the retry backoff.</param>
/// <param name="_connectionFactory">Factory that opens the connection.</param>
/// <param name="_logger">Logger for connection attempts and lifecycle events.</param>
public sealed class RabbitMQConnectionProvider(IOptions<RabbitMQOptions> _rabbitMQOptions, IConnectionFactory _connectionFactory, ILogger<RabbitMQConnectionProvider> _logger) : IRabbitMQConnectionProvider, IAsyncDisposable
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromSeconds(1);

    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private volatile IConnection? _connection;

    /// <inheritdoc/>
    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken)
    {
        IConnection connection = await GetConnectionAsync(cancellationToken);
        return await connection.CreateChannelAsync(cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Returns the shared connection, creating it once even when called concurrently.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the wait or the connection attempts.</param>
    /// <returns>The shared connection; automatic recovery keeps it alive after network failures.</returns>
    private async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is not null)
            return _connection;

        await _connectionLock.WaitAsync(cancellationToken);
        
        try
        {
            _connection ??= await ConnectWithRetryAsync(cancellationToken);
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    /// <summary>
    /// Opens the connection, retrying with exponential backoff (capped at <c>MaxConnectRetryDelaySeconds</c>) while the broker is unreachable.
    /// </summary>
    /// <param name="cancellationToken">Token to stop retrying.</param>
    /// <returns>The opened connection, already subscribed to its lifecycle events.</returns>
    private async Task<IConnection> ConnectWithRetryAsync(CancellationToken cancellationToken)
    {
        TimeSpan maxRetryDelay = TimeSpan.FromSeconds(_rabbitMQOptions.Value.MaxConnectRetryDelaySeconds);
        TimeSpan retryDelay = InitialRetryDelay;
        int attempt = 0;

        _logger.LogInformation("Connecting to RabbitMQ {Host}:{Port}{VirtualHost} as {ClientProvidedName}", _rabbitMQOptions.Value.Host, _rabbitMQOptions.Value.Port, _rabbitMQOptions.Value.VirtualHost, _connectionFactory.ClientProvidedName);

        while (true)
        {
            attempt++;
            try
            {
                IConnection connection = await _connectionFactory.CreateConnectionAsync(cancellationToken);
                SubscribeToConnectionEvents(connection);
                _logger.LogInformation("Connected to RabbitMQ {Host}:{Port}{VirtualHost} as {ClientProvidedName} after {Attempt} attempt(s)", _rabbitMQOptions.Value.Host, _rabbitMQOptions.Value.Port, _rabbitMQOptions.Value.VirtualHost, _connectionFactory.ClientProvidedName, attempt);
                return connection;
            }
            catch (Exception exception) when (exception is BrokerUnreachableException or ConnectFailureException or OperationInterruptedException)
            {
                _logger.LogWarning(exception, "RabbitMQ connection attempt {Attempt} failed, retrying in {RetryDelay}", attempt, retryDelay);
                await Task.Delay(retryDelay, cancellationToken);
                retryDelay = TimeSpan.FromTicks(Math.Min(retryDelay.Ticks * 2, maxRetryDelay.Ticks));
            }
        }
    }

    /// <summary>
    /// Logs the connection lifecycle: shutdown, recovery, recovery errors and broker flow control (blocked/unblocked).
    /// </summary>
    /// <param name="connection">Connection to observe.</param>
    private void SubscribeToConnectionEvents(IConnection connection)
    {
        connection.ConnectionShutdownAsync += (_, eventArgs) =>
        {
            _logger.LogWarning("RabbitMQ connection shutdown. Initiator: {Initiator}, Code: {ReplyCode}, Reason: {ReplyText}", eventArgs.Initiator, eventArgs.ReplyCode, eventArgs.ReplyText);
            return Task.CompletedTask;
        };

        connection.RecoverySucceededAsync += (_, _) =>
        {
            _logger.LogInformation("RabbitMQ connection recovered");
            return Task.CompletedTask;
        };

        connection.ConnectionRecoveryErrorAsync += (_, eventArgs) =>
        {
            _logger.LogError(eventArgs.Exception, "RabbitMQ connection recovery attempt failed");
            return Task.CompletedTask;
        };

        connection.ConnectionBlockedAsync += (_, eventArgs) =>
        {
            _logger.LogWarning("RabbitMQ connection blocked by broker: {Reason}", eventArgs.Reason);
            return Task.CompletedTask;
        };

        connection.ConnectionUnblockedAsync += (_, _) =>
        {
            _logger.LogInformation("RabbitMQ connection unblocked");
            return Task.CompletedTask;
        };
    }

    /// <summary>
    /// Closes and disposes the shared connection, if it was opened, and releases the connection lock. Errors while closing are logged.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            try
            {
                await _connection.CloseAsync();
            }
            catch (Exception exception)
            {
                _logger.LogWarning(exception, "Error closing RabbitMQ connection");
            }

            await _connection.DisposeAsync();
            _connection = null;
        }

        _connectionLock.Dispose();
    }
}
