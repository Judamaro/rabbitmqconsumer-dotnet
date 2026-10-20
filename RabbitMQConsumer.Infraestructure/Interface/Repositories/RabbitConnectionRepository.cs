namespace RabbitMQConsumer.Infraestructure.Interface.Repositories;

public sealed class RabbitConnectionRepository(IOptions<RabbitMQOptions> _rabbitMQOptions, ILogger<RabbitConnectionRepository> _logger) : IRabbitConnectionRepository, IAsyncDisposable
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromSeconds(1);

    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private volatile IConnection? _connection;

    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken)
    {
        IConnection connection = await GetConnectionAsync(cancellationToken);
        return await connection.CreateChannelAsync(cancellationToken: cancellationToken);
    }

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

    private async Task<IConnection> ConnectWithRetryAsync(CancellationToken cancellationToken)
    {
        ConnectionFactory connectionFactory = BuildConnectionFactory();
        TimeSpan maxRetryDelay = TimeSpan.FromSeconds(_rabbitMQOptions.Value.MaxConnectRetryDelaySeconds);
        TimeSpan retryDelay = InitialRetryDelay;
        int attempt = 0;

        while (true)
        {
            attempt++;
            try
            {
                IConnection connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
                SubscribeToConnectionEvents(connection);
                _logger.LogInformation("Connected to RabbitMQ {Host}:{Port}{VirtualHost} after {Attempt} attempt(s)", _rabbitMQOptions.Value.Host, _rabbitMQOptions.Value.Port, _rabbitMQOptions.Value.VirtualHost, attempt);
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

    private ConnectionFactory BuildConnectionFactory()
        => new()
        {
            ClientProvidedName = _rabbitMQOptions.Value.Name,
            HostName = _rabbitMQOptions.Value.Host,
            Port = _rabbitMQOptions.Value.Port,
            VirtualHost = _rabbitMQOptions.Value.VirtualHost,
            UserName = _rabbitMQOptions.Value.User,
            Password = _rabbitMQOptions.Value.Pwd,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(_rabbitMQOptions.Value.NetworkRecoveryIntervalSeconds),
            RequestedHeartbeat = TimeSpan.FromSeconds(_rabbitMQOptions.Value.HeartbeatSeconds)
        };

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
