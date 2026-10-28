namespace RabbitMQConsumer.Test.Infrastructure;

public class RabbitMQConnectionProviderTests
{
    private static readonly RabbitMQOptions RabbitMQOptions = new()
    {
        Name = "consumer-test",
        Host = "rabbit.test",
        User = "user",
        Pwd = "pwd",
        Queue = "queue",
        Port = 5673,
        VirtualHost = "/vhost",
        PrefetchCount = 5,
        HeartbeatSeconds = 15,
        NetworkRecoveryIntervalSeconds = 3,
        MaxConnectRetryDelaySeconds = 1
    };

    private readonly IConnectionFactory _connectionFactory = Substitute.For<IConnectionFactory>();
    private readonly IConnection _connection = Substitute.For<IConnection>();
    private readonly IChannel _channel = Substitute.For<IChannel>();
    private readonly FakeLogger<RabbitMQConnectionProvider> _logger = new();
    private readonly RabbitMQConnectionProvider _repository;

    public RabbitMQConnectionProviderTests()
    {
        _connection.CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>()).Returns(_channel);
        _repository = new RabbitMQConnectionProvider(Options.Create(RabbitMQOptions), _connectionFactory, _logger);
    }

    [Fact]
    public async Task CreateChannelAsync_ReusesSingleConnection()
    {
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_connection);

        IChannel first = await _repository.CreateChannelAsync(CancellationToken.None);
        IChannel second = await _repository.CreateChannelAsync(CancellationToken.None);

        Assert.Same(_channel, first);
        Assert.Same(_channel, second);
        await _connectionFactory.Received(1).CreateConnectionAsync(Arg.Any<CancellationToken>());
        await _connection.Received(2).CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public void IsConnected_BeforeFirstConnection_ReturnsFalse()
    {
        Assert.False(_repository.IsConnected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task IsConnected_AfterConnecting_ReflectsConnectionState(bool isOpen)
    {
        _connection.IsOpen.Returns(isOpen);
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_connection);

        await _repository.CreateChannelAsync(CancellationToken.None);

        Assert.Equal(isOpen, _repository.IsConnected);
    }

    [Fact]
    public async Task CreateChannelAsync_ConcurrentCalls_CreateOnlyOneConnection()
    {
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            await Task.Delay(50);
            return _connection;
        });

        await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => _repository.CreateChannelAsync(CancellationToken.None)));

        await _connectionFactory.Received(1).CreateConnectionAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task CreateChannelAsync_BrokerUnreachable_RetriesUntilConnected()
    {
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(
            _ => throw new BrokerUnreachableException(new IOException("refused")),
            _ => Task.FromResult(_connection));

        IChannel channel = await _repository.CreateChannelAsync(CancellationToken.None);

        Assert.Same(_channel, channel);
        await _connectionFactory.Received(2).CreateConnectionAsync(Arg.Any<CancellationToken>());
        FakeLogRecord warning = Assert.Single(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Warning);
        Assert.Contains("attempt 1", warning.Message);
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Information && record.Message.Contains("after 2 attempt(s)"));
    }

    [Fact]
    public async Task CreateChannelAsync_NonTransientError_PropagatesWithoutRetry()
    {
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("bad configuration"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => _repository.CreateChannelAsync(CancellationToken.None));

        await _connectionFactory.Received(1).CreateConnectionAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task CreateChannelAsync_CancelledWhileRetrying_ThrowsOperationCanceledException()
    {
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).ThrowsAsync(new BrokerUnreachableException(new IOException("refused")));
        using CancellationTokenSource cancellationTokenSource = new(TimeSpan.FromMilliseconds(200));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _repository.CreateChannelAsync(cancellationTokenSource.Token));
    }

    [Fact]
    public async Task CreateChannelAsync_SubscribesToConnectionEvents()
    {
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_connection);
        await _repository.CreateChannelAsync(CancellationToken.None);

        _connection.ConnectionShutdownAsync += Raise.Event<AsyncEventHandler<ShutdownEventArgs>>(_connection, new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "broker restart"));

        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Warning && record.Message.Contains("broker restart"));
    }

    [Fact]
    public async Task DisposeAsync_WithConnection_ClosesAndDisposesConnection()
    {
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_connection);
        await _repository.CreateChannelAsync(CancellationToken.None);

        await _repository.DisposeAsync();

        await _connection.Received(1).CloseAsync(Arg.Any<ushort>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await _connection.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_CloseFails_LogsWarningAndStillDisposes()
    {
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_connection);
        _connection.CloseAsync(Arg.Any<ushort>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Library, 0, "closed")));
        await _repository.CreateChannelAsync(CancellationToken.None);

        await _repository.DisposeAsync();

        await _connection.Received(1).DisposeAsync();
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Warning && record.Message.Contains("Error closing"));
    }

    [Fact]
    public async Task DisposeAsync_WithoutConnection_DoesNotThrow()
    {
        await _repository.DisposeAsync();

        await _connectionFactory.DidNotReceive().CreateConnectionAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task CreateChannelAsync_LogsInstanceConnectionName()
    {
        _connectionFactory.ClientProvidedName.Returns("consumer-test-3f9c2a1b7d4e");
        _connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_connection);

        await _repository.CreateChannelAsync(CancellationToken.None);

        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Information && record.Message.Contains("Connecting to RabbitMQ") && record.Message.Contains("as consumer-test-3f9c2a1b7d4e"));
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Information && record.Message.Contains("Connected to RabbitMQ") && record.Message.Contains("as consumer-test-3f9c2a1b7d4e"));
    }

    [Fact]
    public void RabbitMQConnectionFactoryBuilder_MapsOptionsAndEnablesRecovery()
    {
        ConnectionFactory connectionFactory = RabbitMQConnectionFactoryBuilder.Build(RabbitMQOptions, "instance-1");

        Assert.Equal("consumer-test-instance-1", connectionFactory.ClientProvidedName);
        Assert.Equal(RabbitMQOptions.Host, connectionFactory.HostName);
        Assert.Equal(RabbitMQOptions.Port, connectionFactory.Port);
        Assert.Equal(RabbitMQOptions.VirtualHost, connectionFactory.VirtualHost);
        Assert.Equal(RabbitMQOptions.User, connectionFactory.UserName);
        Assert.Equal(RabbitMQOptions.Pwd, connectionFactory.Password);
        Assert.Equal(TimeSpan.FromSeconds(RabbitMQOptions.HeartbeatSeconds), connectionFactory.RequestedHeartbeat);
        Assert.Equal(TimeSpan.FromSeconds(RabbitMQOptions.NetworkRecoveryIntervalSeconds), connectionFactory.NetworkRecoveryInterval);
        Assert.True(connectionFactory.AutomaticRecoveryEnabled);
        Assert.True(connectionFactory.TopologyRecoveryEnabled);
    }

    [Fact]
    public void RabbitMQConnectionFactoryBuilder_WithoutInstanceId_UsesMachineName()
    {
        ConnectionFactory connectionFactory = RabbitMQConnectionFactoryBuilder.Build(RabbitMQOptions);

        Assert.Equal($"{RabbitMQOptions.Name}-{Environment.MachineName}", connectionFactory.ClientProvidedName);
    }

    [Theory]
    [InlineData("", "instance-1")]
    [InlineData("consumer", " ")]
    public void BuildClientProvidedName_EmptyValue_ThrowsArgumentException(string name, string instanceId)
    {
        Assert.ThrowsAny<ArgumentException>(() => RabbitMQConnectionFactoryBuilder.BuildClientProvidedName(name, instanceId));
    }
}
