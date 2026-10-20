namespace RabbitMQConsumer.Test.Infrastructure;

public class RabbitMQMessageConsumerTests : IAsyncLifetime
{
    private const string ValidBody = "{\"Id\":42,\"Payload\":\"hello\"}";
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private static readonly RabbitMQOptions RabbitMQOptions = new()
    {
        Name = "consumer-test",
        Host = "rabbit.test",
        User = "user",
        Pwd = "pwd",
        Queue = "messages",
        Port = 5672,
        VirtualHost = "/",
        PrefetchCount = 7,
        HeartbeatSeconds = 30,
        NetworkRecoveryIntervalSeconds = 5,
        MaxConnectRetryDelaySeconds = 1
    };

    private readonly IRabbitMQConnectionProvider _connectionProvider = Substitute.For<IRabbitMQConnectionProvider>();
    private readonly IProcessMessageUseCase _processMessageUseCase = Substitute.For<IProcessMessageUseCase>();
    private readonly IChannel _channel = Substitute.For<IChannel>();
    private readonly FakeLogger<RabbitMQMessageConsumer> _logger = new();
    private readonly ServiceProvider _serviceProvider;
    private readonly RabbitMQMessageConsumer _consumer;
    private readonly List<IAsyncBasicConsumer> _registeredConsumers = [];
    private ulong _nextDeliveryTag;

    public RabbitMQMessageConsumerTests()
    {
        _serviceProvider = new ServiceCollection()
            .AddScoped(_ => _processMessageUseCase)
            .BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        _connectionProvider.CreateChannelAsync(Arg.Any<CancellationToken>()).Returns(_channel);
        SetupConsumerCapture(_channel);

        _consumer = new RabbitMQMessageConsumer(_logger, Options.Create(RabbitMQOptions), _connectionProvider, _serviceProvider.GetRequiredService<IServiceScopeFactory>());
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _consumer.StopAsync(CancellationToken.None);
        _consumer.Dispose();
        await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task StartAsync_ConfiguresPrefetchAndConsumesQueueWithManualAck()
    {
        await StartAndWaitForConsumersAsync(1);

        await _channel.Received(1).BasicQosAsync(0, RabbitMQOptions.PrefetchCount, false, Arg.Any<CancellationToken>());
        await _channel.Received(1).BasicConsumeAsync(Arg.Is(RabbitMQOptions.Queue), Arg.Is(false), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<IDictionary<string, object?>?>(), Arg.Any<IAsyncBasicConsumer>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ValidJson_PassesDeserializedMessageToUseCase()
    {
        SetupUseCaseResult(MessageProcessingResult.Processed);
        await StartAndWaitForConsumersAsync(1);

        await DeliverAsync(ValidBody);

        await _processMessageUseCase.Received(1).ExecuteAsync(new MessageDto { Id = 42, Payload = "hello" }, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessedResult_Acks()
    {
        SetupUseCaseResult(MessageProcessingResult.Processed);
        await StartAndWaitForConsumersAsync(1);

        ulong deliveryTag = await DeliverAsync(ValidBody);

        await _channel.Received(1).BasicAckAsync(deliveryTag, false, Arg.Any<CancellationToken>());
        await _channel.DidNotReceiveWithAnyArgs().BasicNackAsync(default, default, default, default);
    }

    [Theory]
    [InlineData(MessageProcessingResult.Rejected)]
    [InlineData(MessageProcessingResult.Failed)]
    public async Task NotProcessedResult_NacksWithoutRequeue(MessageProcessingResult result)
    {
        SetupUseCaseResult(result);
        await StartAndWaitForConsumersAsync(1);

        ulong deliveryTag = await DeliverAsync(ValidBody);

        await _channel.Received(1).BasicNackAsync(deliveryTag, false, false, Arg.Any<CancellationToken>());
        await _channel.DidNotReceiveWithAnyArgs().BasicAckAsync(default, default, default);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not json")]
    [InlineData("null")]
    public async Task UnreadableBody_NacksWithoutCallingUseCase(string body)
    {
        await StartAndWaitForConsumersAsync(1);

        ulong deliveryTag = await DeliverAsync(body);

        await _channel.Received(1).BasicNackAsync(deliveryTag, false, false, Arg.Any<CancellationToken>());
        await _processMessageUseCase.DidNotReceiveWithAnyArgs().ExecuteAsync(default!, default);
    }

    [Fact]
    public async Task UseCaseThrows_NacksAndLogsError()
    {
        _processMessageUseCase.ExecuteAsync(Arg.Any<MessageDto>(), Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("unexpected"));
        await StartAndWaitForConsumersAsync(1);

        ulong deliveryTag = await DeliverAsync(ValidBody);

        await _channel.Received(1).BasicNackAsync(deliveryTag, false, false, Arg.Any<CancellationToken>());
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Error && record.Message.Contains("Unexpected error handling delivery"));
    }

    [Fact]
    public async Task AckOnClosedChannel_DoesNotThrow()
    {
        SetupUseCaseResult(MessageProcessingResult.Processed);
        _channel.BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromException(new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Library, 0, "closed"))));
        await StartAndWaitForConsumersAsync(1);

        await DeliverAsync(ValidBody);

        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Warning && record.Message.Contains("channel is closed"));
    }

    [Fact]
    public async Task ShutdownWhileProcessing_LeavesMessageUnacknowledged()
    {
        TaskCompletionSource processingStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _processMessageUseCase.ExecuteAsync(Arg.Any<MessageDto>(), Arg.Any<CancellationToken>()).Returns(async callInfo =>
        {
            processingStarted.TrySetResult();
            await Task.Delay(Timeout.Infinite, callInfo.ArgAt<CancellationToken>(1));
            return MessageProcessingResult.Processed;
        });
        await StartAndWaitForConsumersAsync(1);

        Task delivery = DeliverAsync(ValidBody);
        await processingStarted.Task.WaitAsync(WaitTimeout);
        await _consumer.StopAsync(CancellationToken.None);
        await delivery.WaitAsync(WaitTimeout);

        await _channel.DidNotReceiveWithAnyArgs().BasicAckAsync(default, default, default);
        await _channel.DidNotReceiveWithAnyArgs().BasicNackAsync(default, default, default, default);
    }

    [Fact]
    public async Task ChannelClosedByBroker_RecreatesChannelAndConsumesAgain()
    {
        IChannel secondChannel = Substitute.For<IChannel>();
        SetupConsumerCapture(secondChannel);
        _connectionProvider.CreateChannelAsync(Arg.Any<CancellationToken>()).Returns(_channel, secondChannel);
        await StartAndWaitForConsumersAsync(1);

        _channel.ChannelShutdownAsync += Raise.Event<AsyncEventHandler<ShutdownEventArgs>>(_channel, new ShutdownEventArgs(ShutdownInitiator.Peer, 404, "NOT_FOUND - no queue"));

        await WaitForConsumersAsync(2);
        await _connectionProvider.Received(2).CreateChannelAsync(Arg.Any<CancellationToken>());
        await secondChannel.Received(1).BasicQosAsync(0, RabbitMQOptions.PrefetchCount, false, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ConnectionForcedClose_LeavesRecoveryToClientLibrary()
    {
        await StartAndWaitForConsumersAsync(1);

        _channel.ChannelShutdownAsync += Raise.Event<AsyncEventHandler<ShutdownEventArgs>>(_channel, new ShutdownEventArgs(ShutdownInitiator.Peer, Constants.ConnectionForced, "CONNECTION_FORCED"));
        await Task.Delay(200);

        await _connectionProvider.Received(1).CreateChannelAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task CreateChannelFails_RetriesAfterBackoff()
    {
        _connectionProvider.CreateChannelAsync(Arg.Any<CancellationToken>()).Returns(
            _ => Task.FromException<IChannel>(new BrokerUnreachableException(new IOException("refused"))),
            _ => Task.FromResult(_channel));

        await StartAndWaitForConsumersAsync(1);

        await _connectionProvider.Received(2).CreateChannelAsync(Arg.Any<CancellationToken>());
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Error && record.Message.Contains("restarting in"));
    }

    private void SetupUseCaseResult(MessageProcessingResult result)
        => _processMessageUseCase.ExecuteAsync(Arg.Any<MessageDto>(), Arg.Any<CancellationToken>()).Returns(result);

    private void SetupConsumerCapture(IChannel channel)
    {
        channel.BasicConsumeAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<IDictionary<string, object?>?>(), Arg.Do<IAsyncBasicConsumer>(consumer =>
            {
                lock (_registeredConsumers)
                    _registeredConsumers.Add(consumer);
            }), Arg.Any<CancellationToken>())
            .Returns("consumer-tag");
    }

    private async Task StartAndWaitForConsumersAsync(int count)
    {
        await _consumer.StartAsync(CancellationToken.None);
        await WaitForConsumersAsync(count);
    }

    private async Task WaitForConsumersAsync(int count)
    {
        using CancellationTokenSource timeout = new(WaitTimeout);
        while (true)
        {
            lock (_registeredConsumers)
            {
                if (_registeredConsumers.Count >= count)
                    return;
            }
            await Task.Delay(10, timeout.Token);
        }
    }

    private async Task<ulong> DeliverAsync(string body)
    {
        IAsyncBasicConsumer consumer;
        lock (_registeredConsumers)
            consumer = _registeredConsumers[^1];

        ulong deliveryTag = ++_nextDeliveryTag;
        await consumer.HandleBasicDeliverAsync("consumer-tag", deliveryTag, false, "exchange", "routing-key", new BasicProperties(), Encoding.UTF8.GetBytes(body));
        return deliveryTag;
    }
}
