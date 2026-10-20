namespace RabbitMQConsumer.Test.Application;

public class ProcessMessageTests : IAsyncLifetime
{
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

    private readonly IRabbitConnectionRepository _rabbitConnectionRepository = Substitute.For<IRabbitConnectionRepository>();
    private readonly IPublisherApiRepository _publisherApiRepository = Substitute.For<IPublisherApiRepository>();
    private readonly IChannel _channel = Substitute.For<IChannel>();
    private readonly FakeLogger<ProcessMessage> _logger = new();
    private readonly ServiceProvider _serviceProvider;
    private readonly ProcessMessage _processMessage;
    private readonly List<IAsyncBasicConsumer> _consumers = [];
    private ulong _nextDeliveryTag;

    public ProcessMessageTests()
    {
        _serviceProvider = new ServiceCollection()
            .AddScoped(_ => _publisherApiRepository)
            .BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        _rabbitConnectionRepository.CreateChannelAsync(Arg.Any<CancellationToken>()).Returns(_channel);
        SetupConsumerCapture(_channel);

        _processMessage = new ProcessMessage(_logger, Options.Create(RabbitMQOptions), _rabbitConnectionRepository, _serviceProvider.GetRequiredService<IServiceScopeFactory>());
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _processMessage.StopAsync(CancellationToken.None);
        _processMessage.Dispose();
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
    public async Task ValidMessage_MarksAsProcessedThenAcks()
    {
        await StartAndWaitForConsumersAsync(1);

        ulong deliveryTag = await DeliverAsync("{\"Id\":42,\"Payload\":\"hello\"}");

        Received.InOrder(() =>
        {
            _publisherApiRepository.MarkAsProcessedAsync(42, Arg.Any<CancellationToken>());
            _channel.BasicAckAsync(deliveryTag, false, Arg.Any<CancellationToken>());
        });
        await _channel.DidNotReceiveWithAnyArgs().BasicNackAsync(default, default, default, default);
        await _publisherApiRepository.DidNotReceiveWithAnyArgs().AddErrorMessageAsync(default, default!, default);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not json")]
    [InlineData("null")]
    [InlineData("{\"Id\":0,\"Payload\":\"hello\"}")]
    [InlineData("{\"Id\":42,\"Payload\":\"\"}")]
    [InlineData("{\"Id\":42}")]
    public async Task InvalidMessage_NacksWithoutRequeueAndDoesNotCallApi(string body)
    {
        await StartAndWaitForConsumersAsync(1);

        ulong deliveryTag = await DeliverAsync(body);

        await _channel.Received(1).BasicNackAsync(deliveryTag, false, false, Arg.Any<CancellationToken>());
        await _channel.DidNotReceiveWithAnyArgs().BasicAckAsync(default, default, default);
        await _publisherApiRepository.DidNotReceiveWithAnyArgs().MarkAsProcessedAsync(default, default);
        await _publisherApiRepository.DidNotReceiveWithAnyArgs().AddErrorMessageAsync(default, default!, default);
    }

    [Fact]
    public async Task MarkAsProcessedFails_ReportsErrorAndNacks()
    {
        _publisherApiRepository.MarkAsProcessedAsync(42, Arg.Any<CancellationToken>()).ThrowsAsync(new HttpRequestException("boom"));
        await StartAndWaitForConsumersAsync(1);

        ulong deliveryTag = await DeliverAsync("{\"Id\":42,\"Payload\":\"hello\"}");

        await _publisherApiRepository.Received(1).AddErrorMessageAsync(42, "boom", Arg.Any<CancellationToken>());
        await _channel.Received(1).BasicNackAsync(deliveryTag, false, false, Arg.Any<CancellationToken>());
        await _channel.DidNotReceiveWithAnyArgs().BasicAckAsync(default, default, default);
    }

    [Fact]
    public async Task AddErrorMessageAlsoFails_StillNacksAndLogsError()
    {
        _publisherApiRepository.MarkAsProcessedAsync(42, Arg.Any<CancellationToken>()).ThrowsAsync(new HttpRequestException("boom"));
        _publisherApiRepository.AddErrorMessageAsync(42, Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(new HttpRequestException("api down"));
        await StartAndWaitForConsumersAsync(1);

        ulong deliveryTag = await DeliverAsync("{\"Id\":42,\"Payload\":\"hello\"}");

        await _channel.Received(1).BasicNackAsync(deliveryTag, false, false, Arg.Any<CancellationToken>());
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Error && record.Message.Contains("Could not report error for message 42"));
    }

    [Fact]
    public async Task AckOnClosedChannel_DoesNotThrow()
    {
        _channel.BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromException(new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Library, 0, "closed"))));
        await StartAndWaitForConsumersAsync(1);

        await DeliverAsync("{\"Id\":42,\"Payload\":\"hello\"}");

        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Warning && record.Message.Contains("channel is closed"));
    }

    [Fact]
    public async Task ShutdownWhileProcessing_LeavesMessageUnacknowledged()
    {
        TaskCompletionSource processingStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _publisherApiRepository.MarkAsProcessedAsync(42, Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            processingStarted.TrySetResult();
            return Task.Delay(Timeout.Infinite, callInfo.ArgAt<CancellationToken>(1));
        });
        await StartAndWaitForConsumersAsync(1);

        Task delivery = DeliverAsync("{\"Id\":42,\"Payload\":\"hello\"}");
        await processingStarted.Task.WaitAsync(WaitTimeout);
        await _processMessage.StopAsync(CancellationToken.None);
        await delivery.WaitAsync(WaitTimeout);

        await _channel.DidNotReceiveWithAnyArgs().BasicAckAsync(default, default, default);
        await _channel.DidNotReceiveWithAnyArgs().BasicNackAsync(default, default, default, default);
        await _publisherApiRepository.DidNotReceiveWithAnyArgs().AddErrorMessageAsync(default, default!, default);
    }

    [Fact]
    public async Task ChannelClosedByBroker_RecreatesChannelAndConsumesAgain()
    {
        IChannel secondChannel = Substitute.For<IChannel>();
        SetupConsumerCapture(secondChannel);
        _rabbitConnectionRepository.CreateChannelAsync(Arg.Any<CancellationToken>()).Returns(_channel, secondChannel);
        await StartAndWaitForConsumersAsync(1);

        _channel.ChannelShutdownAsync += Raise.Event<AsyncEventHandler<ShutdownEventArgs>>(_channel, new ShutdownEventArgs(ShutdownInitiator.Peer, 404, "NOT_FOUND - no queue"));

        await WaitForConsumersAsync(2);
        await _rabbitConnectionRepository.Received(2).CreateChannelAsync(Arg.Any<CancellationToken>());
        await secondChannel.Received(1).BasicQosAsync(0, RabbitMQOptions.PrefetchCount, false, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ConnectionForcedClose_LeavesRecoveryToClientLibrary()
    {
        await StartAndWaitForConsumersAsync(1);

        _channel.ChannelShutdownAsync += Raise.Event<AsyncEventHandler<ShutdownEventArgs>>(_channel, new ShutdownEventArgs(ShutdownInitiator.Peer, Constants.ConnectionForced, "CONNECTION_FORCED"));
        await Task.Delay(200);

        await _rabbitConnectionRepository.Received(1).CreateChannelAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task CreateChannelFails_RetriesAfterBackoff()
    {
        _rabbitConnectionRepository.CreateChannelAsync(Arg.Any<CancellationToken>()).Returns(
            _ => Task.FromException<IChannel>(new BrokerUnreachableException(new IOException("refused"))),
            _ => Task.FromResult(_channel));

        await StartAndWaitForConsumersAsync(1);

        await _rabbitConnectionRepository.Received(2).CreateChannelAsync(Arg.Any<CancellationToken>());
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Error && record.Message.Contains("restarting in"));
    }

    private void SetupConsumerCapture(IChannel channel)
    {
        channel.BasicConsumeAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<IDictionary<string, object?>?>(), Arg.Do<IAsyncBasicConsumer>(consumer =>
            {
                lock (_consumers)
                    _consumers.Add(consumer);
            }), Arg.Any<CancellationToken>())
            .Returns("consumer-tag");
    }

    private async Task StartAndWaitForConsumersAsync(int count)
    {
        await _processMessage.StartAsync(CancellationToken.None);
        await WaitForConsumersAsync(count);
    }

    private async Task WaitForConsumersAsync(int count)
    {
        using CancellationTokenSource timeout = new(WaitTimeout);
        while (true)
        {
            lock (_consumers)
            {
                if (_consumers.Count >= count)
                    return;
            }
            await Task.Delay(10, timeout.Token);
        }
    }

    private async Task<ulong> DeliverAsync(string body)
    {
        IAsyncBasicConsumer consumer;
        lock (_consumers)
            consumer = _consumers[^1];

        ulong deliveryTag = ++_nextDeliveryTag;
        await consumer.HandleBasicDeliverAsync("consumer-tag", deliveryTag, false, "exchange", "routing-key", new BasicProperties(), Encoding.UTF8.GetBytes(body));
        return deliveryTag;
    }
}
