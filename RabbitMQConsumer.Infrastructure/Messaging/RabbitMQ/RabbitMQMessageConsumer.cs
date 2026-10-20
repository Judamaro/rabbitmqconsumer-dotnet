namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

/// <summary>
/// Consumes the RabbitMQ queue and delegates each message to <see cref="IProcessMessageUseCase"/>.
/// Owns only transport concerns: channel lifecycle, prefetch, deserialization and ack/nack.
/// </summary>
public sealed class RabbitMQMessageConsumer(ILogger<RabbitMQMessageConsumer> _logger, IOptions<RabbitMQOptions> _rabbitMQOptions, IRabbitMQConnectionProvider _rabbitMQConnectionProvider, IServiceScopeFactory _serviceScopeFactory) : BackgroundService
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        TimeSpan maxRetryDelay = TimeSpan.FromSeconds(_rabbitMQOptions.Value.MaxConnectRetryDelaySeconds);
        TimeSpan retryDelay = InitialRetryDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeUntilChannelClosedAsync(() => retryDelay = InitialRetryDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "RabbitMQ consumer failed, restarting in {RetryDelay}", retryDelay);
                try
                {
                    await Task.Delay(retryDelay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                retryDelay = TimeSpan.FromTicks(Math.Min(retryDelay.Ticks * 2, maxRetryDelay.Ticks));
            }
        }

        _logger.LogInformation("RabbitMQ consumer stopped");
    }

    /// <summary>
    /// Consumes from the queue until the channel is closed by a channel-level error (which automatic recovery does not handle).
    /// Connection-level failures are recovered by the client library, which also re-registers the consumer.
    /// </summary>
    private async Task ConsumeUntilChannelClosedAsync(Action onConsumerStarted, CancellationToken stoppingToken)
    {
        await using IChannel channel = await _rabbitMQConnectionProvider.CreateChannelAsync(stoppingToken);

        TaskCompletionSource channelClosed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        channel.ChannelShutdownAsync += (_, eventArgs) =>
        {
            if (eventArgs.Initiator == ShutdownInitiator.Peer && eventArgs.ReplyCode != Constants.ConnectionForced)
            {
                _logger.LogWarning("RabbitMQ channel closed by broker. Code: {ReplyCode}, Reason: {ReplyText}", eventArgs.ReplyCode, eventArgs.ReplyText);
                channelClosed.TrySetResult();
            }
            return Task.CompletedTask;
        };

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _rabbitMQOptions.Value.PrefetchCount, global: false, cancellationToken: stoppingToken);

        AsyncEventingBasicConsumer consumer = new(channel);
        consumer.ReceivedAsync += (_, eventArgs) => HandleDeliveryAsync(channel, eventArgs, stoppingToken);

        await channel.BasicConsumeAsync(queue: _rabbitMQOptions.Value.Queue, autoAck: false, consumer: consumer, cancellationToken: stoppingToken);
        _logger.LogInformation("Consuming messages from queue {Queue}", _rabbitMQOptions.Value.Queue);
        onConsumerStarted();

        await channelClosed.Task.WaitAsync(stoppingToken);
    }

    private async Task HandleDeliveryAsync(IChannel channel, BasicDeliverEventArgs eventArgs, CancellationToken stoppingToken)
    {
        MessageProcessingResult result;
        try
        {
            MessageDto? messageDto = DeserializeMessage(eventArgs);
            if (messageDto is null)
            {
                result = MessageProcessingResult.Rejected;
            }
            else
            {
                await using AsyncServiceScope scope = _serviceScopeFactory.CreateAsyncScope();
                IProcessMessageUseCase processMessageUseCase = scope.ServiceProvider.GetRequiredService<IProcessMessageUseCase>();
                result = await processMessageUseCase.ExecuteAsync(messageDto, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Unexpected error handling delivery {DeliveryTag}", eventArgs.DeliveryTag);
            result = MessageProcessingResult.Failed;
        }

        await AckOrNackAsync(channel, eventArgs.DeliveryTag, result);
    }

    private async Task AckOrNackAsync(IChannel channel, ulong deliveryTag, MessageProcessingResult result)
    {
        try
        {
            if (result == MessageProcessingResult.Processed)
                await channel.BasicAckAsync(deliveryTag: deliveryTag, multiple: false);
            else
                await channel.BasicNackAsync(deliveryTag: deliveryTag, multiple: false, requeue: false);
        }
        catch (AlreadyClosedException exception)
        {
            _logger.LogWarning(exception, "Could not ack/nack delivery {DeliveryTag}, channel is closed", deliveryTag);
        }
    }

    private MessageDto? DeserializeMessage(BasicDeliverEventArgs basicDeliverEventArgs)
    {
        if (basicDeliverEventArgs.Body.IsEmpty)
        {
            _logger.LogError("Delivery {DeliveryTag} has an empty body", basicDeliverEventArgs.DeliveryTag);
            return null;
        }

        string body = Encoding.UTF8.GetString(basicDeliverEventArgs.Body.Span);
        try
        {
            MessageDto? messageDto = JsonConvert.DeserializeObject<MessageDto>(body);
            if (messageDto is null)
                _logger.LogError("Delivery {DeliveryTag} body deserialized to null", basicDeliverEventArgs.DeliveryTag);

            return messageDto;
        }
        catch (JsonException exception)
        {
            _logger.LogError(exception, "Delivery {DeliveryTag} body is not valid JSON", basicDeliverEventArgs.DeliveryTag);
            return null;
        }
    }
}
