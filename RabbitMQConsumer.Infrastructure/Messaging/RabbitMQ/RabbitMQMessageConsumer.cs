namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

/// <summary>
/// Consumes the RabbitMQ queue and delegates each message to <see cref="IProcessMessageUseCase"/>.
/// Owns only transport concerns: channel lifecycle, prefetch, deserialization and ack/nack.
/// </summary>
public sealed class RabbitMQMessageConsumer(ILogger<RabbitMQMessageConsumer> _logger, IOptions<RabbitMQOptions> _rabbitMQOptions, IRabbitMQConnectionProvider _rabbitMQConnectionProvider, IServiceScopeFactory _serviceScopeFactory) : BackgroundService
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Keeps consuming the queue until the host stops, recreating the channel after channel-level errors and retrying with exponential backoff when it cannot be created.
    /// </summary>
    /// <param name="stoppingToken">Token signaled when the host is stopping.</param>
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

    /// <summary>
    /// Handles one delivery: deserializes it, runs the use case in its own dependency scope and acknowledges the message according to the result.
    /// When the host is stopping the delivery is left unacknowledged so the broker redelivers it.
    /// </summary>
    /// <param name="channel">Channel the message was delivered on.</param>
    /// <param name="eventArgs">Delivery data, including the body and the delivery tag.</param>
    /// <param name="stoppingToken">Token signaled when the host is stopping.</param>
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

    /// <summary>
    /// Acknowledges processed messages and rejects any other result without requeue. Ignores the error when the channel is already closed, since the broker will redeliver the message.
    /// </summary>
    /// <param name="channel">Channel the message was delivered on.</param>
    /// <param name="deliveryTag">Delivery tag of the message.</param>
    /// <param name="result">Outcome of the processing.</param>
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

    /// <summary>
    /// Deserializes the delivery body as JSON, ignoring a leading UTF-8 BOM.
    /// </summary>
    /// <param name="basicDeliverEventArgs">Delivery data that contains the body.</param>
    /// <returns>The message, or <see langword="null"/> when the body is empty, is not valid JSON or is the JSON <c>null</c> literal.</returns>
    private MessageDto? DeserializeMessage(BasicDeliverEventArgs basicDeliverEventArgs)
    {
        if (basicDeliverEventArgs.Body.IsEmpty)
        {
            _logger.LogError("Delivery {DeliveryTag} has an empty body", basicDeliverEventArgs.DeliveryTag);
            return null;
        }

        ReadOnlySpan<byte> body = basicDeliverEventArgs.Body.Span;
        if (body.StartsWith(Encoding.UTF8.Preamble))
            body = body[Encoding.UTF8.Preamble.Length..];

        try
        {
            MessageDto? messageDto = JsonSerializer.Deserialize(body, MessageJsonContext.Default.MessageDto);
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
