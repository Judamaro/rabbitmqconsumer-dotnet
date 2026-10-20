namespace RabbitMQConsumer.Application.Services;

public class ProcessMessage(ILogger<ProcessMessage> _logger, IOptions<RabbitMQOptions> _rabbitMQOptions, IRabbitConnectionRepository _rabbitConnectionRepository) : BackgroundService
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

    private async Task ConsumeUntilChannelClosedAsync(Action onConsumerStarted, CancellationToken stoppingToken)
    {
        await using IChannel channel = await _rabbitConnectionRepository.CreateChannelAsync(stoppingToken);

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
        consumer.ReceivedAsync += (_, eventArgs) => HandleMessageAsync(channel, eventArgs);

        await channel.BasicConsumeAsync(queue: _rabbitMQOptions.Value.Queue, autoAck: false, consumer: consumer, cancellationToken: stoppingToken);
        _logger.LogInformation("Consuming messages from queue {Queue}", _rabbitMQOptions.Value.Queue);
        onConsumerStarted();

        await channelClosed.Task.WaitAsync(stoppingToken);
    }

    private async Task HandleMessageAsync(IChannel channel, BasicDeliverEventArgs eventArgs)
    {
        bool processed = false;
        try
        {
            MessageDto? messageDto = GetMessageFromBodyEvent(eventArgs);
            processed = messageDto is not null;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Exception consumer.ReceivedAsync: {ExceptionMessage}", exception.Message);
        }

        try
        {
            if (processed)
                await channel.BasicAckAsync(deliveryTag: eventArgs.DeliveryTag, multiple: false);
            else
                await channel.BasicNackAsync(deliveryTag: eventArgs.DeliveryTag, multiple: false, requeue: false);
        }
        catch (AlreadyClosedException exception)
        {
            _logger.LogWarning(exception, "Could not ack/nack delivery {DeliveryTag}, channel is closed", eventArgs.DeliveryTag);
        }
    }

    private MessageDto? GetMessageFromBodyEvent(BasicDeliverEventArgs basicDeliverEventArgs)
    {
        if (basicDeliverEventArgs.Body.IsEmpty)
        {
            _logger.LogError("EventArg body is empty");
            return null;
        }

        string bodyAsString = Encoding.UTF8.GetString(basicDeliverEventArgs.Body.Span);
        if (string.IsNullOrWhiteSpace(bodyAsString))
        {
            _logger.LogError("EventArg body as string is empty");
            return null;
        }

        MessageDto? bodyDeserialized = JsonConvert.DeserializeObject<MessageDto>(bodyAsString);
        if (bodyDeserialized is null)
        {
            _logger.LogError("Body deserialized is null");
            return null;
        }

        if (bodyDeserialized.Id == 0 || string.IsNullOrEmpty(bodyDeserialized.Payload))
        {
            _logger.LogError("Body deserialized id is 0 or payload is empty");
            return null;
        }

        return bodyDeserialized;
    }
}
