namespace RabbitMQConsumer.Application.Messages;

public sealed class ProcessMessageUseCase(IPublisherApiClient _publisherApiClient, ILogger<ProcessMessageUseCase> _logger) : IProcessMessageUseCase
{
    public async Task<MessageProcessingResult> ExecuteAsync(MessageDto messageDto, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messageDto);

        if (!Message.TryCreate(messageDto.Id, messageDto.Payload, out Message? message, out string? validationError))
        {
            _logger.LogError("Message {MessageId} rejected: {ValidationError}", messageDto.Id, validationError);
            return MessageProcessingResult.Rejected;
        }

        try
        {
            await _publisherApiClient.MarkAsProcessedAsync(message.Id, cancellationToken);
            return MessageProcessingResult.Processed;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Message {MessageId} could not be processed: {ExceptionMessage}", message.Id, exception.Message);
            await TryAddErrorMessageAsync(message.Id, exception.Message, cancellationToken);
            return MessageProcessingResult.Failed;
        }
    }

    private async Task TryAddErrorMessageAsync(int messageId, string errorMessage, CancellationToken cancellationToken)
    {
        try
        {
            await _publisherApiClient.AddErrorMessageAsync(messageId, errorMessage, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Could not report error for message {MessageId} to the publisher API", messageId);
        }
    }
}
