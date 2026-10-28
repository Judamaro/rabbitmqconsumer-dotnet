namespace RabbitMQConsumer.Application.Messages;

/// <summary>
/// Applies the domain rules to each message and notifies the publisher API whether it was processed or failed.
/// </summary>
/// <param name="_publisherApiClient">Port used to notify the publisher API.</param>
/// <param name="_logger">Logger for rejected and failed messages.</param>
public sealed class ProcessMessageUseCase(IPublisherApiClient _publisherApiClient, ILogger<ProcessMessageUseCase> _logger) : IProcessMessageUseCase
{
    /// <inheritdoc/>
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
            await _publisherApiClient.UpdateMessageAsProcessedAsync(message.Id, cancellationToken);
            return MessageProcessingResult.Processed;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Message {MessageId} could not be processed: {ExceptionMessage}", message.Id, exception.Message);
            await TryAddErrorTextAsync(message.Id, exception.Message, cancellationToken);
            return MessageProcessingResult.Failed;
        }
    }

    /// <summary>
    /// Reports the processing error to the publisher API on a best-effort basis: failures are logged and never thrown, except cancellation.
    /// </summary>
    /// <param name="messageId">Identifier of the message that failed.</param>
    /// <param name="errorText">Text describing the error.</param>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    private async Task TryAddErrorTextAsync(int messageId, string errorText, CancellationToken cancellationToken)
    {
        try
        {
            await _publisherApiClient.AddErrorTextAsync(messageId, errorText, cancellationToken);
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
