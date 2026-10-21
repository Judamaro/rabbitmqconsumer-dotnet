namespace RabbitMQConsumer.Application.Messages;

/// <summary>
/// Use case that processes a message received from the broker.
/// </summary>
public interface IProcessMessageUseCase
{
    /// <summary>
    /// Applies the business rules to a received message and notifies the publisher API of the outcome.
    /// </summary>
    /// <param name="messageDto">Message as received from the broker.</param>
    /// <param name="cancellationToken">Token to cancel the processing.</param>
    /// <returns>The outcome that tells the transport whether to acknowledge the message.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled while processing.</exception>
    Task<MessageProcessingResult> ExecuteAsync(MessageDto messageDto, CancellationToken cancellationToken);
}
