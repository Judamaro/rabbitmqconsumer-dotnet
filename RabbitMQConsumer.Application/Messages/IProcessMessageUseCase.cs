namespace RabbitMQConsumer.Application.Messages;

public interface IProcessMessageUseCase
{
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled while processing.</exception>
    Task<MessageProcessingResult> ExecuteAsync(MessageDto messageDto, CancellationToken cancellationToken);
}
