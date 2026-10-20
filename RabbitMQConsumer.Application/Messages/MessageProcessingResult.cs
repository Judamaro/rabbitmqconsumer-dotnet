namespace RabbitMQConsumer.Application.Messages;

public enum MessageProcessingResult
{
    /// <summary>The message was processed and can be acknowledged.</summary>
    Processed,

    /// <summary>The message does not satisfy the business rules and must not be retried.</summary>
    Rejected,

    /// <summary>The message was valid but processing failed; the error was reported to the publisher.</summary>
    Failed
}
