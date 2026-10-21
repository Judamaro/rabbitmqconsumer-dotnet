namespace RabbitMQConsumer.Domain.Messages;

/// <summary>
/// A message received from the publisher that satisfies the business rules to be processed.
/// </summary>
public sealed record Message
{
    /// <summary>
    /// Creates a message from values already validated by <see cref="TryCreate"/>.
    /// </summary>
    /// <param name="id">Message identifier.</param>
    /// <param name="payload">Message content.</param>
    private Message(int id, string payload)
    {
        Id = id;
        Payload = payload;
    }

    public int Id { get; }
    public string Payload { get; }

    /// <summary>
    /// Creates a <see cref="Message"/> when the values satisfy the business rules: the id must be greater than zero and the payload must not be empty.
    /// </summary>
    /// <param name="id">Message identifier.</param>
    /// <param name="payload">Message content.</param>
    /// <param name="message">The created message, or <see langword="null"/> when a rule is not satisfied.</param>
    /// <param name="error">Description of the broken rule, or <see langword="null"/> when the message is valid.</param>
    /// <returns><see langword="true"/> when the message is valid; otherwise <see langword="false"/>.</returns>
    public static bool TryCreate(int id, string? payload, [NotNullWhen(true)] out Message? message, [NotNullWhen(false)] out string? error)
    {
        message = null;

        if (id <= 0)
        {
            error = "Id must be greater than zero";
            return false;
        }

        if (string.IsNullOrWhiteSpace(payload))
        {
            error = "Payload must not be empty";
            return false;
        }

        message = new Message(id, payload);
        error = null;
        return true;
    }
}
