namespace RabbitMQConsumer.Domain.Messages;

/// <summary>
/// A message received from the publisher that satisfies the business rules to be processed.
/// </summary>
public sealed record Message
{
    private Message(int id, string payload)
    {
        Id = id;
        Payload = payload;
    }

    public int Id { get; }
    public string Payload { get; }

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
