namespace RabbitMQConsumer.Infrastructure.Http.PublisherApi;

/// <summary>
/// URLs of the external APIs, bound from the <c>UrlOptions</c> configuration section.
/// </summary>
public record UrlOptions
{
    [Required, ValidateObjectMembers]
    public required PublisherApiOptions PublisherApi { get; init; }

    /// <summary>
    /// Absolute URLs of the publisher API endpoints.
    /// </summary>
    public record PublisherApiOptions
    {
        [Required, Url]
        public required string UpdateMessageAsProcessed { get; init; }
        [Required, Url]
        public required string AddErrorText { get; init; }
    }
}
