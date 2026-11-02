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
        /// <summary>
        /// Placeholder replaced by the message id in <see cref="UpdateMessageAsProcessed"/>.
        /// </summary>
        public const string MessageIdPlaceholder = "{id}";

        /// <summary>
        /// URL template of the endpoint called with PATCH when a message is processed. It must contain <c>{id}</c>,
        /// e.g. <c>https://publisher/api/Message/{id}/UpdateAsProcessed</c>.
        /// </summary>
        [Required, Url]
        // ErrorMessage is passed to string.Format, so the braces of the placeholder are escaped
        [RegularExpression(@".*\{id\}.*", ErrorMessage = "The {0} URL must contain the {{id}} placeholder.")]
        public required string UpdateMessageAsProcessed { get; init; }

        /// <summary>
        /// URL of the endpoint called with POST and <c>{ "messageId": …, "text": … }</c> when processing fails,
        /// e.g. <c>https://publisher/api/Error/Add</c>.
        /// </summary>
        [Required, Url]
        public required string AddErrorText { get; init; }
    }
}
