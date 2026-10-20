namespace RabbitMQConsumer.Shared.OptionsValues;

public record UrlOptions
{
    [Required, ValidateObjectMembers]
    public required PublisherApiOptions PublisherApi { get; init; }

    public record PublisherApiOptions
    {
        [Required, Url]
        public required string MarkAsProcessed { get; init; }
        [Required, Url]
        public required string AddErrorMessage { get; init; }
    }
}
