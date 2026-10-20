namespace RabbitMQConsumer.Shared.OptionsValues;

public record UrlOptions
{
    public record PublisherApi
    {        
        public required string MarkAsProcessed { get; init; }
        public required string AddErrorMessage { get; init; }
    }
}
