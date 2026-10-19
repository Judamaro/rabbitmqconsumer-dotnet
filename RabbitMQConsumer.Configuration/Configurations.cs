namespace RabbitMQConsumer.Configuration;

public static class Configurations
{
    public static void LoadOptionsValues(IServiceCollection serviceCollection, IConfigurationManager configurationManager)
    {
        serviceCollection
            .AddOptions<RabbitMQOptions>()
            .Bind(configurationManager.GetSection(typeof(RabbitMQOptions).Name))
            .ValidateOnStart();

        serviceCollection
            .AddOptions<UrlOptions>()
            .Bind(configurationManager.GetSection(typeof(UrlOptions).Name))
            .ValidateOnStart();
    }

    public static void AddHostedService(IServiceCollection serviceCollection)
        => serviceCollection.AddHostedService<ProcessMessage>();
}
