namespace RabbitMQConsumer.Configuration;

public static class Configurations
{
    public static IServiceCollection LoadOptionsValues(this IServiceCollection serviceCollection, IConfigurationManager configurationManager)
    {
        serviceCollection
            .AddOptions<RabbitMQOptions>()
            .Bind(configurationManager.GetSection(nameof(RabbitMQOptions)))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        serviceCollection
            .AddOptions<UrlOptions>()
            .Bind(configurationManager.GetSection(nameof(UrlOptions)))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return serviceCollection;
    }

    public static IServiceCollection AddHostedService(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddHostedService<ProcessMessage>();
        
        return serviceCollection;
    }

    public static IServiceCollection AddScopedServices(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddSingleton<IRabbitConnectionRepository, RabbitConnectionRepository>();

        return serviceCollection;
    }
}
