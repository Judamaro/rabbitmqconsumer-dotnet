namespace RabbitMQConsumer.Configuration;

public static class Configurations
{
    public static IServiceCollection LoadOptionsValues(this IServiceCollection serviceCollection, IConfigurationManager configurationManager)
    {
        serviceCollection
            .AddOptions<RabbitMQOptions>()
            .Bind(configurationManager.GetSection(typeof(RabbitMQOptions).Name))
            .ValidateOnStart();

        serviceCollection
            .AddOptions<UrlOptions>()
            .Bind(configurationManager.GetSection(typeof(UrlOptions).Name))
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
