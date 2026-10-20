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

    public static IServiceCollection AddHttpClients(this IServiceCollection serviceCollection)
    {
        serviceCollection
            .AddHttpClient<IHttpClientRepository, HttpClientRepository>()
            .AddStandardResilienceHandler(options =>
            {
                options.Retry.MaxRetryAttempts = 3;
                options.Retry.BackoffType = DelayBackoffType.Exponential;
                options.Retry.UseJitter = true;
                options.Retry.Delay = TimeSpan.FromSeconds(2);
                options.AttemptTimeout.Timeout = TimeSpan.FromSeconds(10);
                options.TotalRequestTimeout.Timeout = TimeSpan.FromSeconds(60);
                options.CircuitBreaker.SamplingDuration = TimeSpan.FromSeconds(30);
                options.CircuitBreaker.FailureRatio = 0.5;
                options.CircuitBreaker.MinimumThroughput = 10;
                options.CircuitBreaker.BreakDuration = TimeSpan.FromSeconds(30);
            });

        return serviceCollection;
    }

    public static IServiceCollection AddScopedServices(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddSingleton<IRabbitConnectionRepository, RabbitConnectionRepository>();

        return serviceCollection;
    }
}
