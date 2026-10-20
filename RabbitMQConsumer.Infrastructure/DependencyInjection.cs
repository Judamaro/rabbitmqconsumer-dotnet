namespace RabbitMQConsumer.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddInfrastructure(this IServiceCollection serviceCollection, IConfiguration configuration)
    {
        return serviceCollection
            .AddOptionsValues(configuration)
            .AddRabbitMQ()
            .AddPublisherApi();
    }

    private static IServiceCollection AddOptionsValues(this IServiceCollection serviceCollection, IConfiguration configuration)
    {
        serviceCollection
            .AddOptions<RabbitMQOptions>()
            .Bind(configuration.GetSection(nameof(RabbitMQOptions)))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        serviceCollection
            .AddOptions<UrlOptions>()
            .Bind(configuration.GetSection(nameof(UrlOptions)))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return serviceCollection;
    }

    private static IServiceCollection AddRabbitMQ(this IServiceCollection serviceCollection)
    {
        serviceCollection.TryAddSingleton<IConnectionFactory>(serviceProvider => RabbitMQConnectionFactoryBuilder.Build(serviceProvider.GetRequiredService<IOptions<RabbitMQOptions>>().Value));
        serviceCollection.TryAddSingleton<IRabbitMQConnectionProvider, RabbitMQConnectionProvider>();
        serviceCollection.AddHostedService<RabbitMQMessageConsumer>();

        return serviceCollection;
    }

    private static IServiceCollection AddPublisherApi(this IServiceCollection serviceCollection)
    {
        serviceCollection
            .AddHttpClient<IJsonHttpClient, JsonHttpClient>()
            .AddStandardResilienceHandler(options =>
            {
                options.Retry.MaxRetryAttempts = 3;
                options.Retry.BackoffType = DelayBackoffType.Exponential;
                options.Retry.UseJitter = true;
                options.Retry.Delay = TimeSpan.FromSeconds(2);
                options.AttemptTimeout.Timeout = TimeSpan.FromSeconds(10);
                options.TotalRequestTimeout.Timeout = TimeSpan.FromSeconds(60);
                // SamplingDuration must be at least twice the AttemptTimeout
                options.CircuitBreaker.SamplingDuration = TimeSpan.FromSeconds(30);
                options.CircuitBreaker.FailureRatio = 0.5;
                options.CircuitBreaker.MinimumThroughput = 10;
                options.CircuitBreaker.BreakDuration = TimeSpan.FromSeconds(30);
            });

        serviceCollection.TryAddScoped<IPublisherApiClient, PublisherApiClient>();

        return serviceCollection;
    }
}
