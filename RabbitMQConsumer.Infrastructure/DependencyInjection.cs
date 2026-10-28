namespace RabbitMQConsumer.Infrastructure;

/// <summary>
/// Registers the infrastructure adapters in the dependency injection container.
/// </summary>
public static class DependencyInjection
{
    /// <summary>
    /// Registers the infrastructure adapters: validated options, the RabbitMQ consumer and connection, the publisher API client and the health checks.
    /// </summary>
    /// <param name="serviceCollection">Service collection to register the services in.</param>
    /// <param name="configuration">Application configuration that provides the options sections.</param>
    /// <returns>The same <paramref name="serviceCollection"/> to allow chaining.</returns>
    public static IServiceCollection AddInfrastructure(this IServiceCollection serviceCollection, IConfiguration configuration)
    {
        return serviceCollection
            .AddOptionsValues(configuration)
            .AddRabbitMQ()
            .AddPublisherApi()
            .AddHealthStatusFile();
    }

    /// <summary>
    /// Binds <see cref="RabbitMQOptions"/> and <see cref="UrlOptions"/> from configuration and validates them on startup.
    /// </summary>
    /// <param name="serviceCollection">Service collection to register the options in.</param>
    /// <param name="configuration">Application configuration that provides the options sections.</param>
    /// <returns>The same <paramref name="serviceCollection"/> to allow chaining.</returns>
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

        serviceCollection
            .AddOptions<HealthCheckFileOptions>()
            .Bind(configuration.GetSection(nameof(HealthCheckFileOptions)))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return serviceCollection;
    }

    /// <summary>
    /// Registers the RabbitMQ connection factory, the shared connection provider and the hosted consumer.
    /// </summary>
    /// <param name="serviceCollection">Service collection to register the services in.</param>
    /// <returns>The same <paramref name="serviceCollection"/> to allow chaining.</returns>
    private static IServiceCollection AddRabbitMQ(this IServiceCollection serviceCollection)
    {
        serviceCollection.TryAddSingleton<IConnectionFactory>(serviceProvider => RabbitMQConnectionFactoryBuilder.Build(serviceProvider.GetRequiredService<IOptions<RabbitMQOptions>>().Value));
        serviceCollection.TryAddSingleton<IRabbitMQConnectionProvider, RabbitMQConnectionProvider>();
        serviceCollection.TryAddSingleton<RabbitMQConsumerStatus>();
        serviceCollection.AddHostedService<RabbitMQMessageConsumer>();

        return serviceCollection;
    }

    /// <summary>
    /// Registers the RabbitMQ health check and publishes its result periodically to the health status file read by the Docker healthcheck.
    /// </summary>
    /// <param name="serviceCollection">Service collection to register the services in.</param>
    /// <returns>The same <paramref name="serviceCollection"/> to allow chaining.</returns>
    private static IServiceCollection AddHealthStatusFile(this IServiceCollection serviceCollection)
    {
        serviceCollection
            .AddHealthChecks()
            .AddCheck<RabbitMQHealthCheck>("rabbitmq");

        serviceCollection.Configure<HealthCheckPublisherOptions>(options =>
        {
            options.Delay = TimeSpan.FromSeconds(5);
            options.Period = TimeSpan.FromSeconds(10);
        });

        serviceCollection.TryAddEnumerable(ServiceDescriptor.Singleton<IHealthCheckPublisher, FileHealthCheckPublisher>());

        return serviceCollection;
    }

    /// <summary>
    /// Registers the JSON HTTP client with the standard resilience pipeline (retries, timeouts and circuit breaker) and the publisher API client.
    /// </summary>
    /// <param name="serviceCollection">Service collection to register the services in.</param>
    /// <returns>The same <paramref name="serviceCollection"/> to allow chaining.</returns>
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
