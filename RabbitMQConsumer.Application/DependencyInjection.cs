namespace RabbitMQConsumer.Application;

/// <summary>
/// Registers the application layer services in the dependency injection container.
/// </summary>
public static class DependencyInjection
{
    /// <summary>
    /// Registers the application layer services (use cases).
    /// </summary>
    /// <param name="serviceCollection">Service collection to register the services in.</param>
    /// <returns>The same <paramref name="serviceCollection"/> to allow chaining.</returns>
    public static IServiceCollection AddApplication(this IServiceCollection serviceCollection)
    {
        serviceCollection.TryAddScoped<IProcessMessageUseCase, ProcessMessageUseCase>();

        return serviceCollection;
    }
}
