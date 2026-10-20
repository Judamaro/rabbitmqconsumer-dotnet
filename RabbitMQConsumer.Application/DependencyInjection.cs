namespace RabbitMQConsumer.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddApplication(this IServiceCollection serviceCollection)
    {
        serviceCollection.TryAddScoped<IProcessMessageUseCase, ProcessMessageUseCase>();

        return serviceCollection;
    }
}
