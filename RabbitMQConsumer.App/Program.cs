try
{
    Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateBootstrapLogger();

    Log.Information("RabbitMQConsumer initialization");

    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

    Configurations
        .AddHostedService(builder.Services)
        .AddScopedServices()
        .AddHttpClients()
        .LoadOptionsValues(builder.Configuration);

    IHost host = builder.Build();
    await host.RunAsync();

    return 0;
}
catch (Exception exception)
{
    Log.Fatal(exception, "RabbitMQConsumer terminated unexpectedly");
    return 1;
}
finally
{
    await Log.CloseAndFlushAsync();
}
