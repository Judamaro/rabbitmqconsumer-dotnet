try
{
    Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateBootstrapLogger();

    Log.Information("RabbitMQConsumer initialization");

    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

    Configurations.LoadOptionsValues(builder.Services, builder.Configuration);

    Configurations.AddHostedService(builder.Services);

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
