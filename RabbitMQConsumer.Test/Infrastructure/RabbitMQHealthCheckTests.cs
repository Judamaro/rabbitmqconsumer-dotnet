namespace RabbitMQConsumer.Test.Infrastructure;

public class RabbitMQHealthCheckTests
{
    private static readonly RabbitMQOptions RabbitMQOptions = new()
    {
        Name = "consumer-test",
        Host = "rabbit.test",
        User = "user",
        Pwd = "pwd",
        Queue = "messages",
        Port = 5672,
        VirtualHost = "/",
        PrefetchCount = 10,
        HeartbeatSeconds = 30,
        NetworkRecoveryIntervalSeconds = 5,
        MaxConnectRetryDelaySeconds = 60
    };

    private readonly IRabbitMQConnectionProvider _connectionProvider = Substitute.For<IRabbitMQConnectionProvider>();
    private readonly RabbitMQConsumerStatus _consumerStatus = new();
    private readonly RabbitMQHealthCheck _healthCheck;

    public RabbitMQHealthCheckTests()
    {
        _healthCheck = new RabbitMQHealthCheck(Options.Create(RabbitMQOptions), _connectionProvider, _consumerStatus);
    }

    [Fact]
    public async Task ConnectedAndConsuming_ReturnsHealthy()
    {
        _connectionProvider.IsConnected.Returns(true);
        _consumerStatus.MarkConsuming();

        HealthCheckResult result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task NotConnected_ReturnsUnhealthy()
    {
        _connectionProvider.IsConnected.Returns(false);
        _consumerStatus.MarkConsuming();

        HealthCheckResult result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("connection", result.Description);
    }

    [Fact]
    public async Task ConnectedButNotConsuming_ReturnsUnhealthyWithQueueName()
    {
        _connectionProvider.IsConnected.Returns(true);
        _consumerStatus.MarkStopped();

        HealthCheckResult result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains(RabbitMQOptions.Queue, result.Description);
    }
}
