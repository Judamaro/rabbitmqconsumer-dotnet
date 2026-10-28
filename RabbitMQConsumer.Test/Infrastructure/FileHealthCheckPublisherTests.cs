namespace RabbitMQConsumer.Test.Infrastructure;

public sealed class FileHealthCheckPublisherTests : IDisposable
{
    private readonly string _filePath = Path.Combine(Path.GetTempPath(), $"healthy-{Guid.NewGuid():N}");
    private readonly FakeLogger<FileHealthCheckPublisher> _logger = new();
    private readonly FileHealthCheckPublisher _publisher;

    public FileHealthCheckPublisherTests()
    {
        _publisher = new FileHealthCheckPublisher(Options.Create(new HealthCheckFileOptions { FilePath = _filePath }), _logger);
    }

    public void Dispose() => File.Delete(_filePath);

    [Fact]
    public async Task Healthy_WritesFile()
    {
        await _publisher.PublishAsync(CreateReport(HealthStatus.Healthy), CancellationToken.None);

        Assert.True(File.Exists(_filePath));
    }

    [Fact]
    public async Task HealthyAgain_RefreshesFile()
    {
        await _publisher.PublishAsync(CreateReport(HealthStatus.Healthy), CancellationToken.None);
        DateTime oldTime = DateTime.UtcNow.AddMinutes(-5);
        File.SetLastWriteTimeUtc(_filePath, oldTime);

        await _publisher.PublishAsync(CreateReport(HealthStatus.Healthy), CancellationToken.None);

        Assert.True(File.GetLastWriteTimeUtc(_filePath) > oldTime);
    }

    [Fact]
    public async Task Unhealthy_DeletesFileAndLogsReason()
    {
        await _publisher.PublishAsync(CreateReport(HealthStatus.Healthy), CancellationToken.None);

        await _publisher.PublishAsync(CreateReport(HealthStatus.Unhealthy, "RabbitMQ connection is not open"), CancellationToken.None);

        Assert.False(File.Exists(_filePath));
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Warning && record.Message.Contains("RabbitMQ connection is not open"));
    }

    [Fact]
    public async Task SameStatus_LogsOnlyOnce()
    {
        await _publisher.PublishAsync(CreateReport(HealthStatus.Healthy), CancellationToken.None);
        await _publisher.PublishAsync(CreateReport(HealthStatus.Healthy), CancellationToken.None);

        Assert.Single(_logger.Collector.GetSnapshot());
    }

    private static HealthReport CreateReport(HealthStatus status, string? description = null)
    {
        Dictionary<string, HealthReportEntry> entries = new()
        {
            ["rabbitmq"] = new HealthReportEntry(status, description, TimeSpan.Zero, null, null)
        };
        return new HealthReport(entries, TimeSpan.Zero);
    }
}
