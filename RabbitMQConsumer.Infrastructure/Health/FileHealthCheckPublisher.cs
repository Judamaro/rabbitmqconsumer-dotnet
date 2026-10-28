namespace RabbitMQConsumer.Infrastructure.Health;

/// <summary>
/// Publishes the health report to a file so a Docker healthcheck can read it without an HTTP endpoint:
/// the file is rewritten on every healthy report and deleted when the service is not healthy.
/// </summary>
/// <param name="_healthCheckFileOptions">Path of the health status file.</param>
/// <param name="_logger">Logger for status changes and file errors.</param>
public sealed class FileHealthCheckPublisher(IOptions<HealthCheckFileOptions> _healthCheckFileOptions, ILogger<FileHealthCheckPublisher> _logger) : IHealthCheckPublisher
{
    private HealthStatus? _lastStatus;

    /// <inheritdoc/>
    public async Task PublishAsync(HealthReport report, CancellationToken cancellationToken)
    {
        string filePath = _healthCheckFileOptions.Value.FilePath;

        try
        {
            if (report.Status == HealthStatus.Healthy)
                await File.WriteAllTextAsync(filePath, DateTimeOffset.UtcNow.ToString("O"), cancellationToken);
            else
                File.Delete(filePath);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            _logger.LogError(exception, "Could not update health status file {FilePath}", filePath);
        }

        LogStatusChange(report);
    }

    /// <summary>
    /// Logs the report only when the overall status changes, so periodic publications do not flood the logs.
    /// </summary>
    /// <param name="report">Report that was just published.</param>
    private void LogStatusChange(HealthReport report)
    {
        if (_lastStatus == report.Status)
            return;

        _lastStatus = report.Status;

        if (report.Status == HealthStatus.Healthy)
        {
            _logger.LogInformation("Health status changed to {HealthStatus}", report.Status);
            return;
        }

        string reasons = string.Join("; ", report.Entries.Where(entry => entry.Value.Status != HealthStatus.Healthy).Select(entry => $"{entry.Key}: {entry.Value.Description}"));
        _logger.LogWarning("Health status changed to {HealthStatus}: {Reasons}", report.Status, reasons);
    }
}
