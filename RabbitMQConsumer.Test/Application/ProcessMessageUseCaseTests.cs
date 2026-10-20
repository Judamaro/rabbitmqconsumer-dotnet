namespace RabbitMQConsumer.Test.Application;

public class ProcessMessageUseCaseTests
{
    private readonly IPublisherApiClient _publisherApiClient = Substitute.For<IPublisherApiClient>();
    private readonly FakeLogger<ProcessMessageUseCase> _logger = new();
    private readonly ProcessMessageUseCase _useCase;

    public ProcessMessageUseCaseTests()
    {
        _useCase = new ProcessMessageUseCase(_publisherApiClient, _logger);
    }

    [Fact]
    public async Task ValidMessage_MarksAsProcessed()
    {
        using CancellationTokenSource cancellationTokenSource = new();

        MessageProcessingResult result = await _useCase.ExecuteAsync(new MessageDto { Id = 42, Payload = "hello" }, cancellationTokenSource.Token);

        Assert.Equal(MessageProcessingResult.Processed, result);
        await _publisherApiClient.Received(1).MarkAsProcessedAsync(42, cancellationTokenSource.Token);
        await _publisherApiClient.DidNotReceiveWithAnyArgs().AddErrorMessageAsync(default, default!, default);
    }

    [Theory]
    [InlineData(0, "hello")]
    [InlineData(42, null)]
    [InlineData(42, "")]
    public async Task InvalidMessage_IsRejectedWithoutCallingApi(int id, string? payload)
    {
        MessageProcessingResult result = await _useCase.ExecuteAsync(new MessageDto { Id = id, Payload = payload }, CancellationToken.None);

        Assert.Equal(MessageProcessingResult.Rejected, result);
        Assert.Empty(_publisherApiClient.ReceivedCalls());
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Error && record.Message.Contains("rejected"));
    }

    [Fact]
    public async Task MarkAsProcessedFails_ReportsErrorAndReturnsFailed()
    {
        _publisherApiClient.MarkAsProcessedAsync(42, Arg.Any<CancellationToken>()).ThrowsAsync(new HttpRequestException("boom"));

        MessageProcessingResult result = await _useCase.ExecuteAsync(new MessageDto { Id = 42, Payload = "hello" }, CancellationToken.None);

        Assert.Equal(MessageProcessingResult.Failed, result);
        await _publisherApiClient.Received(1).AddErrorMessageAsync(42, "boom", Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AddErrorMessageAlsoFails_ReturnsFailedAndLogsError()
    {
        _publisherApiClient.MarkAsProcessedAsync(42, Arg.Any<CancellationToken>()).ThrowsAsync(new HttpRequestException("boom"));
        _publisherApiClient.AddErrorMessageAsync(42, Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(new HttpRequestException("api down"));

        MessageProcessingResult result = await _useCase.ExecuteAsync(new MessageDto { Id = 42, Payload = "hello" }, CancellationToken.None);

        Assert.Equal(MessageProcessingResult.Failed, result);
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Error && record.Message.Contains("Could not report error for message 42"));
    }

    [Fact]
    public async Task CancelledWhileProcessing_PropagatesCancellationWithoutReportingError()
    {
        using CancellationTokenSource cancellationTokenSource = new();
        _publisherApiClient.MarkAsProcessedAsync(42, Arg.Any<CancellationToken>()).Returns(async callInfo =>
        {
            await cancellationTokenSource.CancelAsync();
            callInfo.ArgAt<CancellationToken>(1).ThrowIfCancellationRequested();
        });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _useCase.ExecuteAsync(new MessageDto { Id = 42, Payload = "hello" }, cancellationTokenSource.Token));

        await _publisherApiClient.DidNotReceiveWithAnyArgs().AddErrorMessageAsync(default, default!, default);
    }

    [Fact]
    public async Task NullMessage_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _useCase.ExecuteAsync(null!, CancellationToken.None));
    }
}
