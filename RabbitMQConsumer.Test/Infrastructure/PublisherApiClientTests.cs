namespace RabbitMQConsumer.Test.Infrastructure;

public class PublisherApiClientTests
{
    private const string UpdateMessageAsProcessedUrlTemplate = "http://publisher.test/api/Message/{id}/UpdateAsProcessed";
    private const string UpdateMessageAsProcessedUrl = "http://publisher.test/api/Message/7/UpdateAsProcessed";
    private const string AddErrorTextUrl = "http://publisher.test/api/Error/Add";

    private readonly IJsonHttpClient _jsonHttpClient = Substitute.For<IJsonHttpClient>();
    private readonly FakeLogger<PublisherApiClient> _logger = new();
    private readonly PublisherApiClient _repository;

    public PublisherApiClientTests()
    {
        IOptions<UrlOptions> urlOptions = Options.Create(new UrlOptions
        {
            PublisherApi = new() { UpdateMessageAsProcessed = UpdateMessageAsProcessedUrlTemplate, AddErrorText = AddErrorTextUrl }
        });
        _repository = new PublisherApiClient(urlOptions, _jsonHttpClient, _logger);
    }

    [Fact]
    public async Task UpdateMessageAsProcessedAsync_PatchesUrlWithMessageId()
    {
        using CancellationTokenSource cancellationTokenSource = new();
        TrackingHttpResponseMessage response = SetupPatchResponse(HttpStatusCode.OK);

        await _repository.UpdateMessageAsProcessedAsync(7, cancellationTokenSource.Token);

        await _jsonHttpClient.Received(1).SendPatchAsync(UpdateMessageAsProcessedUrl, cancellationTokenSource.Token);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task UpdateMessageAsProcessedAsync_AlreadyProcessed_DoesNotThrow()
    {
        TrackingHttpResponseMessage response = SetupPatchResponse(HttpStatusCode.Conflict, "{\"message\":\"already processed\"}");

        await _repository.UpdateMessageAsProcessedAsync(7);

        Assert.True(response.Disposed);
        Assert.Contains(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Information && record.Message.Contains("already marked as processed"));
    }

    [Fact]
    public async Task UpdateMessageAsProcessedAsync_ErrorResponse_ThrowsHttpRequestExceptionWithStatusAndBody()
    {
        TrackingHttpResponseMessage response = SetupPatchResponse(HttpStatusCode.NotFound, "{\"message\":\"not found\"}");

        HttpRequestException exception = await Assert.ThrowsAsync<HttpRequestException>(() => _repository.UpdateMessageAsProcessedAsync(7));

        Assert.Equal(HttpStatusCode.NotFound, exception.StatusCode);
        Assert.Contains($"PATCH {UpdateMessageAsProcessedUrl}", exception.Message);
        Assert.Contains("404", exception.Message);
        Assert.Contains("not found", exception.Message);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task UpdateMessageAsProcessedAsync_HttpClientThrows_PropagatesException()
    {
        _jsonHttpClient.SendPatchAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("connection refused"));

        await Assert.ThrowsAsync<HttpRequestException>(() => _repository.UpdateMessageAsProcessedAsync(7));
    }

    [Fact]
    public async Task AddErrorTextAsync_PostsMessageIdAndTextToAddErrorTextUrl()
    {
        using CancellationTokenSource cancellationTokenSource = new();
        TrackingHttpResponseMessage response = SetupPostResponse(HttpStatusCode.Created);

        await _repository.AddErrorTextAsync(7, "boom", cancellationTokenSource.Token);

        await _jsonHttpClient.Received(1).SendPostAsync(AddErrorTextUrl, new AddErrorTextDto { MessageId = 7, Text = "boom" }, cancellationTokenSource.Token);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task AddErrorTextAsync_ErrorResponse_ThrowsHttpRequestException()
    {
        TrackingHttpResponseMessage response = SetupPostResponse(HttpStatusCode.InternalServerError);

        HttpRequestException exception = await Assert.ThrowsAsync<HttpRequestException>(() => _repository.AddErrorTextAsync(7, "boom"));

        Assert.Equal(HttpStatusCode.InternalServerError, exception.StatusCode);
        Assert.Contains($"POST {AddErrorTextUrl}", exception.Message);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task AddErrorTextAsync_Conflict_ThrowsHttpRequestException()
    {
        SetupPostResponse(HttpStatusCode.Conflict);

        HttpRequestException exception = await Assert.ThrowsAsync<HttpRequestException>(() => _repository.AddErrorTextAsync(7, "boom"));

        Assert.Equal(HttpStatusCode.Conflict, exception.StatusCode);
    }

    private TrackingHttpResponseMessage SetupPatchResponse(HttpStatusCode statusCode, string body = "")
    {
        TrackingHttpResponseMessage response = new(statusCode) { Content = new StringContent(body) };
        _jsonHttpClient.SendPatchAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(response);
        return response;
    }

    private TrackingHttpResponseMessage SetupPostResponse(HttpStatusCode statusCode, string body = "")
    {
        TrackingHttpResponseMessage response = new(statusCode) { Content = new StringContent(body) };
        _jsonHttpClient.SendPostAsync(Arg.Any<string>(), Arg.Any<object>(), Arg.Any<CancellationToken>())
            .Returns(response);
        return response;
    }

    private sealed class TrackingHttpResponseMessage(HttpStatusCode statusCode) : HttpResponseMessage(statusCode)
    {
        public bool Disposed { get; private set; }

        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            base.Dispose(disposing);
        }
    }
}
