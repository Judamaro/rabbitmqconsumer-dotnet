namespace RabbitMQConsumer.Test.Infrastructure;

public class PublisherApiClientTests
{
    private const string UpdateMessageAsProcessedUrl = "http://publisher.test/update-message-as-processed";
    private const string AddErrorTextUrl = "http://publisher.test/add-error-text";

    private readonly IJsonHttpClient _jsonHttpClient = Substitute.For<IJsonHttpClient>();
    private readonly PublisherApiClient _repository;

    public PublisherApiClientTests()
    {
        IOptions<UrlOptions> urlOptions = Options.Create(new UrlOptions
        {
            PublisherApi = new() { UpdateMessageAsProcessed = UpdateMessageAsProcessedUrl, AddErrorText = AddErrorTextUrl }
        });
        _repository = new PublisherApiClient(urlOptions, _jsonHttpClient);
    }

    [Fact]
    public async Task UpdateMessageAsProcessedAsync_PostsMessageIdToUpdateMessageAsProcessedUrl()
    {
        using CancellationTokenSource cancellationTokenSource = new();
        TrackingHttpResponseMessage response = SetupResponse(HttpStatusCode.OK);

        await _repository.UpdateMessageAsProcessedAsync(7, cancellationTokenSource.Token);

        await _jsonHttpClient.Received(1).SendPostAsync(UpdateMessageAsProcessedUrl, new UpdateMessageAsProcessedDto { Id = 7 }, cancellationTokenSource.Token);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task AddErrorTextAsync_PostsErrorToAddErrorTextUrl()
    {
        using CancellationTokenSource cancellationTokenSource = new();
        TrackingHttpResponseMessage response = SetupResponse(HttpStatusCode.OK);

        await _repository.AddErrorTextAsync(7, "boom", cancellationTokenSource.Token);

        await _jsonHttpClient.Received(1).SendPostAsync(AddErrorTextUrl, new AddErrorTextDto { Id = 7, Text = "boom" }, cancellationTokenSource.Token);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task UpdateMessageAsProcessedAsync_ErrorResponse_ThrowsHttpRequestExceptionWithStatusAndBody()
    {
        TrackingHttpResponseMessage response = SetupResponse(HttpStatusCode.BadRequest, "{\"message\":\"invalid id\"}");

        HttpRequestException exception = await Assert.ThrowsAsync<HttpRequestException>(() => _repository.UpdateMessageAsProcessedAsync(7));

        Assert.Equal(HttpStatusCode.BadRequest, exception.StatusCode);
        Assert.Contains(UpdateMessageAsProcessedUrl, exception.Message);
        Assert.Contains("400", exception.Message);
        Assert.Contains("invalid id", exception.Message);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task AddErrorTextAsync_ErrorResponse_ThrowsHttpRequestException()
    {
        TrackingHttpResponseMessage response = SetupResponse(HttpStatusCode.InternalServerError);

        HttpRequestException exception = await Assert.ThrowsAsync<HttpRequestException>(() => _repository.AddErrorTextAsync(7, "boom"));

        Assert.Equal(HttpStatusCode.InternalServerError, exception.StatusCode);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task UpdateMessageAsProcessedAsync_HttpClientThrows_PropagatesException()
    {
        _jsonHttpClient.SendPostAsync(Arg.Any<string>(), Arg.Any<object>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("connection refused"));

        await Assert.ThrowsAsync<HttpRequestException>(() => _repository.UpdateMessageAsProcessedAsync(7));
    }

    private TrackingHttpResponseMessage SetupResponse(HttpStatusCode statusCode, string body = "")
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
