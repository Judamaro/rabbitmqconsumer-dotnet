namespace RabbitMQConsumer.Test.Infraestructura;

public class PublisherApiRepositoryTests
{
    private const string MarkAsProcessedUrl = "http://publisher.test/mark-as-processed";
    private const string AddErrorMessageUrl = "http://publisher.test/add-error-message";

    private readonly IHttpClientRepository _httpClientRepository = Substitute.For<IHttpClientRepository>();
    private readonly PublisherApiRepository _repository;

    public PublisherApiRepositoryTests()
    {
        IOptions<UrlOptions> urlOptions = Options.Create(new UrlOptions
        {
            PublisherApi = new() { MarkAsProcessed = MarkAsProcessedUrl, AddErrorMessage = AddErrorMessageUrl }
        });
        _repository = new PublisherApiRepository(urlOptions, _httpClientRepository);
    }

    [Fact]
    public async Task MarkAsProcessedAsync_PostsMessageIdToMarkAsProcessedUrl()
    {
        using CancellationTokenSource cancellationTokenSource = new();
        TrackingHttpResponseMessage response = SetupResponse(HttpStatusCode.OK);

        await _repository.MarkAsProcessedAsync(7, cancellationTokenSource.Token);

        await _httpClientRepository.Received(1).SendPostAsync(MarkAsProcessedUrl, new MessageAsProcessedDto { Id = 7 }, cancellationTokenSource.Token);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task AddErrorMessageAsync_PostsErrorToAddErrorMessageUrl()
    {
        using CancellationTokenSource cancellationTokenSource = new();
        TrackingHttpResponseMessage response = SetupResponse(HttpStatusCode.OK);

        await _repository.AddErrorMessageAsync(7, "boom", cancellationTokenSource.Token);

        await _httpClientRepository.Received(1).SendPostAsync(AddErrorMessageUrl, new ErrorMessageDto { Id = 7, Text = "boom" }, cancellationTokenSource.Token);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task MarkAsProcessedAsync_ErrorResponse_ThrowsHttpRequestExceptionWithStatusAndBody()
    {
        TrackingHttpResponseMessage response = SetupResponse(HttpStatusCode.BadRequest, "{\"message\":\"invalid id\"}");

        HttpRequestException exception = await Assert.ThrowsAsync<HttpRequestException>(() => _repository.MarkAsProcessedAsync(7));

        Assert.Equal(HttpStatusCode.BadRequest, exception.StatusCode);
        Assert.Contains(MarkAsProcessedUrl, exception.Message);
        Assert.Contains("400", exception.Message);
        Assert.Contains("invalid id", exception.Message);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task AddErrorMessageAsync_ErrorResponse_ThrowsHttpRequestException()
    {
        TrackingHttpResponseMessage response = SetupResponse(HttpStatusCode.InternalServerError);

        HttpRequestException exception = await Assert.ThrowsAsync<HttpRequestException>(() => _repository.AddErrorMessageAsync(7, "boom"));

        Assert.Equal(HttpStatusCode.InternalServerError, exception.StatusCode);
        Assert.True(response.Disposed);
    }

    [Fact]
    public async Task MarkAsProcessedAsync_HttpClientThrows_PropagatesException()
    {
        _httpClientRepository.SendPostAsync(Arg.Any<string>(), Arg.Any<object>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("connection refused"));

        await Assert.ThrowsAsync<HttpRequestException>(() => _repository.MarkAsProcessedAsync(7));
    }

    private TrackingHttpResponseMessage SetupResponse(HttpStatusCode statusCode, string body = "")
    {
        TrackingHttpResponseMessage response = new(statusCode) { Content = new StringContent(body) };
        _httpClientRepository.SendPostAsync(Arg.Any<string>(), Arg.Any<object>(), Arg.Any<CancellationToken>())
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
