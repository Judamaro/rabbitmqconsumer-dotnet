namespace RabbitMQConsumer.Test.Infrastructure;

public class JsonHttpClientTests
{
    private const string Url = "http://publisher.test/api/messages";

    private readonly StubHttpMessageHandler _handler = new();
    private readonly FakeLogger<JsonHttpClient> _logger = new();
    private readonly JsonHttpClient _repository;

    public JsonHttpClientTests()
    {
        _repository = new JsonHttpClient(new HttpClient(_handler), _logger);
    }

    [Fact]
    public async Task SendPostAsync_PostsBodyAsBufferedCamelCaseJson()
    {
        using HttpResponseMessage response = await _repository.SendPostAsync(Url, new UpdateMessageAsProcessedDto { Id = 7 });

        Assert.Equal(HttpMethod.Post, _handler.Request!.Method);
        Assert.Equal(Url, _handler.Request.RequestUri!.ToString());
        Assert.Equal("{\"id\":7}", _handler.RequestBody);
        Assert.Equal("application/json", _handler.RequestContentType);
        Assert.Equal(Encoding.UTF8.GetByteCount(_handler.RequestBody!), _handler.RequestContentLength);
        Assert.NotEqual(true, _handler.Request.Headers.TransferEncodingChunked);
    }

    [Fact]
    public async Task SendPostAsync_SerializesRuntimeTypeOfBody()
    {
        object body = new AddErrorTextDto { Id = 3, Text = "boom" };

        using HttpResponseMessage response = await _repository.SendPostAsync(Url, body);

        Assert.Equal("{\"id\":3,\"text\":\"boom\"}", _handler.RequestBody);
    }

    [Fact]
    public async Task SendPostAsync_SuccessResponse_ReturnsResponseWithoutWarning()
    {
        _handler.ResponseStatusCode = HttpStatusCode.OK;

        using HttpResponseMessage response = await _repository.SendPostAsync(Url, new { Id = 1 });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.DoesNotContain(_logger.Collector.GetSnapshot(), record => record.Level >= LogLevel.Warning);
    }

    [Fact]
    public async Task SendPostAsync_ErrorResponse_ReturnsResponseAndLogsWarning()
    {
        _handler.ResponseStatusCode = HttpStatusCode.InternalServerError;

        using HttpResponseMessage response = await _repository.SendPostAsync(Url, new { Id = 1 });

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        FakeLogRecord warning = Assert.Single(_logger.Collector.GetSnapshot(), record => record.Level == LogLevel.Warning);
        Assert.Contains(Url, warning.Message);
        Assert.Contains("500", warning.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task SendPostAsync_InvalidUrl_ThrowsArgumentException(string? url)
    {
        await Assert.ThrowsAnyAsync<ArgumentException>(() => _repository.SendPostAsync(url!, new { Id = 1 }));
        Assert.Null(_handler.Request);
    }

    [Fact]
    public async Task SendPostAsync_NullBody_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _repository.SendPostAsync(Url, null!));
        Assert.Null(_handler.Request);
    }

    [Fact]
    public async Task SendPostAsync_CancelledToken_ThrowsOperationCanceledException()
    {
        using CancellationTokenSource cancellationTokenSource = new();
        await cancellationTokenSource.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _repository.SendPostAsync(Url, new { Id = 1 }, cancellationTokenSource.Token));
    }

    private sealed class StubHttpMessageHandler : HttpMessageHandler
    {
        public HttpStatusCode ResponseStatusCode { get; set; } = HttpStatusCode.OK;
        public HttpRequestMessage? Request { get; private set; }
        public string? RequestBody { get; private set; }
        public string? RequestContentType { get; private set; }
        public long? RequestContentLength { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Request = request;
            // The content is disposed by the repository once the call returns, so capture it here
            RequestContentType = request.Content?.Headers.ContentType?.MediaType;
            RequestContentLength = request.Content?.Headers.ContentLength;
            RequestBody = request.Content is null ? null : await request.Content.ReadAsStringAsync(cancellationToken);
            return new HttpResponseMessage(ResponseStatusCode) { RequestMessage = request };
        }
    }
}
