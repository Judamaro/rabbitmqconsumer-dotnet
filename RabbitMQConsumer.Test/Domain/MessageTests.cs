namespace RabbitMQConsumer.Test.Domain;

public class MessageTests
{
    [Fact]
    public void TryCreate_ValidValues_CreatesMessage()
    {
        bool created = Message.TryCreate(42, "hello", out Message? message, out string? error);

        Assert.True(created);
        Assert.NotNull(message);
        Assert.Equal(42, message.Id);
        Assert.Equal("hello", message.Payload);
        Assert.Null(error);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void TryCreate_IdNotPositive_Fails(int id)
    {
        bool created = Message.TryCreate(id, "hello", out Message? message, out string? error);

        Assert.False(created);
        Assert.Null(message);
        Assert.Contains("Id", error);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void TryCreate_EmptyPayload_Fails(string? payload)
    {
        bool created = Message.TryCreate(42, payload, out Message? message, out string? error);

        Assert.False(created);
        Assert.Null(message);
        Assert.Contains("Payload", error);
    }
}
