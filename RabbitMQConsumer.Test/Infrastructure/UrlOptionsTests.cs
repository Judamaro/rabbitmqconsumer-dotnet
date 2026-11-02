using System.ComponentModel.DataAnnotations;

namespace RabbitMQConsumer.Test.Infrastructure;

public class UrlOptionsTests
{
    [Theory]
    [InlineData("http://publisher.test/api/Message/{id}/UpdateAsProcessed", true)]
    [InlineData("http://publisher.test/api/Message/UpdateAsProcessed", false)]
    [InlineData("publisher.test/api/Message/{id}/UpdateAsProcessed", false)]
    public void UpdateMessageAsProcessed_RequiresAbsoluteUrlWithIdPlaceholder(string url, bool isValid)
    {
        UrlOptions.PublisherApiOptions options = new() { UpdateMessageAsProcessed = url, AddErrorText = "http://publisher.test/api/Error/Add" };
        List<ValidationResult> results = [];

        bool valid = Validator.TryValidateObject(options, new ValidationContext(options), results, validateAllProperties: true);

        Assert.Equal(isValid, valid);
    }

    [Fact]
    public void UpdateMessageAsProcessed_WithoutIdPlaceholder_ReportsPlaceholderError()
    {
        UrlOptions.PublisherApiOptions options = new() { UpdateMessageAsProcessed = "http://publisher.test/api/Message/UpdateAsProcessed", AddErrorText = "http://publisher.test/api/Error/Add" };
        List<ValidationResult> results = [];

        Validator.TryValidateObject(options, new ValidationContext(options), results, validateAllProperties: true);

        ValidationResult result = Assert.Single(results);
        Assert.Equal("The UpdateMessageAsProcessed URL must contain the {id} placeholder.", result.ErrorMessage);
    }
}
