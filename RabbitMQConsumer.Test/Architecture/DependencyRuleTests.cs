namespace RabbitMQConsumer.Test.Architecture;

/// <summary>
/// Guards the Clean Architecture dependency rule: dependencies only point inwards (Infrastructure → Application → Domain).
/// </summary>
public class DependencyRuleTests
{
    private static readonly Assembly DomainAssembly = typeof(Message).Assembly;
    private static readonly Assembly ApplicationAssembly = typeof(IProcessMessageUseCase).Assembly;

    [Fact]
    public void Domain_DependsOnlyOnTheFramework()
    {
        string[] references = GetReferencedAssemblyNames(DomainAssembly);

        Assert.All(references, reference => Assert.True(
            reference.StartsWith("System", StringComparison.Ordinal) || reference == "netstandard",
            $"Domain must not reference '{reference}'"));
    }

    [Theory]
    [InlineData("RabbitMQConsumer.Infrastructure")]
    [InlineData("RabbitMQ.Client")]
    [InlineData("Newtonsoft.Json")]
    [InlineData("Microsoft.Extensions.Http")]
    [InlineData("Microsoft.Extensions.Hosting")]
    [InlineData("Polly")]
    public void Application_DoesNotDependOnInfrastructureDetails(string forbiddenPrefix)
    {
        string[] references = GetReferencedAssemblyNames(ApplicationAssembly);

        Assert.DoesNotContain(references, reference => reference.StartsWith(forbiddenPrefix, StringComparison.Ordinal));
    }

    [Fact]
    public void Application_DependsOnDomain()
    {
        Assert.Contains(DomainAssembly.GetName().Name, GetReferencedAssemblyNames(ApplicationAssembly));
    }

    private static string[] GetReferencedAssemblyNames(Assembly assembly)
        => assembly.GetReferencedAssemblies().Select(name => name.Name!).ToArray();
}
