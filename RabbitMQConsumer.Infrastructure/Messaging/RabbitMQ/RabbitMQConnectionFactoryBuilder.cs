namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

/// <summary>
/// Builds the RabbitMQ <see cref="ConnectionFactory"/> from <see cref="RabbitMQOptions"/>.
/// </summary>
public static class RabbitMQConnectionFactoryBuilder
{
    /// <summary>
    /// Builds the RabbitMQ connection factory for the current instance, using the machine name (the container id when running in Docker) as instance identifier.
    /// </summary>
    /// <param name="rabbitMQOptions">Broker address, credentials and connection settings.</param>
    /// <returns>A configured <see cref="ConnectionFactory"/>.</returns>
    public static ConnectionFactory Build(RabbitMQOptions rabbitMQOptions)
        => Build(rabbitMQOptions, Environment.MachineName);

    /// <summary>
    /// Builds the RabbitMQ connection factory from the options, with automatic connection and topology recovery enabled.
    /// </summary>
    /// <param name="rabbitMQOptions">Broker address, credentials and connection settings.</param>
    /// <param name="instanceId">Identifier of this instance, appended to <see cref="RabbitMQOptions.Name"/> so each instance has its own connection name.</param>
    /// <returns>A configured <see cref="ConnectionFactory"/>.</returns>
    public static ConnectionFactory Build(RabbitMQOptions rabbitMQOptions, string instanceId)
        => new()
        {
            ClientProvidedName = BuildClientProvidedName(rabbitMQOptions.Name, instanceId),
            HostName = rabbitMQOptions.Host,
            Port = rabbitMQOptions.Port,
            VirtualHost = rabbitMQOptions.VirtualHost,
            UserName = rabbitMQOptions.User,
            Password = rabbitMQOptions.Pwd,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(rabbitMQOptions.NetworkRecoveryIntervalSeconds),
            RequestedHeartbeat = TimeSpan.FromSeconds(rabbitMQOptions.HeartbeatSeconds)
        };

    /// <summary>
    /// Builds the connection name shown in RabbitMQ for this instance, for example <c>rabbitmq-consumer-3f9c2a1b7d4e</c>.
    /// </summary>
    /// <param name="name">Base name shared by all the instances.</param>
    /// <param name="instanceId">Identifier of this instance.</param>
    /// <returns>The base name followed by the instance identifier.</returns>
    public static string BuildClientProvidedName(string name, string instanceId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);

        return $"{name}-{instanceId}";
    }
}
