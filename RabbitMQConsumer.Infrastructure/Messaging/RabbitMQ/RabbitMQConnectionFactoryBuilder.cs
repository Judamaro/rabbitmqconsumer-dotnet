namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

/// <summary>
/// Builds the RabbitMQ <see cref="ConnectionFactory"/> from <see cref="RabbitMQOptions"/>.
/// </summary>
public static class RabbitMQConnectionFactoryBuilder
{
    /// <summary>
    /// Builds the RabbitMQ connection factory from the options, with automatic connection and topology recovery enabled.
    /// </summary>
    /// <param name="rabbitMQOptions">Broker address, credentials and connection settings.</param>
    /// <returns>A configured <see cref="ConnectionFactory"/>.</returns>
    public static ConnectionFactory Build(RabbitMQOptions rabbitMQOptions)
        => new()
        {
            ClientProvidedName = rabbitMQOptions.Name,
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
}
