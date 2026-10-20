namespace RabbitMQConsumer.Infrastructure.Messaging.RabbitMQ;

public static class RabbitMQConnectionFactoryBuilder
{
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
