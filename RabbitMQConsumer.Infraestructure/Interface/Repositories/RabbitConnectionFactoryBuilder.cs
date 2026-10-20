namespace RabbitMQConsumer.Infraestructure.Interface.Repositories;

public static class RabbitConnectionFactoryBuilder
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
