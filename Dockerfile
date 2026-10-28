# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore first so the dependency layer is cached while only source files change
COPY RabbitMQConsumer.Domain/RabbitMQConsumer.Domain.csproj RabbitMQConsumer.Domain/
COPY RabbitMQConsumer.Application/RabbitMQConsumer.Application.csproj RabbitMQConsumer.Application/
COPY RabbitMQConsumer.Infrastructure/RabbitMQConsumer.Infrastructure.csproj RabbitMQConsumer.Infrastructure/
COPY RabbitMQConsumer.App/RabbitMQConsumer.App.csproj RabbitMQConsumer.App/
RUN dotnet restore RabbitMQConsumer.App/RabbitMQConsumer.App.csproj

COPY RabbitMQConsumer.Domain/ RabbitMQConsumer.Domain/
COPY RabbitMQConsumer.Application/ RabbitMQConsumer.Application/
COPY RabbitMQConsumer.Infrastructure/ RabbitMQConsumer.Infrastructure/
COPY RabbitMQConsumer.App/ RabbitMQConsumer.App/
RUN dotnet publish RabbitMQConsumer.App/RabbitMQConsumer.App.csproj -c Release -o /app --no-restore -p:UseAppHost=false

# Worker service: the plain .NET runtime is enough, ASP.NET Core is not needed
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app .

# The Serilog file sink writes to /app/logs, so it must be writable by the non-root user
RUN mkdir -p /app/logs && chown "$APP_UID" /app/logs
USER $APP_UID

ENV DOTNET_ENVIRONMENT=Production
ENTRYPOINT ["dotnet", "RabbitMQConsumer.App.dll"]
