using Delivery_Service;
using Delivery_Service.Configuration;
using Delivery_Service.Messaging;
using Delivery_Service.Processing;
using Delivery_Service.Routing;
using MassTransit;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddOptions<DeliveryOptions>()
    .Bind(builder.Configuration.GetSection(DeliveryOptions.SectionName))
    .Validate(options => options.WorkerCount is > 0 and <= 256,
        "Delivery:WorkerCount must be between 1 and 256.")
    .ValidateOnStart();

builder.Services.AddOptions<RabbitMqOptions>()
    .Bind(builder.Configuration.GetSection(RabbitMqOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.Host),
        "RabbitMQ:Host is required.")
    .Validate(options => options.Port is > 0 and <= 65535,
        "RabbitMQ:Port must be a valid TCP port.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.QueueName),
        "RabbitMQ:QueueName is required.")
    .ValidateOnStart();

builder.Services.AddOptions<RedisOptions>()
    .Bind(builder.Configuration.GetSection(RedisOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConnectionString),
        "Redis:ConnectionString is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.PresenceKeyPrefix),
        "Redis:PresenceKeyPrefix is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.GatewayMappingKeyPrefix),
        "Redis:GatewayMappingKeyPrefix is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.DeliveryChannelPrefix),
        "Redis:DeliveryChannelPrefix is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.SingleGatewayDeliveryChannel),
        "Redis:SingleGatewayDeliveryChannel is required.")
    .ValidateOnStart();

builder.Services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
{
    var options = serviceProvider.GetRequiredService<IOptions<RedisOptions>>().Value;
    var configuration = ConfigurationOptions.Parse(options.ConnectionString);
    configuration.AbortOnConnectFail = false;
    configuration.ClientName = "delivery-service";
    return ConnectionMultiplexer.Connect(configuration);
});

builder.Services.AddSingleton<IRedisGatewayStore, RedisGatewayStore>();
builder.Services.AddSingleton<IDeliveryRouter, RedisDeliveryRouter>();
builder.Services.AddSingleton<IDeliveryMessageProcessor, DeliveryMessageProcessor>();
builder.Services.AddSingleton(serviceProvider =>
{
    var deliveryOptions = serviceProvider
        .GetRequiredService<IOptions<DeliveryOptions>>()
        .Value;

    return new DeliveryWorkerPool(
        deliveryOptions.WorkerCount,
        serviceProvider.GetRequiredService<IDeliveryMessageProcessor>(),
        serviceProvider.GetRequiredService<ILoggerFactory>());
});

builder.Services.AddMassTransit(config =>
{
    config.AddConsumer<QueueReceiver>();

    config.UsingRabbitMq((context, rabbit) =>
    {
        var rabbitOptions = context.GetRequiredService<IOptions<RabbitMqOptions>>().Value;
        var deliveryOptions = context.GetRequiredService<IOptions<DeliveryOptions>>().Value;

        rabbit.Host(
            rabbitOptions.Host,
            checked((ushort)rabbitOptions.Port),
            rabbitOptions.VirtualHost,
            host =>
            {
                host.Username(rabbitOptions.Username);
                host.Password(rabbitOptions.Password);
            });

        rabbit.ReceiveEndpoint(rabbitOptions.QueueName, endpoint =>
        {
            // Storage sends directly to this queue after the database write.
            // Do not bind delivery_queue to the original Gateway exchange.
            endpoint.Durable = true;
            endpoint.AutoDelete = false;
            endpoint.ConfigureConsumeTopology = false;
            endpoint.PrefetchCount = checked((ushort)deliveryOptions.WorkerCount);
            endpoint.UseMessageRetry(retry =>
                retry.Interval(3, TimeSpan.FromSeconds(5)));
            endpoint.ConfigureConsumer<QueueReceiver>(
                context,
                consumer => consumer.UseConcurrentMessageLimit(
                    deliveryOptions.WorkerCount));
        });
    });
});

await builder.Build().RunAsync();
