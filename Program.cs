using Delivery_Service;
using Delivery_Service.Configuration;
using Delivery_Service.Messaging;
using Delivery_Service.Processing;
using Delivery_Service.Routing;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddOptions<DeliveryOptions>()
    .Bind(builder.Configuration.GetSection(DeliveryOptions.SectionName))
    .Validate(options => options.WorkerCount is > 0 and <= 256,
        "Delivery:WorkerCount must be between 1 and 256.")
    .Validate(options => options.ConnectionRetryDelaySeconds > 0,
        "Delivery:ConnectionRetryDelaySeconds must be greater than zero.")
    .Validate(options => options.ShutdownTimeoutSeconds > 0,
        "Delivery:ShutdownTimeoutSeconds must be greater than zero.")
    .ValidateOnStart();

builder.Services.AddOptions<RabbitMqOptions>()
    .Bind(builder.Configuration.GetSection(RabbitMqOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.Host),
        "RabbitMQ:Host is required.")
    .Validate(options => options.Port is > 0 and <= 65535,
        "RabbitMQ:Port must be a valid TCP port.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ExchangeName),
        "RabbitMQ:ExchangeName is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.QueueName),
        "RabbitMQ:QueueName is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.RoutingKey),
        "RabbitMQ:RoutingKey is required.")
    .ValidateOnStart();

builder.Services.AddOptions<RedisOptions>()
    .Bind(builder.Configuration.GetSection(RedisOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConnectionString),
        "Redis:ConnectionString is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.GatewayMappingKeyPrefix),
        "Redis:GatewayMappingKeyPrefix is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.DeliveryChannelPrefix),
        "Redis:DeliveryChannelPrefix is required.")
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
builder.Services.AddSingleton<IDeliveryQueueConsumer, RabbitMqDeliveryQueueConsumer>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
