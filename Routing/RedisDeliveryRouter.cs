using System.Text.Json;
using Delivery_Service.Configuration;
using EVA_ECS.Chat.Contracts.Events;
using Microsoft.Extensions.Options;

namespace Delivery_Service.Routing;

public sealed class RedisDeliveryRouter : IDeliveryRouter
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly IRedisGatewayStore _store;
    private readonly RedisOptions _options;
    private readonly ILogger<RedisDeliveryRouter> _logger;

    public RedisDeliveryRouter(
        IRedisGatewayStore store,
        IOptions<RedisOptions> options,
        ILogger<RedisDeliveryRouter> logger)
    {
        _store = store;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<DeliveryRouteResult> RouteAsync(
        ChatMessagePublishedEvent message,
        CancellationToken cancellationToken)
    {
        var userId = message.TargetId.ToString();
        var presenceTask = _store.GetStringAsync(
            $"{_options.PresenceKeyPrefix}{userId}",
            cancellationToken);
        var gatewayTask = _store.GetStringAsync(
            $"{_options.GatewayMappingKeyPrefix}{userId}",
            cancellationToken);

        await Task.WhenAll(presenceTask, gatewayTask);

        var presence = await presenceTask;
        var gatewayId = await gatewayTask;

        if (string.IsNullOrWhiteSpace(presence) || string.IsNullOrWhiteSpace(gatewayId))
        {
            return DeliveryRouteResult.Offline;
        }

        var channel = $"{_options.DeliveryChannelPrefix}{gatewayId}";
        var subscriberCount = await _store.PublishAsync(
            channel,
            JsonSerializer.Serialize(message, JsonOptions),
            cancellationToken);

        if (subscriberCount == 0)
        {
            _logger.LogWarning(
                "Gateway {GatewayId} has no Redis subscriber; recipient {TargetId} is treated as offline.",
                gatewayId,
                message.TargetId);
            return DeliveryRouteResult.Offline;
        }

        return DeliveryRouteResult.Delivered;
    }
}
