using System.Text.Json;
using Delivery_Service.Configuration;
using Chat.Contracts.Events;
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
        ChatMessageEvent message,
        CancellationToken cancellationToken)
    {
        var userId = message.TargetId;
        var gatewayId = await _store.GetStringAsync(
            $"{_options.GatewayMappingKeyPrefix}{userId}",
            cancellationToken);

        string? channel;
        if (!string.IsNullOrWhiteSpace(gatewayId))
        {
            // Multi-Gateway deployments use the mapping set by Gateway.
            channel = $"{_options.DeliveryChannelPrefix}{gatewayId}";
        }
        else
        {
            // The MVP can run one Gateway without a user-to-gateway map.
            var presence = await _store.GetStringAsync(
                $"{_options.PresenceKeyPrefix}{userId}",
                cancellationToken);

            if (string.IsNullOrWhiteSpace(presence))
            {
                return DeliveryRouteResult.Offline;
            }

            channel = _options.SingleGatewayDeliveryChannel;
        }

        var subscriberCount = await _store.PublishAsync(
            channel,
            System.Text.Json.JsonSerializer.Serialize(message, JsonOptions),
            cancellationToken);

        if (subscriberCount == 0)
        {
            _logger.LogWarning(
                "Gateway {GatewayId} has no Redis subscriber; recipient {TargetId} is treated as offline.",
                gatewayId ?? "single-gateway",
                userId);
            return DeliveryRouteResult.Offline;
        }

        return DeliveryRouteResult.Delivered;
    }
}
