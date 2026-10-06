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

        string channel;
        if (!string.IsNullOrWhiteSpace(gatewayId))
        {
            // Spezifisches Gateway aus der Mapping-Tabelle
            channel = $"{_options.DeliveryChannelPrefix}{gatewayId}";
        }
        else
        {
            // FALLBACK: Wir werfen es in den allgemeinen Channel, 
            // auf den ALLE Gateways lauschen!
            channel = _options.SingleGatewayDeliveryChannel;
            
            // Optional: Nur loggen, aber NICHT mehr abbrechen!
            var presence = await _store.GetStringAsync($"{_options.PresenceKeyPrefix}{userId}", cancellationToken);
            if (string.IsNullOrWhiteSpace(presence))
            {
                _logger.LogWarning("Nutzer {UserId} scheint laut Redis offline zu sein. Sende trotzdem als Broadcast an alle Gateways...", userId);
            }
        }

        var subscriberCount = await _store.PublishAsync(
            channel,
            System.Text.Json.JsonSerializer.Serialize(message, JsonOptions),
            cancellationToken);

        if (subscriberCount == 0)
        {
            _logger.LogWarning(
                "Kein einziges Gateway lauscht auf Redis-Channel! (Message-ID: {MessageId})",
                message.MessageId);
            return DeliveryRouteResult.Offline;
        }

        return DeliveryRouteResult.Delivered;
    }
}
