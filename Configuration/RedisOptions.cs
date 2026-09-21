namespace Delivery_Service.Configuration;

public sealed class RedisOptions
{
    public const string SectionName = "Redis";

    public string ConnectionString { get; set; } = "localhost:6379";
    public string PresenceKeyPrefix { get; set; } = "eva-chat:online:";
    public string GatewayMappingKeyPrefix { get; set; } = "gateway_for_user:";
    public string DeliveryChannelPrefix { get; set; } = "gateway:delivery:";
    public string SingleGatewayDeliveryChannel { get; set; } = "gateway:delivery";
}
