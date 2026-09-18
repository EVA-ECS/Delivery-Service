namespace Delivery_Service.Routing;

public interface IRedisGatewayStore
{
    Task<string?> GetStringAsync(string key, CancellationToken cancellationToken);
    Task<long> PublishAsync(string channel, string payload, CancellationToken cancellationToken);
}
