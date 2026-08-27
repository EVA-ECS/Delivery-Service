using StackExchange.Redis;

namespace Delivery_Service.Routing;

public sealed class RedisGatewayStore : IRedisGatewayStore
{
    private readonly IDatabase _database;
    private readonly ISubscriber _subscriber;

    public RedisGatewayStore(IConnectionMultiplexer connection)
    {
        _database = connection.GetDatabase();
        _subscriber = connection.GetSubscriber();
    }

    public async Task<string?> GetStringAsync(
        string key,
        CancellationToken cancellationToken)
    {
        var value = await _database.StringGetAsync(key).WaitAsync(cancellationToken);
        return value.HasValue ? value.ToString() : null;
    }

    public Task<long> PublishAsync(
        string channel,
        string payload,
        CancellationToken cancellationToken) =>
        _subscriber.PublishAsync(RedisChannel.Literal(channel), payload)
            .WaitAsync(cancellationToken);
}
