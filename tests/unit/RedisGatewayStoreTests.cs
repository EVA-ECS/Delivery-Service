using Delivery_Service.Routing;
using Moq;
using StackExchange.Redis;

namespace Delivery_Service.Tests;

public sealed class RedisGatewayStoreTests
{
    [Fact]
    public async Task ReadsMissingAndPresentValuesAndPublishesOnLiteralChannel()
    {
        var db = new Mock<IDatabase>();
        db.Setup(x => x.StringGetAsync("missing", CommandFlags.None)).ReturnsAsync(RedisValue.Null);
        db.Setup(x => x.StringGetAsync("present", CommandFlags.None)).ReturnsAsync("gateway-1");
        var sub = new Mock<ISubscriber>();
        sub.Setup(x => x.PublishAsync(RedisChannel.Literal("delivery:1"), "encrypted", CommandFlags.None)).ReturnsAsync(2);
        var redis = new Mock<IConnectionMultiplexer>();
        redis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(db.Object);
        redis.Setup(x => x.GetSubscriber(It.IsAny<object>())).Returns(sub.Object);
        var store = new RedisGatewayStore(redis.Object);
        Assert.Null(await store.GetStringAsync("missing", default));
        Assert.Equal("gateway-1", await store.GetStringAsync("present", default));
        Assert.Equal(2, await store.PublishAsync("delivery:1", "encrypted", default));
        using var cancellation = new CancellationTokenSource();
        db.Setup(x => x.StringGetAsync("slow", CommandFlags.None)).Returns(new TaskCompletionSource<RedisValue>().Task);
        sub.Setup(x => x.PublishAsync(RedisChannel.Literal("slow"), "encrypted", CommandFlags.None)).Returns(new TaskCompletionSource<long>().Task);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.GetStringAsync("slow", cancellation.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.PublishAsync("slow", "encrypted", cancellation.Token));
    }
}
