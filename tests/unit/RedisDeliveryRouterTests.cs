using System.Text.Json;
using Chat.Contracts.Events;
using Delivery_Service.Configuration;
using Delivery_Service.Routing;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Delivery_Service.Tests;

public sealed class RedisDeliveryRouterTests
{
    private static readonly RedisOptions DefaultOptions = new()
    {
        ConnectionString = "unused",
        PresenceKeyPrefix = "presence:",
        GatewayMappingKeyPrefix = "gateway_for_user:",
        DeliveryChannelPrefix = "gateway:delivery:",
        SingleGatewayDeliveryChannel = "gateway:delivery"
    };

    [Fact]
    public async Task UsesMappedGatewayWhenMappingExists()
    {
        var message = TestMessageFactory.CreateEvent();
        var store = new FakeRedisStore();
        store.Values[$"gateway_for_user:{message.TargetId}"] = "gateway-b";
        var router = CreateRouter(store);

        var result = await router.RouteAsync(message, CancellationToken.None);

        Assert.Equal(DeliveryRouteResult.Delivered, result);
        Assert.Equal("gateway:delivery:gateway-b", store.PublishedChannel);
        Assert.Equal(
            message,
            JsonSerializer.Deserialize<ChatMessageEvent>(
                store.PublishedPayload!,
                new JsonSerializerOptions(JsonSerializerDefaults.Web)));
    }

    [Fact]
    public async Task UsesSingleGatewayChannelWhenPresenceExists()
    {
        var message = TestMessageFactory.CreateEvent();
        var store = new FakeRedisStore();
        store.Values[$"presence:{message.TargetId}"] = "1";
        var router = CreateRouter(store);

        var result = await router.RouteAsync(message, CancellationToken.None);

        Assert.Equal(DeliveryRouteResult.Delivered, result);
        Assert.Equal("gateway:delivery", store.PublishedChannel);
    }

    [Fact]
    public async Task MissingPresenceAndMappingMeansOffline()
    {
        var store = new FakeRedisStore();
        var router = CreateRouter(store);

        var result = await router.RouteAsync(
            TestMessageFactory.CreateEvent(),
            CancellationToken.None);

        Assert.Equal(DeliveryRouteResult.Offline, result);
        Assert.Null(store.PublishedChannel);
    }

    [Fact]
    public async Task RedisReadFailureIsPropagated()
    {
        var router = CreateRouter(new FakeRedisStore
        {
            ReadException = new IOException("redis unavailable")
        });

        await Assert.ThrowsAsync<IOException>(() =>
            router.RouteAsync(TestMessageFactory.CreateEvent(), CancellationToken.None));
    }

    [Fact]
    public async Task RedisPublishFailureIsPropagated()
    {
        var message = TestMessageFactory.CreateEvent();
        var store = new FakeRedisStore
        {
            PublishException = new IOException("redis unavailable")
        };
        store.Values[$"presence:{message.TargetId}"] = "1";
        var router = CreateRouter(store);

        await Assert.ThrowsAsync<IOException>(() =>
            router.RouteAsync(message, CancellationToken.None));
    }

    [Fact]
    public async Task NoSubscriberMeansOffline()
    {
        var message = TestMessageFactory.CreateEvent();
        var store = new FakeRedisStore
        {
            PublishedSubscriberCount = 0
        };
        store.Values[$"presence:{message.TargetId}"] = "1";
        var router = CreateRouter(store);

        var result = await router.RouteAsync(message, CancellationToken.None);

        Assert.Equal(DeliveryRouteResult.Offline, result);
    }

    private static RedisDeliveryRouter CreateRouter(FakeRedisStore store) =>
        new(
            store,
            Microsoft.Extensions.Options.Options.Create(DefaultOptions),
            NullLogger<RedisDeliveryRouter>.Instance);

    private sealed class FakeRedisStore : IRedisGatewayStore
    {
        public Dictionary<string, string> Values { get; } = new();
        public Exception? ReadException { get; init; }
        public Exception? PublishException { get; init; }
        public long PublishedSubscriberCount { get; init; } = 1;
        public string? PublishedChannel { get; private set; }
        public string? PublishedPayload { get; private set; }

        public Task<string?> GetStringAsync(
            string key,
            CancellationToken cancellationToken)
        {
            if (ReadException is not null)
            {
                return Task.FromException<string?>(ReadException);
            }

            return Task.FromResult(Values.GetValueOrDefault(key));
        }

        public Task<long> PublishAsync(
            string channel,
            string payload,
            CancellationToken cancellationToken)
        {
            if (PublishException is not null)
            {
                return Task.FromException<long>(PublishException);
            }

            PublishedChannel = channel;
            PublishedPayload = payload;
            return Task.FromResult(PublishedSubscriberCount);
        }
    }
}
