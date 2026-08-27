using System.Text.Json;
using Delivery_Service.Configuration;
using Delivery_Service.Routing;
using EVA_ECS.Chat.Contracts.Events;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Delivery_Service.Tests;

public sealed class RedisDeliveryRouterTests
{
    private static readonly RedisOptions Options = new()
    {
        ConnectionString = "unused",
        PresenceKeyPrefix = "presence:",
        GatewayMappingKeyPrefix = "gateway_for_user:",
        DeliveryChannelPrefix = "gateway:delivery:"
    };

    [Fact]
    public async Task RoutesEncryptedPayloadToMappedGateway()
    {
        var message = TestMessageFactory.CreateEvent();
        var store = new FakeRedisStore();
        store.Values[$"presence:{message.TargetId}"] = "1";
        store.Values[$"gateway_for_user:{message.TargetId}"] = "gateway-b";
        var router = CreateRouter(store);

        var result = await router.RouteAsync(message, CancellationToken.None);

        Assert.Equal(DeliveryRouteResult.Delivered, result);
        Assert.Equal("gateway:delivery:gateway-b", store.PublishedChannel);
        var published = JsonSerializer.Deserialize<ChatMessagePublishedEvent>(
            store.PublishedPayload!,
            new JsonSerializerOptions(JsonSerializerDefaults.Web));
        Assert.Equal(message, published);
    }

    [Fact]
    public async Task MissingGatewayMappingMeansOfflineAndDoesNotPublish()
    {
        var message = TestMessageFactory.CreateEvent();
        var store = new FakeRedisStore();
        store.Values[$"presence:{message.TargetId}"] = "1";
        var router = CreateRouter(store);

        var result = await router.RouteAsync(message, CancellationToken.None);

        Assert.Equal(DeliveryRouteResult.Offline, result);
        Assert.Null(store.PublishedChannel);
    }

    [Fact]
    public async Task RedisFailureIsPropagatedForRabbitMqRequeue()
    {
        var store = new FakeRedisStore { ReadException = new IOException("redis unavailable") };
        var router = CreateRouter(store);

        await Assert.ThrowsAsync<IOException>(() =>
            router.RouteAsync(TestMessageFactory.CreateEvent(), CancellationToken.None));
    }

    private static RedisDeliveryRouter CreateRouter(FakeRedisStore store) =>
        new(
            store,
            Microsoft.Extensions.Options.Options.Create(Options),
            NullLogger<RedisDeliveryRouter>.Instance);

    private sealed class FakeRedisStore : IRedisGatewayStore
    {
        public Dictionary<string, string> Values { get; } = new();
        public Exception? ReadException { get; init; }
        public string? PublishedChannel { get; private set; }
        public string? PublishedPayload { get; private set; }

        public Task<string?> GetStringAsync(string key, CancellationToken cancellationToken)
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
            PublishedChannel = channel;
            PublishedPayload = payload;
            return Task.FromResult(1L);
        }
    }
}
