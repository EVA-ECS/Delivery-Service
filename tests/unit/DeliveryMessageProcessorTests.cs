using Chat.Contracts.Events;
using Delivery_Service.Processing;
using Delivery_Service.Routing;
using Microsoft.Extensions.Logging.Abstractions;

namespace Delivery_Service.Tests;

public sealed class DeliveryMessageProcessorTests
{
    [Fact]
    public async Task ValidMessageIsRoutedToTheRecipient()
    {
        var expected = TestMessageFactory.CreateEvent();
        var router = new RecordingRouter(DeliveryRouteResult.Delivered);
        var processor = new DeliveryMessageProcessor(
            router,
            NullLogger<DeliveryMessageProcessor>.Instance);

        var result = await processor.ProcessAsync(expected, CancellationToken.None);

        Assert.Equal(DeliveryProcessingResult.Processed, result);
        Assert.Equal(expected, router.Message);
    }

    [Fact]
    public async Task OfflineRecipientIsAValidCompletedDelivery()
    {
        var router = new RecordingRouter(DeliveryRouteResult.Offline);
        var processor = new DeliveryMessageProcessor(
            router,
            NullLogger<DeliveryMessageProcessor>.Instance);

        var result = await processor.ProcessAsync(
            TestMessageFactory.CreateEvent(),
            CancellationToken.None);

        Assert.Equal(DeliveryProcessingResult.Processed, result);
        Assert.NotNull(router.Message);
    }

    [Fact]
    public async Task InvalidMessageIsIgnoredWithoutRouting()
    {
        var router = new RecordingRouter(DeliveryRouteResult.Delivered);
        var processor = new DeliveryMessageProcessor(
            router,
            NullLogger<DeliveryMessageProcessor>.Instance);

        var result = await processor.ProcessAsync(
            new ChatMessageEvent("not-a-guid", "not-a-guid", "not-a-guid", "text", DateTime.UtcNow),
            CancellationToken.None);

        Assert.Equal(DeliveryProcessingResult.Invalid, result);
        Assert.Null(router.Message);
    }

    [Fact]
    public async Task RedisFailureIsPropagatedToMassTransit()
    {
        var processor = new DeliveryMessageProcessor(
            new ThrowingRouter(),
            NullLogger<DeliveryMessageProcessor>.Instance);

        await Assert.ThrowsAsync<IOException>(() =>
            processor.ProcessAsync(
                TestMessageFactory.CreateEvent(),
                CancellationToken.None));
    }

    private sealed class RecordingRouter : IDeliveryRouter
    {
        private readonly DeliveryRouteResult _result;

        public RecordingRouter(DeliveryRouteResult result) => _result = result;

        public ChatMessageEvent? Message { get; private set; }

        public Task<DeliveryRouteResult> RouteAsync(
            ChatMessageEvent message,
            CancellationToken cancellationToken)
        {
            Message = message;
            return Task.FromResult(_result);
        }
    }

    private sealed class ThrowingRouter : IDeliveryRouter
    {
        public Task<DeliveryRouteResult> RouteAsync(
            ChatMessageEvent message,
            CancellationToken cancellationToken) =>
            Task.FromException<DeliveryRouteResult>(
                new IOException("redis unavailable"));
    }
}
