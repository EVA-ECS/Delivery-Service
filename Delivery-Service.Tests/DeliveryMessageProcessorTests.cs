using System.Text;
using System.Text.Json;
using Delivery_Service.Messaging;
using Delivery_Service.Processing;
using Delivery_Service.Routing;
using EVA_ECS.Chat.Contracts.Events;
using Microsoft.Extensions.Logging.Abstractions;

namespace Delivery_Service.Tests;

public sealed class DeliveryMessageProcessorTests
{
    [Fact]
    public async Task ValidRabbitMqMessageUsesSharedContract()
    {
        var expected = TestMessageFactory.CreateEvent();
        var router = new RecordingRouter();
        var processor = new DeliveryMessageProcessor(
            router,
            NullLogger<DeliveryMessageProcessor>.Instance);

        var result = await processor.ProcessAsync(
            TestMessageFactory.CreateQueueMessage(expected),
            CancellationToken.None);

        Assert.Equal(DeliveryProcessingResult.Processed, result);
        Assert.NotNull(router.Message);
        Assert.Equal(expected.MessageId, router.Message!.MessageId);
        Assert.Equal(expected.SenderId, router.Message.SenderId);
        Assert.Equal(expected.TargetId, router.Message.TargetId);
        Assert.Equal(expected.Payload, router.Message.Payload);
        Assert.Equal(expected.Timestamp, router.Message.Timestamp);
    }

    [Fact]
    public async Task MassTransitEnvelopeIsAccepted()
    {
        var expected = TestMessageFactory.CreateEvent();
        var router = new RecordingRouter();
        var processor = new DeliveryMessageProcessor(
            router,
            NullLogger<DeliveryMessageProcessor>.Instance);
        var body = JsonSerializer.SerializeToUtf8Bytes(
            new { message = expected },
            new JsonSerializerOptions(JsonSerializerDefaults.Web));
        var queueMessage = new DeliveryQueueMessage(
            body,
            $"msg.private.{expected.TargetId}",
            _ => ValueTask.CompletedTask,
            (_, _) => ValueTask.CompletedTask);

        var result = await processor.ProcessAsync(queueMessage, CancellationToken.None);

        Assert.Equal(DeliveryProcessingResult.Processed, result);
        Assert.Equal(expected.MessageId, router.Message?.MessageId);
    }

    [Fact]
    public async Task MalformedMessageIsInvalidAndNotRouted()
    {
        var router = new RecordingRouter();
        var processor = new DeliveryMessageProcessor(
            router,
            NullLogger<DeliveryMessageProcessor>.Instance);
        var queueMessage = new DeliveryQueueMessage(
            Encoding.UTF8.GetBytes("not-json"),
            "msg.private.invalid",
            _ => ValueTask.CompletedTask,
            (_, _) => ValueTask.CompletedTask);

        var result = await processor.ProcessAsync(queueMessage, CancellationToken.None);

        Assert.Equal(DeliveryProcessingResult.Invalid, result);
        Assert.Null(router.Message);
    }

    [Fact]
    public async Task NullEncryptedPayloadIsInvalidAndNotRequeuedForever()
    {
        var message = TestMessageFactory.CreateEvent() with { Payload = null! };
        var router = new RecordingRouter();
        var processor = new DeliveryMessageProcessor(
            router,
            NullLogger<DeliveryMessageProcessor>.Instance);

        var result = await processor.ProcessAsync(
            TestMessageFactory.CreateQueueMessage(message),
            CancellationToken.None);

        Assert.Equal(DeliveryProcessingResult.Invalid, result);
        Assert.Null(router.Message);
    }

    [Fact]
    public async Task GroupRoutingIsRejectedUntilRecipientsAreInContract()
    {
        var message = TestMessageFactory.CreateEvent();
        var router = new RecordingRouter();
        var processor = new DeliveryMessageProcessor(
            router,
            NullLogger<DeliveryMessageProcessor>.Instance);

        var result = await processor.ProcessAsync(
            TestMessageFactory.CreateQueueMessage(
                message,
                routingKey: $"msg.group.{message.TargetId}"),
            CancellationToken.None);

        Assert.Equal(DeliveryProcessingResult.Invalid, result);
        Assert.Null(router.Message);
    }

    private sealed class RecordingRouter : IDeliveryRouter
    {
        public ChatMessagePublishedEvent? Message { get; private set; }

        public Task<DeliveryRouteResult> RouteAsync(
            ChatMessagePublishedEvent message,
            CancellationToken cancellationToken)
        {
            Message = message;
            return Task.FromResult(DeliveryRouteResult.Delivered);
        }
    }
}
