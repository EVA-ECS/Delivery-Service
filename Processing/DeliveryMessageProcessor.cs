using System.Text.Json;
using Delivery_Service.Messaging;
using Delivery_Service.Routing;
using EVA_ECS.Chat.Contracts.Events;

namespace Delivery_Service.Processing;

public sealed class DeliveryMessageProcessor : IDeliveryMessageProcessor
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly IDeliveryRouter _router;
    private readonly ILogger<DeliveryMessageProcessor> _logger;

    public DeliveryMessageProcessor(
        IDeliveryRouter router,
        ILogger<DeliveryMessageProcessor> logger)
    {
        _router = router;
        _logger = logger;
    }

    public async Task<DeliveryProcessingResult> ProcessAsync(
        DeliveryQueueMessage message,
        CancellationToken cancellationToken)
    {
        ChatMessagePublishedEvent? chatMessage;

        try
        {
            chatMessage = Deserialize(message.Body);
        }
        catch (JsonException exception)
        {
            _logger.LogWarning(exception, "Rejected malformed delivery message JSON.");
            return DeliveryProcessingResult.Invalid;
        }

        if (chatMessage is null || !IsValid(chatMessage))
        {
            _logger.LogWarning("Rejected delivery message with missing contract fields.");
            return DeliveryProcessingResult.Invalid;
        }

        if (!message.RoutingKey.StartsWith("msg.private.", StringComparison.Ordinal))
        {
            _logger.LogWarning(
                "Rejected unsupported routing key {RoutingKey}. Group delivery needs recipient envelopes.",
                message.RoutingKey);
            return DeliveryProcessingResult.Invalid;
        }

        var expectedRoutingKey = $"msg.private.{chatMessage.TargetId}";
        if (!string.Equals(message.RoutingKey, expectedRoutingKey, StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning(
                "Rejected message {MessageId} because routing key and target differ.",
                chatMessage.MessageId);
            return DeliveryProcessingResult.Invalid;
        }

        var result = await _router.RouteAsync(chatMessage, cancellationToken);

        if (result == DeliveryRouteResult.Offline)
        {
            _logger.LogInformation(
                "Recipient {TargetId} is offline; message {MessageId} remains available through storage sync.",
                chatMessage.TargetId,
                chatMessage.MessageId);
        }

        return DeliveryProcessingResult.Processed;
    }

    private static ChatMessagePublishedEvent? Deserialize(ReadOnlyMemory<byte> body)
    {
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;

        // MassTransit wraps published messages in a top-level "message" property.
        var eventElement = root.TryGetProperty("message", out var wrappedMessage)
            ? wrappedMessage
            : root;

        return eventElement.Deserialize<ChatMessagePublishedEvent>(JsonOptions);
    }

    private static bool IsValid(ChatMessagePublishedEvent message) =>
        message.MessageId != Guid.Empty &&
        message.SenderId != Guid.Empty &&
        message.TargetId != Guid.Empty &&
        message.Timestamp > 0 &&
        message.Payload is not null &&
        !string.IsNullOrWhiteSpace(message.Payload.EncryptedKey) &&
        !string.IsNullOrWhiteSpace(message.Payload.Iv) &&
        !string.IsNullOrWhiteSpace(message.Payload.Ciphertext) &&
        !string.IsNullOrWhiteSpace(message.Payload.Signature);
}
