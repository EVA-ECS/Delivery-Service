using System.Text.Json;
using Delivery_Service.Routing;
using Chat.Contracts.Events;

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
        ChatMessageEvent message,
        CancellationToken cancellationToken)
    {
        if (!IsValid(message))
        {
            _logger.LogWarning("Rejected delivery message with missing contract fields.");
            return DeliveryProcessingResult.Invalid;
        }

        var result = await _router.RouteAsync(message, cancellationToken);

        if (result == DeliveryRouteResult.Offline)
        {
            _logger.LogInformation(
                "Recipient {TargetId} is offline; message {MessageId} remains available through storage sync.",
                message.TargetId,
                message.MessageId);
        }

        return DeliveryProcessingResult.Processed;
    }

    private static bool IsValid(ChatMessageEvent message) =>
        Guid.TryParse(message.MessageId, out var messageId) &&
        messageId != Guid.Empty &&
        Guid.TryParse(message.SenderId, out var senderId) &&
        senderId != Guid.Empty &&
        Guid.TryParse(message.TargetId, out var targetId) &&
        targetId != Guid.Empty &&
        message.Timestamp != default &&
        !string.IsNullOrWhiteSpace(message.Ciphertext);
}
