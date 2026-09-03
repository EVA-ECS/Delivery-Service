using Chat.Contracts.Events;

namespace Delivery_Service.Processing;

public interface IDeliveryMessageProcessor
{
    Task<DeliveryProcessingResult> ProcessAsync(
        ChatMessageEvent message,
        CancellationToken cancellationToken);
}

public enum DeliveryProcessingResult
{
    Processed,
    Invalid
}
