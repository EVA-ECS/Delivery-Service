using Delivery_Service.Messaging;

namespace Delivery_Service.Processing;

public interface IDeliveryMessageProcessor
{
    Task<DeliveryProcessingResult> ProcessAsync(
        DeliveryQueueMessage message,
        CancellationToken cancellationToken);
}

public enum DeliveryProcessingResult
{
    Processed,
    Invalid
}
