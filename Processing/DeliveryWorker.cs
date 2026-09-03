using Chat.Contracts.Events;

namespace Delivery_Service.Processing;

public sealed class DeliveryWorker
{
    private readonly IDeliveryMessageProcessor _processor;
    private readonly ILogger<DeliveryWorker> _logger;

    public DeliveryWorker(
        IDeliveryMessageProcessor processor,
        ILogger<DeliveryWorker> logger)
    {
        _processor = processor;
        _logger = logger;
    }

    public async Task ProcessAsync(
        ChatMessageEvent message,
        CancellationToken cancellationToken)
    {
        var result = await _processor.ProcessAsync(message, cancellationToken);

        if (result == DeliveryProcessingResult.Invalid)
        {
            // Invalid messages are intentionally acknowledged by MassTransit
            // after this method returns; retrying malformed input is useless.
            _logger.LogWarning(
                "Ignored invalid delivery message {MessageId}.",
                message.MessageId);
        }
    }
}
