using Chat.Contracts.Events;
using Delivery_Service.Processing;
using MassTransit;

namespace Delivery_Service.Messaging;

/// <summary>
/// Receives the delivery message that Storage publishes after persistence.
/// MassTransit owns the RabbitMQ acknowledgement and retry lifecycle.
/// </summary>
public sealed class QueueReceiver : IConsumer<ChatMessageEvent>
{
    private readonly DeliveryWorkerPool _workerPool;

    public QueueReceiver(DeliveryWorkerPool workerPool)
    {
        _workerPool = workerPool;
    }

    public Task Consume(ConsumeContext<ChatMessageEvent> context) =>
        _workerPool.ProcessAsync(context.Message, context.CancellationToken);
}
