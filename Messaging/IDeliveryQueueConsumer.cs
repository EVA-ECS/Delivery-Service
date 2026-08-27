namespace Delivery_Service.Messaging;

public interface IDeliveryQueueConsumer : IAsyncDisposable
{
    Task Completion { get; }

    Task StartAsync(
        Func<DeliveryQueueMessage, CancellationToken, ValueTask> onMessage,
        ushort prefetchCount,
        CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);
}
