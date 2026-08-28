namespace Delivery_Service.Messaging;

public sealed class DeliveryQueueMessage
{
    private readonly Func<CancellationToken, ValueTask> _acknowledge;
    private readonly Func<bool, CancellationToken, ValueTask> _negativeAcknowledge;

    public DeliveryQueueMessage(
        ReadOnlyMemory<byte> body,
        string routingKey,
        Func<CancellationToken, ValueTask> acknowledge,
        Func<bool, CancellationToken, ValueTask> negativeAcknowledge)
    {
        Body = body;
        RoutingKey = routingKey;
        _acknowledge = acknowledge;
        _negativeAcknowledge = negativeAcknowledge;
    }

    public ReadOnlyMemory<byte> Body { get; }
    public string RoutingKey { get; }

    public ValueTask AcknowledgeAsync(CancellationToken cancellationToken = default) =>
        _acknowledge(cancellationToken);

    public ValueTask NegativeAcknowledgeAsync(
        bool requeue,
        CancellationToken cancellationToken = default) =>
        _negativeAcknowledge(requeue, cancellationToken);
}
