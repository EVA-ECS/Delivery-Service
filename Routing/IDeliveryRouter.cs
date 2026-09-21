using Chat.Contracts.Events;

namespace Delivery_Service.Routing;

public interface IDeliveryRouter
{
    Task<DeliveryRouteResult> RouteAsync(
        ChatMessageEvent message,
        CancellationToken cancellationToken);
}

public enum DeliveryRouteResult
{
    Delivered,
    Offline
}
