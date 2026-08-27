using EVA_ECS.Chat.Contracts.Events;

namespace Delivery_Service.Routing;

public interface IDeliveryRouter
{
    Task<DeliveryRouteResult> RouteAsync(
        ChatMessagePublishedEvent message,
        CancellationToken cancellationToken);
}

public enum DeliveryRouteResult
{
    Delivered,
    Offline
}
