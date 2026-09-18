using Chat.Contracts.Events;

namespace Delivery_Service.Tests;

internal static class TestMessageFactory
{
    public static ChatMessageEvent CreateEvent(Guid? targetId = null) =>
        new(
            Guid.NewGuid().ToString(),
            Guid.NewGuid().ToString(),
            (targetId ?? Guid.NewGuid()).ToString(),
            "Hallo aus dem Test",
            DateTime.UtcNow);
}
