using System.Text.Json;
using Delivery_Service.Messaging;
using EVA_ECS.Chat.Contracts.Events;
using EVA_ECS.Chat.Contracts.Messages;

namespace Delivery_Service.Tests;

internal static class TestMessageFactory
{
    public static ChatMessagePublishedEvent CreateEvent(Guid? targetId = null) => new()
    {
        MessageId = Guid.NewGuid(),
        RoomId = Guid.NewGuid(),
        SenderId = Guid.NewGuid(),
        TargetId = targetId ?? Guid.NewGuid(),
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Payload = new EncryptedMessagePayload
        {
            EncryptedKey = "encrypted-key",
            Iv = "initialization-vector",
            Ciphertext = "ciphertext",
            Signature = "signature"
        }
    };

    public static DeliveryQueueMessage CreateQueueMessage(
        ChatMessagePublishedEvent message,
        Action? acknowledge = null,
        Action<bool>? negativeAcknowledge = null,
        Func<CancellationToken, ValueTask>? acknowledgeAsync = null,
        string? routingKey = null)
    {
        return new DeliveryQueueMessage(
            JsonSerializer.SerializeToUtf8Bytes(
                message,
                new JsonSerializerOptions(JsonSerializerDefaults.Web)),
            routingKey ?? $"msg.private.{message.TargetId}",
            redelivered: false,
            acknowledgeAsync ?? (_ =>
            {
                acknowledge?.Invoke();
                return ValueTask.CompletedTask;
            }),
            (requeue, _) =>
            {
                negativeAcknowledge?.Invoke(requeue);
                return ValueTask.CompletedTask;
            });
    }
}
