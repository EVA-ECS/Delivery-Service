using System.Reflection;
using Chat.Contracts.Events;
using Delivery_Service.Messaging;
using Delivery_Service.Processing;
using MassTransit;
using Microsoft.Extensions.Logging.Abstractions;

namespace Delivery_Service.Tests;

public sealed class QueueReceiverTests
{
    [Fact]
    public async Task ConsumerCannotCompleteBeforeProcessingAndPropagatesFailure()
    {
        var processor = new WaitingProcessor();
        var receiver = new QueueReceiver(new DeliveryWorkerPool(1, processor, NullLoggerFactory.Instance));
        var context = DispatchProxy.Create<ConsumeContext<ChatMessageEvent>, ContextProxy>();
        var consuming = receiver.Consume(context);
        await processor.Started.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.False(consuming.IsCompleted); // MassTransit must not ACK yet.
        processor.Processed.SetException(new IOException("Redis unavailable"));
        await Assert.ThrowsAsync<IOException>(() => consuming); // Triggers retry/error handling, not successful ACK.
    }

    [Fact]
    public async Task ConsumerCompletesOnlyAfterProcessingSucceeds()
    {
        var processor = new WaitingProcessor();
        var receiver = new QueueReceiver(new DeliveryWorkerPool(1, processor, NullLoggerFactory.Instance));
        var context = DispatchProxy.Create<ConsumeContext<ChatMessageEvent>, ContextProxy>();
        var consuming = receiver.Consume(context);
        await processor.Started.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.False(consuming.IsCompleted);
        processor.Processed.SetResult(DeliveryProcessingResult.Processed);
        await consuming.WaitAsync(TimeSpan.FromSeconds(3));
    }
    public class ContextProxy : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => targetMethod?.Name switch
        {
            "get_Message" => TestMessageFactory.CreateEvent(),
            "get_CancellationToken" => CancellationToken.None,
            _ => throw new NotSupportedException(targetMethod?.Name)
        };
    }
    private sealed class WaitingProcessor : IDeliveryMessageProcessor
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<DeliveryProcessingResult> Processed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task<DeliveryProcessingResult> ProcessAsync(ChatMessageEvent message, CancellationToken cancellationToken)
        {
            Started.SetResult();
            return Processed.Task;
        }
    }
}
