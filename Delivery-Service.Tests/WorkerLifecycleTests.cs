using Delivery_Service.Configuration;
using Delivery_Service.Messaging;
using Delivery_Service.Processing;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Delivery_Service.Tests;

public sealed class WorkerLifecycleTests
{
    [Fact]
    public async Task RabbitMqStartupFailureIsRetriedAndShutdownStopsConsumer()
    {
        var consumer = new RetryConsumer();
        var worker = new Worker(
            NullLogger<Worker>.Instance,
            consumer,
            new NoOpProcessor(),
            Options.Create(new DeliveryOptions
            {
                WorkerCount = 2,
                ConnectionRetryDelaySeconds = 0,
                ShutdownTimeoutSeconds = 2
            }));

        await worker.StartAsync(CancellationToken.None);
        await consumer.SecondAttempt.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await worker.StopAsync(CancellationToken.None);

        Assert.True(consumer.StartAttempts >= 2);
        Assert.True(consumer.StopCalls >= 1);
    }

    private sealed class RetryConsumer : IDeliveryQueueConsumer
    {
        private readonly TaskCompletionSource _completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int StartAttempts { get; private set; }
        public int StopCalls { get; private set; }
        public TaskCompletionSource SecondAttempt { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task Completion => _completion.Task;

        public Task StartAsync(
            Func<DeliveryQueueMessage, CancellationToken, ValueTask> onMessage,
            ushort prefetchCount,
            CancellationToken cancellationToken)
        {
            StartAttempts++;
            if (StartAttempts == 1)
            {
                throw new IOException("rabbit unavailable");
            }

            Assert.Equal((ushort)2, prefetchCount);
            SecondAttempt.TrySetResult();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            StopCalls++;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NoOpProcessor : IDeliveryMessageProcessor
    {
        public Task<DeliveryProcessingResult> ProcessAsync(
            DeliveryQueueMessage message,
            CancellationToken cancellationToken) =>
            Task.FromResult(DeliveryProcessingResult.Processed);
    }
}
