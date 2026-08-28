using Delivery_Service.Messaging;
using Delivery_Service.Processing;
using Microsoft.Extensions.Logging.Abstractions;

namespace Delivery_Service.Tests;

public sealed class DeliveryDispatcherTests
{
    [Fact]
    public async Task RunsExactlyConfiguredWorkersInParallel()
    {
        var processor = new BarrierProcessor(expectedParallelism: 3);
        await using var dispatcher = new DeliveryDispatcher(
            3,
            processor,
            NullLogger.Instance);
        dispatcher.Start();
        var acknowledgements = 0;

        for (var index = 0; index < 3; index++)
        {
            await dispatcher.EnqueueAsync(
                TestMessageFactory.CreateQueueMessage(
                    TestMessageFactory.CreateEvent(),
                    acknowledge: () => Interlocked.Increment(ref acknowledgements)),
                CancellationToken.None);
        }

        await processor.AllWorkersEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(3, processor.MaximumConcurrency);
        processor.Release.TrySetResult();
        dispatcher.Complete();
        await dispatcher.WaitForCompletionAsync(CancellationToken.None);

        Assert.Equal(3, acknowledgements);
    }

    [Fact]
    public async Task ProcessingFailureNacksWithRequeueAndNeverAcks()
    {
        await using var dispatcher = new DeliveryDispatcher(
            1,
            new ThrowingProcessor(),
            NullLogger.Instance);
        dispatcher.Start();
        var acknowledgements = 0;
        bool? requeue = null;

        await dispatcher.EnqueueAsync(
            TestMessageFactory.CreateQueueMessage(
                TestMessageFactory.CreateEvent(),
                acknowledge: () => acknowledgements++,
                negativeAcknowledge: value => requeue = value),
            CancellationToken.None);
        dispatcher.Complete();
        await dispatcher.WaitForCompletionAsync(CancellationToken.None);

        Assert.Equal(0, acknowledgements);
        Assert.True(requeue);
    }

    [Fact]
    public async Task InvalidMessageNacksWithoutRequeue()
    {
        await using var dispatcher = new DeliveryDispatcher(
            1,
            new ConstantProcessor(DeliveryProcessingResult.Invalid),
            NullLogger.Instance);
        dispatcher.Start();
        bool? requeue = null;

        await dispatcher.EnqueueAsync(
            TestMessageFactory.CreateQueueMessage(
                TestMessageFactory.CreateEvent(),
                negativeAcknowledge: value => requeue = value),
            CancellationToken.None);
        dispatcher.Complete();
        await dispatcher.WaitForCompletionAsync(CancellationToken.None);

        Assert.False(requeue);
    }

    [Fact]
    public async Task RabbitMqAckFailureAttemptsRequeue()
    {
        await using var dispatcher = new DeliveryDispatcher(
            1,
            new ConstantProcessor(DeliveryProcessingResult.Processed),
            NullLogger.Instance);
        dispatcher.Start();
        bool? requeue = null;

        await dispatcher.EnqueueAsync(
            TestMessageFactory.CreateQueueMessage(
                TestMessageFactory.CreateEvent(),
                acknowledgeAsync: _ => ValueTask.FromException(
                    new IOException("rabbit channel closed")),
                negativeAcknowledge: value => requeue = value),
            CancellationToken.None);
        dispatcher.Complete();
        await dispatcher.WaitForCompletionAsync(CancellationToken.None);

        Assert.True(requeue);
    }

    [Fact]
    public async Task NackFailureDoesNotStopTheWorker()
    {
        await using var dispatcher = new DeliveryDispatcher(
            1,
            new FirstCallFailsProcessor(),
            NullLogger.Instance);
        dispatcher.Start();
        var secondMessageAcknowledged = false;

        await dispatcher.EnqueueAsync(
            TestMessageFactory.CreateQueueMessage(
                TestMessageFactory.CreateEvent(),
                negativeAcknowledgeAsync: (_, _) => ValueTask.FromException(
                    new IOException("rabbit channel failed"))),
            CancellationToken.None);
        await dispatcher.EnqueueAsync(
            TestMessageFactory.CreateQueueMessage(
                TestMessageFactory.CreateEvent(),
                acknowledge: () => secondMessageAcknowledged = true),
            CancellationToken.None);

        dispatcher.Complete();
        await dispatcher.WaitForCompletionAsync(CancellationToken.None);

        Assert.True(secondMessageAcknowledged);
    }

    [Fact]
    public async Task ShutdownDrainsInFlightMessageBeforeAcknowledging()
    {
        var processor = new ControlledProcessor();
        await using var dispatcher = new DeliveryDispatcher(
            1,
            processor,
            NullLogger.Instance);
        dispatcher.Start();
        var acknowledged = false;

        await dispatcher.EnqueueAsync(
            TestMessageFactory.CreateQueueMessage(
                TestMessageFactory.CreateEvent(),
                acknowledge: () => acknowledged = true),
            CancellationToken.None);
        await processor.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        dispatcher.Complete();

        var completion = dispatcher.WaitForCompletionAsync(CancellationToken.None);
        Assert.False(completion.IsCompleted);
        processor.Release.TrySetResult();
        await completion;

        Assert.True(acknowledged);
    }

    [Fact]
    public async Task ShutdownTimeoutCancelsAndRequeuesInFlightMessage()
    {
        var processor = new CancellableProcessor();
        await using var dispatcher = new DeliveryDispatcher(
            1,
            processor,
            NullLogger.Instance);
        dispatcher.Start();
        bool? requeue = null;

        await dispatcher.EnqueueAsync(
            TestMessageFactory.CreateQueueMessage(
                TestMessageFactory.CreateEvent(),
                negativeAcknowledge: value => requeue = value),
            CancellationToken.None);
        await processor.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        dispatcher.Complete();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await dispatcher.WaitForCompletionAsync(timeout.Token);

        Assert.True(requeue);
    }

    private sealed class ConstantProcessor : IDeliveryMessageProcessor
    {
        private readonly DeliveryProcessingResult _result;
        public ConstantProcessor(DeliveryProcessingResult result) => _result = result;
        public Task<DeliveryProcessingResult> ProcessAsync(
            DeliveryQueueMessage message,
            CancellationToken cancellationToken) => Task.FromResult(_result);
    }

    private sealed class ThrowingProcessor : IDeliveryMessageProcessor
    {
        public Task<DeliveryProcessingResult> ProcessAsync(
            DeliveryQueueMessage message,
            CancellationToken cancellationToken) =>
            Task.FromException<DeliveryProcessingResult>(new IOException("redis unavailable"));
    }

    private sealed class FirstCallFailsProcessor : IDeliveryMessageProcessor
    {
        private int _calls;

        public Task<DeliveryProcessingResult> ProcessAsync(
            DeliveryQueueMessage message,
            CancellationToken cancellationToken) =>
            Interlocked.Increment(ref _calls) == 1
                ? Task.FromException<DeliveryProcessingResult>(
                    new IOException("redis unavailable"))
                : Task.FromResult(DeliveryProcessingResult.Processed);
    }

    private sealed class BarrierProcessor : IDeliveryMessageProcessor
    {
        private readonly int _expectedParallelism;
        private int _concurrency;
        private int _maximumConcurrency;

        public BarrierProcessor(int expectedParallelism) =>
            _expectedParallelism = expectedParallelism;

        public TaskCompletionSource AllWorkersEntered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int MaximumConcurrency => Volatile.Read(ref _maximumConcurrency);

        public async Task<DeliveryProcessingResult> ProcessAsync(
            DeliveryQueueMessage message,
            CancellationToken cancellationToken)
        {
            var current = Interlocked.Increment(ref _concurrency);
            UpdateMaximum(current);
            if (current == _expectedParallelism)
            {
                AllWorkersEntered.TrySetResult();
            }

            await Release.Task.WaitAsync(cancellationToken);
            Interlocked.Decrement(ref _concurrency);
            return DeliveryProcessingResult.Processed;
        }

        private void UpdateMaximum(int value)
        {
            int observed;
            do
            {
                observed = Volatile.Read(ref _maximumConcurrency);
                if (value <= observed)
                {
                    return;
                }
            }
            while (Interlocked.CompareExchange(
                       ref _maximumConcurrency,
                       value,
                       observed) != observed);
        }
    }

    private sealed class ControlledProcessor : IDeliveryMessageProcessor
    {
        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<DeliveryProcessingResult> ProcessAsync(
            DeliveryQueueMessage message,
            CancellationToken cancellationToken)
        {
            Started.TrySetResult();
            await Release.Task.WaitAsync(cancellationToken);
            return DeliveryProcessingResult.Processed;
        }
    }

    private sealed class CancellableProcessor : IDeliveryMessageProcessor
    {
        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<DeliveryProcessingResult> ProcessAsync(
            DeliveryQueueMessage message,
            CancellationToken cancellationToken)
        {
            Started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return DeliveryProcessingResult.Processed;
        }
    }
}
