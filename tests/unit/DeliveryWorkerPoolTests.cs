using Chat.Contracts.Events;
using Delivery_Service.Processing;
using Microsoft.Extensions.Logging.Abstractions;

namespace Delivery_Service.Tests;

public sealed class DeliveryWorkerPoolTests
{
    [Fact]
    public async Task UsesExactlyTheConfiguredNumberOfWorkersInParallel()
    {
        var processor = new ParallelProcessor(expectedParallelism: 3);
        var pool = new DeliveryWorkerPool(3, processor, NullLoggerFactory.Instance);

        var tasks = Enumerable.Range(0, 3)
            .Select(_ => pool.ProcessAsync(
                TestMessageFactory.CreateEvent(),
                CancellationToken.None))
            .ToArray();

        await processor.AllWorkersEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(3, processor.MaximumConcurrency);

        processor.Release.TrySetResult();
        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task WorkerIsReturnedToPoolAfterFailure()
    {
        var processor = new FailOnceProcessor();
        var pool = new DeliveryWorkerPool(1, processor, NullLoggerFactory.Instance);

        await Assert.ThrowsAsync<IOException>(() =>
            pool.ProcessAsync(TestMessageFactory.CreateEvent(), CancellationToken.None));

        await pool.ProcessAsync(TestMessageFactory.CreateEvent(), CancellationToken.None);

        Assert.Equal(2, processor.Calls);
    }

    [Fact]
    public async Task CancellationStopsTheInFlightWorker()
    {
        var processor = new CancellationProcessor();
        var pool = new DeliveryWorkerPool(1, processor, NullLoggerFactory.Instance);
        using var cancellation = new CancellationTokenSource();

        var processing = pool.ProcessAsync(
            TestMessageFactory.CreateEvent(),
            cancellation.Token);
        await processor.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => processing);

        // The worker is put back in the pool even when shutdown cancels it.
        await pool.ProcessAsync(
            TestMessageFactory.CreateEvent(),
            CancellationToken.None);
    }

    private sealed class ParallelProcessor : IDeliveryMessageProcessor
    {
        private readonly int _expectedParallelism;
        private int _current;
        private int _maximum;

        public ParallelProcessor(int expectedParallelism) =>
            _expectedParallelism = expectedParallelism;

        public TaskCompletionSource AllWorkersEntered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int MaximumConcurrency => Volatile.Read(ref _maximum);

        public async Task<DeliveryProcessingResult> ProcessAsync(
            ChatMessageEvent message,
            CancellationToken cancellationToken)
        {
            var current = Interlocked.Increment(ref _current);
            UpdateMaximum(current);

            if (current == _expectedParallelism)
            {
                AllWorkersEntered.TrySetResult();
            }

            await Release.Task.WaitAsync(cancellationToken);
            Interlocked.Decrement(ref _current);
            return DeliveryProcessingResult.Processed;
        }

        private void UpdateMaximum(int value)
        {
            int observed;
            do
            {
                observed = Volatile.Read(ref _maximum);
                if (value <= observed)
                {
                    return;
                }
            }
            while (Interlocked.CompareExchange(ref _maximum, value, observed) != observed);
        }
    }

    private sealed class FailOnceProcessor : IDeliveryMessageProcessor
    {
        public int Calls { get; private set; }

        public Task<DeliveryProcessingResult> ProcessAsync(
            ChatMessageEvent message,
            CancellationToken cancellationToken)
        {
            Calls++;
            return Calls == 1
                ? Task.FromException<DeliveryProcessingResult>(
                    new IOException("redis unavailable"))
                : Task.FromResult(DeliveryProcessingResult.Processed);
        }
    }

    private sealed class CancellationProcessor : IDeliveryMessageProcessor
    {
        private int _calls;

        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<DeliveryProcessingResult> ProcessAsync(
            ChatMessageEvent message,
            CancellationToken cancellationToken)
        {
            Started.TrySetResult();
            if (Interlocked.Increment(ref _calls) > 1)
            {
                return DeliveryProcessingResult.Processed;
            }

            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return DeliveryProcessingResult.Processed;
        }
    }
}
