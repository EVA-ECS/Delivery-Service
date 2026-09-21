using System.Collections.Concurrent;
using Chat.Contracts.Events;

namespace Delivery_Service.Processing;

/// <summary>
/// Keeps a bounded set of reusable delivery workers. The MassTransit endpoint
/// limits message concurrency to the same number, so a message is only handed
/// to a free worker.
/// </summary>
public sealed class DeliveryWorkerPool
{
    private readonly ConcurrentBag<DeliveryWorker> _freeWorkers = [];

    public DeliveryWorkerPool(
        int workerCount,
        IDeliveryMessageProcessor processor,
        ILoggerFactory loggerFactory)
    {
        if (workerCount is < 1 or > 256)
        {
            throw new ArgumentOutOfRangeException(nameof(workerCount));
        }

        for (var index = 0; index < workerCount; index++)
        {
            _freeWorkers.Add(new DeliveryWorker(
                processor,
                loggerFactory.CreateLogger<DeliveryWorker>()));
        }

        WorkerCount = workerCount;
    }

    public int WorkerCount { get; }

    public async Task ProcessAsync(
        ChatMessageEvent message,
        CancellationToken cancellationToken)
    {
        if (!_freeWorkers.TryTake(out var worker))
        {
            throw new InvalidOperationException("No delivery worker is free.");
        }

        try
        {
            await worker.ProcessAsync(message, cancellationToken);
        }
        finally
        {
            _freeWorkers.Add(worker);
        }
    }
}
