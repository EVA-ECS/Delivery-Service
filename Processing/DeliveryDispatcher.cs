using System.Threading.Channels;
using Delivery_Service.Messaging;

namespace Delivery_Service.Processing;

public sealed class DeliveryDispatcher : IAsyncDisposable
{
    private readonly Channel<DeliveryQueueMessage> _messages;
    private readonly IDeliveryMessageProcessor _processor;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _processingCancellation = new();
    private readonly Task[] _workers;
    private bool _started;

    public DeliveryDispatcher(
        int workerCount,
        IDeliveryMessageProcessor processor,
        ILogger logger)
    {
        if (workerCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(workerCount));
        }

        _processor = processor;
        _logger = logger;
        _workers = new Task[workerCount];
        _messages = Channel.CreateBounded<DeliveryQueueMessage>(
            new BoundedChannelOptions(workerCount)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = workerCount == 1,
                SingleWriter = true,
                AllowSynchronousContinuations = false
            });
    }

    public void Start()
    {
        if (_started)
        {
            throw new InvalidOperationException("Dispatcher has already been started.");
        }

        _started = true;
        for (var index = 0; index < _workers.Length; index++)
        {
            var workerId = index + 1;
            _workers[index] = RunWorkerAsync(workerId);
        }
    }

    public ValueTask EnqueueAsync(
        DeliveryQueueMessage message,
        CancellationToken cancellationToken) =>
        _messages.Writer.WriteAsync(message, cancellationToken);

    public void Complete() => _messages.Writer.TryComplete();

    public async Task WaitForCompletionAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.WhenAll(_workers).WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _processingCancellation.Cancel();
            await Task.WhenAll(_workers);
        }
    }

    public async ValueTask DisposeAsync()
    {
        Complete();
        _processingCancellation.Cancel();

        if (_started)
        {
            await Task.WhenAll(_workers);
        }

        _processingCancellation.Dispose();
    }

    private async Task RunWorkerAsync(int workerId)
    {
        await foreach (var message in _messages.Reader.ReadAllAsync())
        {
            try
            {
                var result = await _processor.ProcessAsync(
                    message,
                    _processingCancellation.Token);

                if (result == DeliveryProcessingResult.Processed)
                {
                    await message.AcknowledgeAsync(_processingCancellation.Token);
                }
                else
                {
                    await TryNegativeAcknowledgeAsync(message, requeue: false);
                }
            }
            catch (OperationCanceledException) when (_processingCancellation.IsCancellationRequested)
            {
                await TryNegativeAcknowledgeAsync(message, requeue: true);
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "Delivery worker {WorkerId} failed; message will be requeued.",
                    workerId);

                await TryNegativeAcknowledgeAsync(message, requeue: true);
            }
        }
    }

    private async Task TryNegativeAcknowledgeAsync(
        DeliveryQueueMessage message,
        bool requeue)
    {
        try
        {
            await message.NegativeAcknowledgeAsync(
                requeue,
                CancellationToken.None);
        }
        catch (Exception exception)
        {
            // A closed RabbitMQ channel requeues unacknowledged messages itself.
            // Keep this worker alive so the configured pool size does not shrink.
            _logger.LogWarning(
                exception,
                "RabbitMQ Nack failed; channel recovery will handle the unacknowledged message.");
        }
    }
}
