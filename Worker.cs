using Delivery_Service.Configuration;
using Delivery_Service.Messaging;
using Delivery_Service.Processing;
using Microsoft.Extensions.Options;

namespace Delivery_Service;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IDeliveryQueueConsumer _consumer;
    private readonly IDeliveryMessageProcessor _processor;
    private readonly DeliveryOptions _options;

    public Worker(
        ILogger<Worker> logger,
        IDeliveryQueueConsumer consumer,
        IDeliveryMessageProcessor processor,
        IOptions<DeliveryOptions> options)
    {
        _logger = logger;
        _consumer = consumer;
        _processor = processor;
        _options = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var dispatcher = new DeliveryDispatcher(
            _options.WorkerCount,
            _processor,
            _logger);

        dispatcher.Start();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _consumer.StartAsync(
                        dispatcher.EnqueueAsync,
                        checked((ushort)_options.WorkerCount),
                        stoppingToken);

                    _logger.LogInformation(
                        "Delivery consumer started with {WorkerCount} workers and prefetch {PrefetchCount}.",
                        _options.WorkerCount,
                        _options.WorkerCount);

                    await _consumer.Completion.WaitAsync(stoppingToken);

                    if (!stoppingToken.IsCancellationRequested)
                    {
                        throw new InvalidOperationException(
                            "RabbitMQ consumer stopped unexpectedly.");
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    _logger.LogError(
                        exception,
                        "RabbitMQ consumer unavailable. Retrying in {RetryDelaySeconds} seconds.",
                        _options.ConnectionRetryDelaySeconds);

                    await StopConsumerSafelyAsync(CancellationToken.None);
                    await Task.Delay(
                        TimeSpan.FromSeconds(_options.ConnectionRetryDelaySeconds),
                        stoppingToken);
                }
            }
        }
        finally
        {
            using var shutdownTimeout = new CancellationTokenSource(
                TimeSpan.FromSeconds(_options.ShutdownTimeoutSeconds));

            await StopConsumerSafelyAsync(shutdownTimeout.Token);
            dispatcher.Complete();
            await dispatcher.WaitForCompletionAsync(shutdownTimeout.Token);

            _logger.LogInformation("Delivery worker stopped cleanly.");
        }
    }

    private async Task StopConsumerSafelyAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _consumer.StopAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("Timed out while stopping the RabbitMQ consumer.");
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "RabbitMQ consumer could not be stopped cleanly.");
        }
    }
}
