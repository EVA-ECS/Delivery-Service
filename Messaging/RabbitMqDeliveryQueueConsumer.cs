using Delivery_Service.Configuration;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Delivery_Service.Messaging;

public sealed class RabbitMqDeliveryQueueConsumer : IDeliveryQueueConsumer
{
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqDeliveryQueueConsumer> _logger;
    // The workers share one RabbitMQ channel, so Ack/Nack calls must be serialized.
    private readonly SemaphoreSlim _channelLock = new(1, 1);
    private IConnection? _connection;
    private IChannel? _channel;
    private string? _consumerTag;
    private TaskCompletionSource _completion = NewCompletionSource();
    private bool _stopping;

    public RabbitMqDeliveryQueueConsumer(
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqDeliveryQueueConsumer> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public Task Completion => _completion.Task;

    public async Task StartAsync(
        Func<DeliveryQueueMessage, CancellationToken, ValueTask> onMessage,
        ushort prefetchCount,
        CancellationToken cancellationToken)
    {
        await DisposeSessionAsync();

        _stopping = false;
        _completion = NewCompletionSource();

        var factory = new ConnectionFactory
        {
            HostName = _options.Host,
            Port = _options.Port,
            UserName = _options.Username,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
            // Worker.cs owns the single, visible retry path.
            AutomaticRecoveryEnabled = false,
            ConsumerDispatchConcurrency = 1
        };

        _connection = await factory.CreateConnectionAsync(
            "delivery-service",
            cancellationToken);
        var channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
        _channel = channel;

        await channel.ExchangeDeclareAsync(
            exchange: _options.ExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);

        await channel.QueueBindAsync(
            queue: _options.QueueName,
            exchange: _options.ExchangeName,
            routingKey: _options.RoutingKey,
            arguments: null,
            cancellationToken: cancellationToken);

        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: prefetchCount,
            global: false,
            cancellationToken: cancellationToken);

        channel.ChannelShutdownAsync += (_, args) =>
        {
            if (!_stopping)
            {
                _logger.LogWarning(
                    "RabbitMQ channel shut down: {ReplyCode} {ReplyText}",
                    args.ReplyCode,
                    args.ReplyText);
            }

            _completion.TrySetResult();
            return Task.CompletedTask;
        };

        channel.CallbackExceptionAsync += (_, args) =>
        {
            _logger.LogError(args.Exception, "RabbitMQ consumer callback failed.");
            return Task.CompletedTask;
        };

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, args) =>
        {
            // RabbitMQ.Client owns args.Body after this callback returns.
            var body = args.Body.ToArray();
            var deliveryTag = args.DeliveryTag;
            var message = new DeliveryQueueMessage(
                body,
                args.RoutingKey,
                token => AcknowledgeAsync(channel, deliveryTag, token),
                (requeue, token) => NegativeAcknowledgeAsync(
                    channel,
                    deliveryTag,
                    requeue,
                    token));

            await onMessage(message, CancellationToken.None);
        };

        _consumerTag = await channel.BasicConsumeAsync(
            queue: _options.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopping = true;

        if (_channel is not null && _channel.IsOpen && !string.IsNullOrWhiteSpace(_consumerTag))
        {
            try
            {
                await _channel.BasicCancelAsync(
                    _consumerTag,
                    cancellationToken: cancellationToken);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                _logger.LogWarning(
                    exception,
                    "RabbitMQ consumer cancellation failed; unacknowledged messages will be recovered when the channel closes.");
            }
        }

        _consumerTag = null;
        _completion.TrySetResult();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);
        await DisposeSessionAsync();
        _channelLock.Dispose();
    }

    private async Task DisposeSessionAsync()
    {
        _consumerTag = null;

        if (_channel is not null)
        {
            await _channel.DisposeAsync();
            _channel = null;
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }

    private async ValueTask AcknowledgeAsync(
        IChannel channel,
        ulong deliveryTag,
        CancellationToken cancellationToken)
    {
        await _channelLock.WaitAsync(cancellationToken);
        try
        {
            if (!channel.IsOpen)
            {
                throw new InvalidOperationException("RabbitMQ channel is not open.");
            }

            await channel.BasicAckAsync(
                deliveryTag,
                multiple: false,
                cancellationToken);
        }
        finally
        {
            _channelLock.Release();
        }
    }

    private async ValueTask NegativeAcknowledgeAsync(
        IChannel channel,
        ulong deliveryTag,
        bool requeue,
        CancellationToken cancellationToken)
    {
        await _channelLock.WaitAsync(cancellationToken);
        try
        {
            if (!channel.IsOpen)
            {
                return;
            }

            await channel.BasicNackAsync(
                deliveryTag,
                multiple: false,
                requeue,
                cancellationToken);
        }
        finally
        {
            _channelLock.Release();
        }
    }

    private static TaskCompletionSource NewCompletionSource() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
