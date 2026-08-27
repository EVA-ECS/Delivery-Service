namespace Delivery_Service.Configuration;

public sealed class RabbitMqOptions
{
    public const string SectionName = "RabbitMQ";

    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string Username { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string VirtualHost { get; set; } = "/";
    public string ExchangeName { get; set; } = "chat_events";
    public string QueueName { get; set; } = "delivery.queue";
    public string RoutingKey { get; set; } = "msg.#";
}
