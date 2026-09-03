namespace Delivery_Service.Configuration;

public sealed class DeliveryOptions
{
    public const string SectionName = "Delivery";

    public int WorkerCount { get; set; } = 3;
}
