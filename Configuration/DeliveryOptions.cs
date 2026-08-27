namespace Delivery_Service.Configuration;

public sealed class DeliveryOptions
{
    public const string SectionName = "Delivery";

    public int WorkerCount { get; set; } = 4;
    public int ConnectionRetryDelaySeconds { get; set; } = 5;
    public int ShutdownTimeoutSeconds { get; set; } = 30;
}
