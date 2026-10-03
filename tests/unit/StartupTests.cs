using System.Diagnostics;
using Delivery_Service.Processing;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace IsolatedStartup.Tests;

// Run the real worker composition root with disconnected infrastructure.
// No application source, real credentials, network clients or listening ports are required.
public sealed class StartupTests
{
    [Fact]
    public async Task CompositionRootBuildsValidServicesWithoutStartingExternalInfrastructure()
    {
        var application = typeof(DeliveryWorkerPool).Assembly;
        var observed = false;
        var database = new Mock<IDatabase>();
        var subscriber = new Mock<ISubscriber>();
        var redis = new Mock<IConnectionMultiplexer>();
        redis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(database.Object);
        redis.Setup(x => x.GetSubscriber(It.IsAny<object>())).Returns(subscriber.Object);
        using var subscriptions = new Subscriptions();
        using var listeners = DiagnosticListener.AllListeners.Subscribe(new Observer<DiagnosticListener>(listener =>
        {
            if (listener.Name != "Microsoft.Extensions.Hosting") return;
            subscriptions.Add(listener.Subscribe(new Observer<KeyValuePair<string, object?>>(entry =>
            {
                if (entry.Key == "HostBuilding" && entry.Value is IHostBuilder builder)
                {
                    builder.ConfigureServices((_, services) =>
                    {
                        foreach (var service in services.Where(x => x.ServiceType == typeof(IHostedService)
                            && x.ImplementationType?.Name != "GenericWebHostService").ToArray()) services.Remove(service);
                        services.RemoveAll<IConnectionMultiplexer>();
                        services.AddSingleton(redis.Object);
                    });
                }
                if (entry.Key == "HostBuilt" && entry.Value is IHost host)
                {
                    observed = true;
                    _ = host.Services.GetService<IBus>(); // Evaluate topology configuration without connecting.
                    foreach (var type in application.GetTypes().Where(x => x.IsClass && !x.IsAbstract && x.Name.EndsWith("Options")))
                    {
                        var option = host.Services.GetService(typeof(IOptions<>).MakeGenericType(type));
                        _ = option?.GetType().GetProperty("Value")?.GetValue(option);
                    }
                    foreach (var type in application.GetTypes().Where(x => x.IsInterface))
                        _ = host.Services.GetService(type);
                    var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
                    lifetime.ApplicationStarted.Register(lifetime.StopApplication);
                }
            })));
        }));
        string[] args = ["--Supabase:Url=https://example.invalid", "--Supabase:PublishableKey=fake-publishable",
            "--Supabase:SecretKey=fake-backend", "--RabbitMQ:Host=example.invalid", "--Redis:ConnectionString=example.invalid:6379"];
        var result = application.EntryPoint!.Invoke(null, [args]);
        if (result is Task running) await running.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.True(observed, "The application's host was not exercised.");
        redis.Verify(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()), Times.AtLeastOnce);
    }

    private sealed class Subscriptions : List<IDisposable>, IDisposable
    {
        public void Dispose() { foreach (var subscription in this) subscription.Dispose(); }
    }

    private sealed class Observer<T>(Action<T> next) : IObserver<T>
    {
        public void OnNext(T value) => next(value);
        public void OnError(Exception error) => throw error;
        public void OnCompleted() { }
    }
}
