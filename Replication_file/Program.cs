using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using CMDDSReplication.Services;

// Replace ConfigureFunctionsWorkerDefaults with ConfigureFunctionsWebApplication for ASP.NET Core Integration
var host = new HostBuilder()
    .ConfigureFunctionsWebApplication() // <-- Fix for AZFW0014
    .ConfigureServices(services =>
    {
        services.AddSingleton<ScpService>();
        services.AddSingleton<DatabaseService>();
    })
    .Build();

host.Run();