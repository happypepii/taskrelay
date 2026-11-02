using StackExchange.Redis;
using TaskRelay.Worker;

var builder = Host.CreateApplicationBuilder(args);

var redisConnection = builder.Configuration.GetConnectionString("Redis")
    ?? throw new InvalidOperationException("ConnectionStrings:Redis is not configured");

builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect(redisConnection));
builder.Services.AddHostedService<Worker>();

builder.Build().Run();