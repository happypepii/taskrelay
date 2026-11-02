using StackExchange.Redis;
using TaskRelay.Shared;

var builder = WebApplication.CreateBuilder(args);

var redisConnection = builder.Configuration.GetConnectionString("Redis")
    ?? throw new InvalidOperationException("ConnectionStrings:Redis is not configured");

builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect(redisConnection));

var app = builder.Build();

app.MapPost("/tasks", async (SubmitTaskRequest request, IConnectionMultiplexer redis) =>
{
    if (string.IsNullOrWhiteSpace(request.Payload))
        return Results.BadRequest(new { error = "payload is required" });

    var db = redis.GetDatabase();
    var id = await db.StreamAddAsync(QueueNames.TaskStream, "payload", request.Payload);

    return Results.Accepted($"/tasks/{id}", new { id = id.ToString() });
});

app.Run();

record SubmitTaskRequest(string? Payload);