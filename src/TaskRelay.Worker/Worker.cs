using StackExchange.Redis;
using TaskRelay.Shared;

namespace TaskRelay.Worker;

public class Worker(IConnectionMultiplexer redis, IConfiguration config, ILogger<Worker> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = redis.GetDatabase();
        var consumer = config["Worker:Name"] ?? $"worker-{Guid.NewGuid().ToString("N")[..6]}";
        var minIdleMs = config.GetValue("Worker:MinIdleMs", 10000);
        var workMs = config.GetValue("Worker:SimulatedWorkMs", 5000);

        try
        {
            await db.StreamCreateConsumerGroupAsync(
                QueueNames.TaskStream, QueueNames.ConsumerGroup,
                StreamPosition.Beginning, createStream: true);
        }
        catch (RedisServerException e) when (e.Message.Contains("BUSYGROUP"))
        {
            // 群組已經存在,正常情況,忽略
        }

        logger.LogInformation("{Consumer} started (minIdle={MinIdle}ms)", consumer, minIdleMs);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // 1. 先找閒置太久的單子,有就接手
                var claimed = await db.StreamAutoClaimAsync(
                    QueueNames.TaskStream, QueueNames.ConsumerGroup, consumer,
                    minIdleMs, "0-0", count: 1);

                if (claimed.ClaimedEntries.Length > 0)
                {
                    await ProcessAsync(db, consumer, claimed.ClaimedEntries[0], "claimed", workMs, stoppingToken);
                    continue;
                }

                // 2. 沒有卡住的單子,才拿新任務
                var entries = await db.StreamReadGroupAsync(
                    QueueNames.TaskStream, QueueNames.ConsumerGroup, consumer,
                    StreamPosition.NewMessages, count: 1);

                if (entries.Length > 0)
                {
                    await ProcessAsync(db, consumer, entries[0], "new", workMs, stoppingToken);
                    continue;
                }

                await Task.Delay(1000, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task ProcessAsync(
        IDatabase db, string consumer, StreamEntry entry, string source, int workMs, CancellationToken ct)
    {
        var payload = entry.Values[0].Value;
        logger.LogInformation("{Consumer} start {Id} payload={Payload} ({Source})",
            consumer, entry.Id, payload, source);

        await Task.Delay(workMs, ct);

        await db.StreamAcknowledgeAsync(QueueNames.TaskStream, QueueNames.ConsumerGroup, entry.Id);
        logger.LogInformation("{Consumer} done {Id}", consumer, entry.Id);
    }
}