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
        var heartbeatMs = config.GetValue("Worker:HeartbeatMs", 3000);   // 0 = 關閉心跳
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

        logger.LogInformation("{Consumer} started (minIdle={MinIdle}ms, heartbeat={Heartbeat}ms)",
            consumer, minIdleMs, heartbeatMs);

        if (heartbeatMs > 0 && minIdleMs < heartbeatMs * 3)
        {
            logger.LogWarning("MinIdleMs ({MinIdle}) should be at least 3x HeartbeatMs ({Heartbeat})",
                minIdleMs, heartbeatMs);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var claimed = await db.StreamAutoClaimAsync(
                    QueueNames.TaskStream, QueueNames.ConsumerGroup, consumer,
                    minIdleMs, "0-0", count: 1);

                if (claimed.ClaimedEntries.Length > 0)
                {
                    await ProcessAsync(db, consumer, claimed.ClaimedEntries[0], "claimed",
                        workMs, heartbeatMs, stoppingToken);
                    continue;
                }

                var entries = await db.StreamReadGroupAsync(
                    QueueNames.TaskStream, QueueNames.ConsumerGroup, consumer,
                    StreamPosition.NewMessages, count: 1);

                if (entries.Length > 0)
                {
                    await ProcessAsync(db, consumer, entries[0], "new",
                        workMs, heartbeatMs, stoppingToken);
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
        IDatabase db, string consumer, StreamEntry entry, string source,
        int workMs, int heartbeatMs, CancellationToken ct)
    {
        var payload = entry.Values[0].Value;
        logger.LogInformation("{Consumer} start {Id} payload={Payload} ({Source})",
            consumer, entry.Id, payload, source);

        using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var heartbeat = heartbeatMs > 0
            ? HeartbeatAsync(db, consumer, entry.Id, heartbeatMs, heartbeatCts.Token)
            : Task.CompletedTask;

        try
        {
            await Task.Delay(workMs, ct);   // 之後這裡會換成真正的工作
        }
        finally
        {
            heartbeatCts.Cancel();
            await heartbeat;
        }

        await db.StreamAcknowledgeAsync(QueueNames.TaskStream, QueueNames.ConsumerGroup, entry.Id);
        logger.LogInformation("{Consumer} done {Id}", consumer, entry.Id);
    }

    private async Task HeartbeatAsync(
        IDatabase db, string consumer, RedisValue id, int intervalMs, CancellationToken ct)
    {
        try
        {
            while (true)
            {
                await Task.Delay(intervalMs, ct);
                try
                {
                    // minIdle=0:把這張單子重新認領給自己,閒置計時歸零
                    // IdsOnly(JUSTID):不會增加派送次數
                    await db.StreamClaimIdsOnlyAsync(
                        QueueNames.TaskStream, QueueNames.ConsumerGroup, consumer, 0, new[] { id });
                    logger.LogInformation("{Consumer} heartbeat {Id}", consumer, id);
                }
                catch (RedisException e)
                {
                    logger.LogWarning(e, "{Consumer} heartbeat failed for {Id}", consumer, id);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // 工作結束或程式關閉,心跳停止
        }
    }
}