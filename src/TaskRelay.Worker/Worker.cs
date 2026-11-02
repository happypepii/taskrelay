using StackExchange.Redis;
using TaskRelay.Shared;

namespace TaskRelay.Worker;

public class Worker(IConnectionMultiplexer redis, IConfiguration config, ILogger<Worker> logger)
    : BackgroundService
{
    private const int SimulatedWorkMs = 5000;   // 暫時假裝每個任務要做 5 秒

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = redis.GetDatabase();
        var consumer = config["Worker:Name"] ?? $"worker-{Guid.NewGuid().ToString("N")[..6]}";

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

        logger.LogInformation("{Consumer} started", consumer);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var entries = await db.StreamReadGroupAsync(
                    QueueNames.TaskStream, QueueNames.ConsumerGroup, consumer,
                    StreamPosition.NewMessages, count: 1);

                if (entries.Length == 0)
                {
                    await Task.Delay(1000, stoppingToken);
                    continue;
                }

                var entry = entries[0];
                var payload = entry.Values[0].Value;
                logger.LogInformation("{Consumer} start {Id} payload={Payload}", consumer, entry.Id, payload);

                await Task.Delay(SimulatedWorkMs, stoppingToken);

                await db.StreamAcknowledgeAsync(QueueNames.TaskStream, QueueNames.ConsumerGroup, entry.Id);
                logger.LogInformation("{Consumer} done {Id}", consumer, entry.Id);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}