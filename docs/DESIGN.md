# TaskRelay Design Notes

TaskRelay is a small task queue built on Redis Streams. It has one goal: **no task is lost when a worker crashes or hangs.**

Each section covers one mechanism: the problem it solves, the solution, and how to verify it. The hash next to each title is the commit that introduced it (`git show <hash>`).

```
POST /tasks -> tasks-stream -> consumer group "workers" -> worker -> ACK
                                      ^                       |
                                      +-- idle too long: another worker takes over
```

Terms:

- **PEL (pending entries list):** tasks delivered to a worker but not yet acknowledged.
- **ACK:** removes a task from the PEL. It does not delete the message from the stream.
- **Idle time:** time since a task was last delivered, claimed, or heartbeated.
- **Delivery count:** how many times a task has been delivered. A takeover adds 1.

---

## 1. Streams instead of a List (`601e80f`)

**Problem.** With a List, a worker that pops a task and crashes mid-work loses the task silently.

**Solution.** Use a Stream with a consumer group. A delivered task stays in the PEL until the worker ACKs it, so a crash leaves a record.

**Trade-off.** Work happens before ACK, so delivery is at-least-once: a task may run more than once (for example, finished but crashed before ACK). Tasks must be idempotent.

**Verify.** Kill a worker mid-task. `XPENDING tasks-stream workers - + 10` still lists the task.

---

## 2. Taking over idle tasks (`c0527a1`)

**Problem.** A task sits in the PEL owned by a dead worker. Nobody processes it.

**Solution.** Each loop iteration first runs `XAUTOCLAIM` for tasks idle longer than `MinIdleMs`, and only reads new tasks if there is nothing to claim. Abandoned work is cleared before new work is started.

**Verify.** Kill a worker mid-task. After `MinIdleMs`, another worker logs `(claimed)` and the delivery count goes up by 1.

---

## 3. Heartbeat (`2c35852`)

**Problem.** Idle time alone cannot tell a dead worker from a slow task. A slow task gets taken over while its worker is still running, so two workers process it at once.

**Solution.** While working, a background loop calls `XCLAIM ... JUSTID` every `HeartbeatMs` to reset idle time. `JUSTID` does not increase the delivery count, so a heartbeat is not counted as a retry.

**Trade-off.** `MinIdleMs` should be at least 3x `HeartbeatMs` to tolerate network jitter and pauses. A startup warning is logged otherwise.

**Known limitation.** The heartbeat claims unconditionally. If a worker is paused for a long time and another worker takes over its task, the first worker's heartbeat claims the task back when it wakes up, and both end up processing it.

**Verify.** With `SimulatedWorkMs` greater than `MinIdleMs`, the slow task is not taken over. In `XPENDING`, idle time stays between 0 and `HeartbeatMs` and the delivery count stays at 1.

---

## 4. Task timeout (`4d40497`)

**Problem.** The heartbeat shows that the worker process is alive, not that the task is making progress. A stuck task keeps getting heartbeats, so:

1. the worker is occupied forever,
2. no other worker takes the task over, because idle time never grows,
3. the delivery count stays at 1, so nothing signals that the task is bad.

**Solution.** Each task gets its own timeout (`TaskTimeoutMs`), linked with the shutdown token into one cancellation token. The work and the heartbeat both listen to it. On timeout:

- the work and the heartbeat are cancelled,
- the task is **not ACKed** and stays in the PEL,
- idle time starts to grow, the task is claimed after `MinIdleMs`, and the delivery count goes up,
- the worker returns to its loop and takes the next task.

**Why not ACK or delete on timeout.** Either removes the task from the PEL, so it would never be retried and would be lost. A timeout is only a guess that the task is stuck.

**Timeout vs shutdown.** Both throw `OperationCanceledException`. The catch block checks `stoppingToken`: if it is cancelled the app is shutting down and the exception is rethrown to end the loop; if not, the timeout fired, so it logs and moves on. The linked token cannot be used for this because it does not say which source cancelled it.

**Trade-offs and limitations.**

- Cancellation is cooperative. Work stuck in a call that ignores the token is not interrupted. The heartbeat still stops, so the task is taken over, but that worker stays occupied.
- A timeout is a guess. A slow task may be treated as stuck, or a stuck worker may wake up later and finish while another worker has taken over. Tasks must stay idempotent.
- There is no retry limit yet, so a permanently stuck task is claimed again and again. Retry limit and dead-letter queue are the next step.

**Verify.**

```bash
dotnet run --project src/TaskRelay.Worker --Worker:Name=worker-A \
  --Worker:SimulatedWorkMs=20000 --Worker:TaskTimeoutMs=5000

curl -i -X POST http://localhost:5222/tasks \
  -H "Content-Type: application/json" -d '{"payload":"job-1"}'

docker compose exec redis redis-cli XPENDING tasks-stream workers - + 10
```

Expected:

- A heartbeat at about 3s, then `timeout` at about 5s, and no `done`.
- The worker keeps running. After about 10s the same task is logged as `(claimed)`, and the delivery count rises each round.
- Ctrl+C within 5s: no `timeout` log, clean exit, task still in the PEL.
- With `SimulatedWorkMs=3000` and `TaskTimeoutMs=30000`: `start` then `done`, and `XPENDING` is empty.

---

## Known limitations

| Limitation | Effect | Current handling |
|---|---|---|
| At-least-once delivery | Side effects may run twice | Tasks must be idempotent |
| Unconditional heartbeat claim | A long-paused worker can take back a task that was taken over | Not handled |
| Cooperative cancellation | Uncancellable calls keep a worker occupied | Task is still taken over; idempotency |
| No retry limit | A poison task is claimed forever | Next: dead-letter queue |