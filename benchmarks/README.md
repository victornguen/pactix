# Benchmarks: standard polling vs wakeup-driven outbox scheduling

Compares the two outbox scheduling strategies pactix exposes, on a real
PostgreSQL 16: the default caller-owned fixed-interval poll loop, and the
opt-in wakeup-driven loop (`WakeupRunner`) fed by LISTEN/NOTIFY.

Run it (Docker required):

```bash
uv run python -m benchmarks.bench_outbox
```

Writes `benchmarks/results.json` and prints the table below. Total runtime is
~75 seconds; one testcontainer is reused across all runs.

## Method

**Harness** (`benchmarks/bench_outbox.py`): a testcontainers
`postgres:16-alpine` instance (URL handling mirrors `tests/conftest.py`),
schema created per run with `pactix.db.tables.metadata.drop_all` /
`create_all`, so each scenario × profile starts from a fresh, empty outbox.
Producer and worker get **separate async engines**; worker-side statements are
counted via SQLAlchemy's `before_cursor_execute` event attached to
`engine.sync_engine` on the worker engine only, so producer inserts, the
drain observer, and `CoalescedNotifier`'s `SELECT pg_notify(...)` are never
counted as worker queries.

**Scenarios**:

- `polling` — the loop from `examples/worker_loop.py`:
  `OutboxRunner.run_once` then `asyncio.sleep(outbox_poll_interval)` (250 ms).
- `wakeup-trigger` — the `CREATE FUNCTION` / `CREATE TRIGGER` DDL from
  migration `0003_add_outbox_wake_trigger.py` executed verbatim, then
  `WakeupRunner` with `WakeupPolicy(enabled=True, notify_mode=NotifyMode.TRIGGER)`.
- `wakeup-coalesced` — `WakeupRunner` with `NotifyMode.COALESCED`; the
  producer calls `CoalescedNotifier(engine, channel, coalesce_interval).notify()`
  after each commit (defaults: channel `outbox_wake`, coalesce interval 50 ms).

`LocalWakeup` is deliberately **not** wired in: an in-process signal would
fire identically in both wakeup modes and hide the cross-process NOTIFY path
this benchmark exists to compare. Adaptive fallback polling (1 s initial,
×2, 60 s cap) remains enabled in all wakeup runs as the safety net.

**Load profiles**:

- `steady` — 1000 events, one transaction each, deadline-paced at 100 events/s.
- `burst` — 2000 events as fast as the producer can commit, 50 per transaction.

**Publisher**: a trivial async function returning `PublishOutcome.published()`
immediately — the benchmark isolates scheduling and persistence costs, not
transport latency. Each publish is still its own `mark_published` transaction,
exactly as `OutboxRunner` does in production.

**Metrics**:

- `latency_*_ms` — per-event `published_at - created_at`, read back from the
  database after drain. Both timestamps are written by the benchmark process
  itself (append and publish), so the clock is identical; values are tz-aware.
- `throughput_eps` — events / wall seconds from the first append to the moment
  an observer (polling the published count on the *producer* engine every
  25 ms) sees all rows published.
- `worker_queries` / `q/s` — statements counted on the worker engine within
  that wall window.
- `cpu%` — `resource.getrusage(RUSAGE_SELF)` utime+stime delta / wall × 100.
  Producer and worker share one process, so this is whole-process CPU.
- `idle_q/s` — worker statements/s during a 5 s post-drain window with an
  empty outbox: fixed-interval polling keeps polling; the wakeup fallback
  backs off.

## Environment

Results below are from a single run on 2026-08-30 (UTC):

- PostgreSQL `postgres:16-alpine` in testcontainers (Docker Desktop 29.7.2)
- macOS 26.6.2, arm64, Apple M4 Pro
- Python 3.14.3, SQLAlchemy 2.0 + asyncpg, local `src/` build of pactix

Absolute numbers are shaped by Docker Desktop commit latency (~2–3 ms per
transaction); treat them as comparable across scenarios within this run, not
as universal constants.

## Results

| scenario         | profile | events | wall_s |   eps | mean_ms | p50_ms | p99_ms | queries |   q/s | cpu% | idle_q/s |
|------------------|---------|-------:|-------:|------:|--------:|-------:|-------:|--------:|------:|-----:|---------:|
| polling          | steady  |   1000 |  10.12 |  98.8 |  229.85 | 233.74 | 368.26 |    1064 | 105.1 | 28.4 |      7.6 |
| polling          | burst   |   2000 |   9.61 | 208.2 | 4189.64 | 4179.44 | 8390.10 |  2046 | 212.9 | 28.4 |      7.6 |
| wakeup-trigger   | steady  |   1000 |  10.00 | 100.0 |    5.35 |   4.98 |  11.37 |    4954 | 495.2 | 40.9 |      0.8 |
| wakeup-trigger   | burst   |   2000 |   2.07 | 965.7 |  641.23 | 684.58 | 1053.10 |  2046 | 987.9 | 56.8 |      0.4 |
| wakeup-coalesced | steady  |   1000 |  10.00 | 100.0 |   20.71 |  18.11 |  53.71 |    2406 | 240.5 | 35.4 |      0.8 |
| wakeup-coalesced | burst   |   2000 |   2.22 | 901.2 |  703.54 | 739.70 | 1157.68 |  2046 | 921.9 | 53.4 |      0.4 |

## Observations

- **Polling steady p50 is 234 ms, not the naive `poll_interval/2` ≈ 125 ms.**
  Investigated; the harness is correct. The effective poll cycle is
  250 ms sleep + pass duration, and at 100 events/s each pass publishes ~25
  events with one commit each (~3 ms/commit here), stretching the cycle to
  ~330 ms. Expected mean latency is then `cycle/2 + position-in-pass` ≈
  220 ms — matching the measurement. Cross-check via query arithmetic:
  1064 statements ≈ 1000 publishes + ~32 passes × 2 statements (reclaim +
  claim), i.e. a 10.1 s window / 32 passes ≈ 316 ms cycle. Under truly light
  load (near-empty passes) p50 does approach 125 ms.
- **Wakeup latency is single/double-digit milliseconds.** Trigger mode:
  p50 5.0 ms, p99 11.4 ms under steady load — ~47× lower than polling.
  Coalesced mode: p50 18.1 ms, p99 53.7 ms, bounded by the 50 ms coalesce
  window; in exchange it emits ~5× fewer NOTIFYs and does about half the
  worker statements of trigger mode (2406 vs 4954).
- **Burst: wakeup drains ~4.5× faster** (966 vs 208 events/s; 2.1 s vs 9.6 s
  wall; p99 1.05 s vs 8.4 s) with a byte-identical statement count (2046
  everywhere = 2000 publishes + ~23 claim passes × 2). The entire difference
  is the 250 ms inter-pass sleep in the polling loop; `WakeupRunner` drains
  back-to-back until the queue is empty.
- **Idle workers: ~10–19× fewer statements.** Polling burns 7.6 statements/s
  forever (4 polls/s × reclaim+claim). The wakeup fallback backs off
  1 s → 2 s → 4 s … → 60 s cap, so an idle wakeup worker costs 0.4–0.8
  statements/s, dropping toward ~2 statements/min as backoff saturates. This
  is where the query-volume advantage of wakeup actually lives.
- **The trade-off is real and visible**: under a continuous trickle,
  wakeup-*trigger* does more statements per second than polling (495 vs 105),
  because each wake costs a claim pass plus an empty verification pass where
  polling batches ~25 events into one pass. Those extra passes are cheap
  (index-only empty claims) and buy two orders of magnitude of latency;
  coalesced mode halves the overhead. CPU tracks the same story (41–57% vs
  28% of one core, whole-process).
- Sanity checks all held: latencies are same-clock (no negative values),
  every run drained to exactly `events` published rows, burst statement
  counts are identical across scenarios, and two consecutive full runs agreed
  within a few percent.
