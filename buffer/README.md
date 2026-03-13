# buffer

A generic, batching buffer for Go. It accumulates items from concurrent producers into batches and flushes them to a sink — by count, by size, or on a time interval.

Designed for high-throughput pipelines where writing one item at a time is too expensive: Kinesis, BigQuery, Kafka, HTTP bulk APIs, and similar backends.

---

## Features

- **Generic** — works with any type `T`
- **Concurrent-safe** — multiple producers can call `Add` simultaneously
- **Flexible flush triggers** — batch size, payload size, elapsed time, or any custom condition
- **Overflow protection** — pre-emptively flush before a backend hard limit is breached
- **Backpressure** — `Add` blocks naturally when the internal channel is full
- **Graceful shutdown** — `Close` drains all buffered items before returning

---

## Installation

```bash
go get github.com/CloudStuffTech/go-utils/buffer
```

---

## Quick Start

```go
buf, err := buffer.NewBuffer(buffer.Config[MyMsg]{
    Capacity: 500,
    Flush: func(ctx context.Context, batch []MyMsg) error {
        return kinesis.PutRecords(ctx, batch)
    },
})
if err != nil {
    log.Fatal(err)
}

ctx, cancel := context.WithCancel(context.Background())

// Start the processing loop in the background.
go buf.Run(ctx)

// Produce items from anywhere.
buf.Add(ctx, msg)

// Shutdown: stop producers first, then close the buffer.
cancel()
buf.Close()
```

---

## How It Works

```
producers                   buffer                      sink
─────────                   ──────                      ────
Add(item) ──→  [ dataChan ] ──→  Run() loop  ──→  Flush(batch)
Add(item) ──→  [ dataChan ]          ↑
Add(item) ──→  [ dataChan ]     ticker fires
                                 (interval)
```

`Run` is a single goroutine that owns the batch. It reads from the internal channel and accumulates items. A flush is triggered when any of the following occur:

- `ShouldFlush` returns true after an item is appended (default: `len(batch) >= Capacity`)
- `WillOverflow` returns true before an item is appended (pre-emptive flush)
- The `FlushInterval` ticker fires
- `Close()` is called (final drain)

Because only `Run` ever reads from the channel and mutates the batch, no internal locking is needed.

---

## Configuration

Only `Flush` is required. All other fields have sensible defaults.

```go
buf, err := buffer.NewBuffer(buffer.Config[MyMsg]{
    // Required: called with each completed batch.
    Flush: func(ctx context.Context, batch []MyMsg) error {
        return kinesis.PutRecords(ctx, batch)
    },

    // Target batch size. Also the default flush trigger.
    // Default: 100.
    Capacity: 500,

    // Internal channel buffer. Larger values absorb bursts better.
    // Default: 2 * Capacity.
    ChanSize: 2000,

    // Maximum time between flushes, even if Capacity isn't reached.
    // Default: 5 seconds.
    FlushInterval: 2 * time.Second,

    // Pre-emptive flush when adding an item would breach a hard limit.
    // The existing batch is flushed first; then the item is re-evaluated.
    WillOverflow: func(batch []MyMsg, item MyMsg) bool {
        return totalBytes(batch)+len(item.Data) > 5*1024*1024
    },

    // Per-item admission check. Return false to reject an item.
    CanAdd: func(batch []MyMsg, item MyMsg) bool {
        return len(item.Data) <= 1*1024*1024 // reject items over 1MB
    },

    // Called for every rejected item.
    OnReject: func(item MyMsg) {
        log.Printf("rejected oversized message: %s", item.ID)
    },

    // Custom flush condition. Overrides the default Capacity check.
    ShouldFlush: func(batch []MyMsg) bool {
        return len(batch) >= 500 || totalBytes(batch) >= 4*1024*1024
    },

    // Called when Flush returns an error. Batch is already reset.
    OnFlushError: func(err error, batch []MyMsg) {
        metrics.FlushErrors.Inc()
        deadLetterQueue.Send(batch)
    },
})
```

---

## Shutdown

The buffer separates two concerns that are easy to conflate:

| Concern | Mechanism |
|---|---|
| Stop the Flush from hitting a cancelled backend | `ctx` passed to `Run` |
| Stop `Run` itself and drain remaining items | `buf.Close()` |

**Cancelling `ctx` does not stop `Run`.** This is intentional — the buffer has no visibility into whether producers have finished calling `Add`. Stopping `Run` on context cancellation alone would risk silently dropping items that are already in the channel.

The correct shutdown sequence is always:

1. **Stop all producers** — ensure no further `Add` calls will be made
2. **Call `buf.Close()`** — signals `Run` that input is exhausted
3. **Wait for `Run` to return** — it will drain and flush everything first

```go
// Correct shutdown pattern with a WaitGroup.
var producerWg sync.WaitGroup

producerWg.Add(1)
go func() {
    defer producerWg.Done()
    produceItems(ctx, buf)
}()

// Signal producers to stop.
cancel()

// Wait until the last Add() call has returned.
producerWg.Wait()

// Now safe to close — no concurrent Add() calls are possible.
buf.Close()
```

### With Pub/Sub (Google Cloud)

`sub.Receive` handles producer coordination for you. It blocks until `ctx` is cancelled **and** all in-flight message handlers have returned — guaranteeing no further `Add` calls before it exits.

```go
go buf.Run(ctx)

sub.Receive(ctx, func(msgCtx context.Context, m *pubsub.Message) {
    if err := buf.Add(msgCtx, m); err != nil {
        m.Nack()
    }
})

// sub.Receive has fully stopped — safe to close.
buf.Close()
```

---

## Flush Context and Graceful Shutdown

When `buf.Close()` is called during shutdown, the final drain flush fires with the original `ctx` — which may already be cancelled. If your `Flush` implementation passes that context directly to a backend call, the call will fail immediately.

Shield against this with `context.WithoutCancel`:

```go
Flush: func(ctx context.Context, batch []MyMsg) error {
    // Detach from cancellation, but enforce a hard deadline
    // so the flush can't block indefinitely.
    flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
    defer cancel()
    return backend.Send(flushCtx, batch)
},
```

---

## Backpressure

`Add` blocks if the internal channel is full. This is intentional — it propagates backpressure to producers rather than silently dropping items or growing memory unboundedly.

To bound how long `Add` may block, pass a context with a timeout or deadline:

```go
addCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
defer cancel()

if err := buf.Add(addCtx, item); err != nil {
    // Channel was full for too long — handle accordingly.
    item.Nack()
}
```

Tune `ChanSize` to control how much burst the buffer can absorb before producers start to feel backpressure. A good starting point is `2–4x Capacity`.

---

## Batch Ownership

The batch slice passed to `Flush` must not be retained after `Flush` returns. The buffer reuses the underlying array for the next batch.

If you need to hold onto the batch — for example, to hand it off to a goroutine — copy it first:

```go
Flush: func(ctx context.Context, batch []MyMsg) error {
    owned := append([]MyMsg(nil), batch...) // copy before returning
    go process(owned)
    return nil
},
```

---

## Benchmarks

```
goos: darwin
goarch: arm64
pkg: github.com/CloudStuffTech/go-utils/buffer
cpu: Apple M4 Pro
BenchmarkBuffer_ParallelThroughput-12           12784041               185.5 ns/op             0 B/op          0 allocs/op
```

Run yourself:

```bash
go test -bench=. -benchmem ./...
```