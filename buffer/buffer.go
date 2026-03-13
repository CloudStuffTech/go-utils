package buffer

import (
	"context"
	"fmt"
	"time"
)

// FlushFunc processes a batch of accumulated items.
//
// It is called when:
//   - ShouldFlush returns true after an item is appended
//   - The flush interval ticker fires
//   - dataChan is closed (graceful shutdown)
//
// The context passed is the exact same one given to Run(). During a
// graceful shutdown, it is highly likely this context has already been
// cancelled by the parent application. Therefore, implementations must
// shield the backend call to ensure the final drain succeeds:
//
//	Flush: func(ctx context.Context, batch []MyMsg) error {
//	    // Shield the context from parent cancellation but enforce a hard timeout
//	    flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
//	    defer cancel()
//	    return backend.Send(flushCtx, batch)
//	},
//
// Returning a non-nil error triggers OnFlushError if set. The batch is
// reset regardless — the buffer does not retry. Retry logic, if needed,
// belongs inside FlushFunc or OnFlushError.
//
// The batch slice must not be retained after FlushFunc returns. The buffer
// will reuse the underlying array. Copy if you need to hold on to items:
//
//	Flush: func(ctx context.Context, batch []MyMsg) error {
//	    owned := append([]MyMsg(nil), batch...) // safe to retain
//	    go process(owned)
//	    return nil
//	},
type FlushFunc[T any] func(ctx context.Context, batch []T) error

// Config holds all behavioural knobs for a Buffer.
//
// Only Flush is required. All other fields have sensible defaults and
// can be omitted.
//
// Example minimal configuration:
//
//	buf, err := buffer.NewBuffer(buffer.Config[MyMsg]{
//	    Capacity: 500,
//	    Flush: func(ctx context.Context, batch []MyMsg) error {
//	        return kinesis.PutRecords(ctx, batch)
//	    },
//	})
//
// Example full configuration:
//
//	buf, err := buffer.NewBuffer(buffer.Config[MyMsg]{
//	    Capacity:      500,
//	    ChanSize:      2000,
//	    FlushInterval: 2 * time.Second,
//
//	    Flush: func(ctx context.Context, batch []MyMsg) error {
//	        return kinesis.PutRecords(ctx, batch)
//	    },
//	    CanAdd: func(batch []MyMsg, item MyMsg) bool {
//	        return totalBytes(batch)+len(item.Data) <= 5*1024*1024
//	    },
//	    OnReject: func(item MyMsg) {
//	        log.Printf("rejected oversized message: %v", item.ID)
//	    },
//	    ShouldFlush: func(batch []MyMsg) bool {
//	        return len(batch) >= 500 || totalBytes(batch) >= 4*1024*1024
//	    },
//	    OnFlushError: func(err error, batch []MyMsg) {
//	        metrics.FlushErrors.Inc()
//	        deadLetterQueue.Send(batch)
//	    },
//	})
type Config[T any] struct {
	// Flush is the only required field.
	//
	// It is called with the current batch every time a flush is triggered.
	// See FlushFunc for full semantics.
	Flush FlushFunc[T]

	// WillOverflow is called before an item is added to the current batch to check
	// if appending it would breach a strict backend limit (e.g., a 5MB payload size).
	//
	// If it returns true, the buffer immediately flushes the *existing* batch
	// to make room, and then evaluates the new item for the next empty batch.
	//
	// This is crucial for byte-size constraints where adding an item might
	// create an invalid payload that the backend (like AWS Kinesis) would reject.
	//
	//  WillOverflow: func(batch []MyMsg, item MyMsg) bool {
	//      return totalBytes(batch)+len(item.Data) > 5*1024*1024 // 5MB limit
	//  },
	//
	// This function MUST NOT mutate the batch.
	//
	// Default: always returns false (no preemptive size-based flushing).
	WillOverflow func(batch []T, item T) bool

	// CanAdd is called before appending each incoming item to the batch.
	// Return false to reject the item — OnReject will be called instead.
	//
	// Useful for enforcing constraints that go beyond simple count limits,
	// such as total payload size (e.g. Kinesis 5MB per PutRecords call):
	//
	//	CanAdd: func(batch []MyMsg, item MyMsg) bool {
	//	    return totalBytes(batch)+len(item.Data) <= 5*1024*1024
	//	},
	//
	// This function MUST NOT mutate batch.
	//
	// Default: always returns true (all items are accepted).
	CanAdd func(batch []T, item T) bool

	// OnReject is called when CanAdd returns false for an item.
	//
	// Use this to log rejections, forward to a dead-letter queue,
	// or record metrics. It must not block indefinitely, as it is
	// called synchronously inside Run().
	//
	// Default: no-op.
	OnReject func(item T)

	// ShouldFlush is called after each successful append.
	// Return true to trigger an immediate flush.
	//
	// This is where you define your primary flush condition.
	// Common strategies:
	//   - Flush by count:  len(batch) >= maxRecords
	//   - Flush by size:   totalBytes(batch) >= maxBytes
	//   - Flush by both:   either condition above
	//
	// Time-based flushing is handled separately by FlushInterval —
	// you do not need to track time here.
	//
	// This function must be side-effect free.
	//
	// Default: flush when len(batch) >= Capacity.
	ShouldFlush func(batch []T) bool

	// OnFlushError is called when Flush returns a non-nil error.
	//
	// The batch passed here is the one that failed. The buffer has
	// already reset — it will not retry. If you need retry or
	// dead-lettering, implement it here or inside Flush.
	//
	// Note: if Flush spawns a goroutine and returns nil optimistically,
	// errors from that goroutine are outside the buffer's visibility —
	// handle them within the goroutine itself.
	//
	// Default: no-op (errors are silently ignored).
	OnFlushError func(err error, batch []T)

	// Capacity is the target batch size: the number of items to accumulate
	// before ShouldFlush (default) triggers a flush.
	//
	// It also controls the initial allocation of the internal batch slice,
	// so setting it close to your expected batch size avoids reallocations.
	//
	// Default: 100.
	Capacity int

	// ChanSize is the buffer size of the internal dataChan.
	//
	// A larger value decouples producers from the flush cycle, reducing
	// the chance that Add() blocks under bursty load. However, it increases
	// memory usage and the number of in-flight unprocessed items on shutdown.
	//
	// Rule of thumb: set to at least Capacity, ideally 2–4x Capacity.
	//
	// Default: 2 * Capacity.
	ChanSize int

	// FlushInterval is the maximum time between flushes, regardless of
	// whether ShouldFlush has fired.
	//
	// This acts as a safety net to ensure items don't sit indefinitely
	// in a partially filled batch during low-traffic periods.
	//
	// Setting this too low increases flush frequency and backend pressure.
	// Setting this too high increases latency for tail items.
	//
	// Default: 5 seconds.
	FlushInterval time.Duration
}

func (c *Config[T]) withDefaults() Config[T] {
	cfg := *c

	if cfg.Capacity == 0 {
		cfg.Capacity = 100
	}
	if cfg.ChanSize == 0 {
		cfg.ChanSize = 2 * cfg.Capacity
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	if cfg.CanAdd == nil {
		cfg.CanAdd = func([]T, T) bool { return true }
	}
	if cfg.WillOverflow == nil {
		cfg.WillOverflow = func([]T, T) bool { return false }
	}
	if cfg.OnReject == nil {
		cfg.OnReject = func(T) {}
	}
	if cfg.ShouldFlush == nil {
		cfg.ShouldFlush = func(batch []T) bool {
			return len(batch) >= cfg.Capacity
		}
	}
	if cfg.OnFlushError == nil {
		cfg.OnFlushError = func(error, []T) {}
	}

	return cfg
}

type Buffer[T any] struct {
	dataChan chan T
	cfg      Config[T]
}

func NewBuffer[T any](cfg Config[T]) (*Buffer[T], error) {
	if cfg.Flush == nil {
		return nil, fmt.Errorf("buffer: Flush is required")
	}
	c := cfg.withDefaults()
	return &Buffer[T]{
		dataChan: make(chan T, c.ChanSize),
		cfg:      c,
	}, nil
}

// Add sends an item to the buffer's internal channel for processing by Run.
//
// It blocks if the channel is full, providing natural backpressure to the
// producer. Pass a context with a timeout or deadline to bound how long Add
// may block before giving up.
//
// Add returns ctx.Err() if the context is cancelled before the item could
// be queued. The item is not added in that case — the caller is responsible
// for handling it (e.g. nacking a Pub/Sub message).
//
// Add must not be called after Close. Doing so will panic.
func (b *Buffer[T]) Add(ctx context.Context, item T) error {
	// If context is already cancelled, we just return
	// so that items are not added in the select
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case b.dataChan <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close signals Run that no more items will be produced.
//
// It closes the internal channel, which causes Run to drain all remaining
// buffered items, perform a final flush, and return nil.
//
// # Caller responsibility
//
// Close must only be called once all producers have finished calling Add.
// Calling Close while a producer might still call Add will cause a panic
// (send on closed channel). The caller is responsible for coordinating this,
// typically with a sync.WaitGroup:
//
//	var wg sync.WaitGroup
//	wg.Add(1)
//	go func() {
//	    defer wg.Done()
//	    produceItems(ctx, buf)
//	}()
//
//	// Shutdown sequence: wait for producers, then signal the buffer.
//	wg.Wait()
//	buf.Close()
//
// Close is the only mechanism that stops Run. Cancelling the context passed
// to Run does NOT stop it — see Run for details.
func (b *Buffer[T]) Close() {
	close(b.dataChan)
}

// ItemsInChannel returns the number of items currently sitting in the
// internal channel, waiting to be picked up by Run.
//
// This is a snapshot value — it may be stale by the time the caller
// acts on it. Intended for monitoring and diagnostics only.
func (b *Buffer[T]) ItemsInChannel() int {
	return len(b.dataChan)
}

// Run is the buffer's main processing loop. It must be called exactly once,
// in its own goroutine, before any calls to Add.
//
// Run blocks until the internal channel is closed via [Buffer.Close], at
// which point it drains any remaining items, performs a final flush, and
// returns nil.
//
// # Context
//
// The context controls the lifecycle of Flush calls, not the lifecycle of
// Run itself. Cancelling ctx does NOT stop Run — it only causes that
// cancelled context to be forwarded to each subsequent Flush invocation.
//
// This is an intentional design decision. The buffer cannot safely stop on
// context cancellation alone because it has no visibility into whether
// producers have finished calling Add. Stopping Run prematurely could
// cause items that are already in the channel to be silently dropped.
//
// The correct shutdown sequence is always:
//
//  1. Stop all producers (ensure no further Add calls will be made).
//  2. Call buf.Close() to signal Run that the input is exhausted.
//  3. Wait for Run to return (it will drain and flush everything first).
//
// Example with a Pub/Sub subscriber as the producer:
//
//	go buf.Run(ctx)
//
//	// sub.Receive blocks until ctx is cancelled AND all in-flight
//	// message handlers have returned — guaranteeing no more Add calls.
//	sub.Receive(ctx, func(msgCtx context.Context, m *pubsub.Message) {
//	    if err := buf.Add(msgCtx, m); err != nil {
//	        m.Nack()
//	    }
//	})
//
//	// Safe to close: sub.Receive has exited, no more Add calls possible.
//	buf.Close()
//
// # Flush context and graceful shutdown
//
// Because ctx is likely already cancelled by the time the final drain flush
// fires, Flush implementations should shield their backend calls:
//
//	Flush: func(ctx context.Context, batch []MyMsg) error {
//	    flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
//	    defer cancel()
//	    return backend.Send(flushCtx, batch)
//	},
func (b *Buffer[T]) Run(ctx context.Context) error {
	cfg := b.cfg
	batch := make([]T, 0, cfg.Capacity)

	ticker := time.NewTicker(cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		if err := cfg.Flush(ctx, batch); err != nil {
			cfg.OnFlushError(err, batch)
		}

		batch = batch[:0]
		ticker.Reset(cfg.FlushInterval)
	}

	processItem := func(item T) {
		if cfg.WillOverflow(batch, item) {
			if len(batch) > 0 {
				flush()
			}
		}

		if !cfg.CanAdd(batch, item) {
			cfg.OnReject(item)
			return
		}

		batch = append(batch, item)

		if cfg.ShouldFlush(batch) {
			flush()
		}
	}

	for {
		select {
		case item, ok := <-b.dataChan:
			if !ok {
				flush()
				return nil
			}
			processItem(item)
		case <-ticker.C:
			flush()
		}
	}
}
