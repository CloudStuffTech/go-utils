package buffer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mustNewBuffer is a test helper that fails the test/benchmark immediately
// if the buffer fails to initialize.
func mustNewBuffer[T any](tb testing.TB, cfg Config[T]) *Buffer[T] {
	tb.Helper() // Marks this as a helper so error line numbers point to the test, not here!
	buf, err := NewBuffer(cfg)
	if err != nil {
		tb.Fatalf("failed to initialize buffer: %v", err)
	}
	return buf
}

func BenchmarkBuffer_ParallelThroughput(b *testing.B) {
	// 1. Setup mock telemetry to verify everything processed
	var totalFlushed atomic.Int64
	var batchCount atomic.Int64

	// 2. Configure the buffer for maximum speed
	// We use a dummy integer instead of a complex struct to isolate the buffer's pure mechanical overhead.
	cfg := Config[int]{
		Capacity:      500,
		FlushInterval: 1 * time.Second,
		// Mock Flush: Instantly returns without doing any I/O
		Flush: func(ctx context.Context, batch []int) error {
			totalFlushed.Add(int64(len(batch)))
			batchCount.Add(1)
			return nil
		},
	}

	buf := mustNewBuffer(b, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Start the consumer loop in the background
	errChan := make(chan error, 1)
	go func() {
		errChan <- buf.Run(ctx)
	}()

	// 4. Reset the timer and track memory allocations!
	// We don't want the setup time to affect the nanosecond score.
	b.ResetTimer()
	b.ReportAllocs()

	// 5. THE CRASH TEST
	// b.RunParallel spins up multiple goroutines based on your CPU cores
	// and hammers the Add() function concurrently, exactly like Pub/Sub.
	b.RunParallel(func(pb *testing.PB) {
		item := 42 // Dummy payload
		for pb.Next() {
			if err := buf.Add(ctx, item); err != nil {
				b.Errorf("Failed to add item: %v", err)
			}
		}
	})

	// 6. Stop the timer to isolate the pure Add/Batch speed
	b.StopTimer()

	// 7. Cleanly shut down to ensure no deadlocks
	buf.Close()
	cancel()

	if err := <-errChan; err != nil {
		b.Fatalf("Buffer run exited with error: %v", err)
	}

	// Optional: Print the results to verify no messages were dropped
	b.Logf("Processed %d items across %d batches.", totalFlushed.Load(), batchCount.Load())
}

// TestBuffer_Basic verifies that the buffer flushes exactly when Capacity is reached.
func TestBuffer_Basic(t *testing.T) {
	var mu sync.Mutex
	var received [][]int

	cfg := Config[int]{
		Capacity: 3,
		Flush: func(ctx context.Context, batch []int) error {
			mu.Lock()
			// We MUST copy the slice because the buffer reuses the underlying array
			cb := make([]int, len(batch))
			copy(cb, batch)
			received = append(received, cb)
			mu.Unlock()
			return nil
		},
	}

	buf := mustNewBuffer(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // Automatically fires when the test finishes (or panics)

	go buf.Run(ctx)

	// Send 5 items. With Capacity 3, we expect 1 flush of [1, 2, 3].
	// The items [4, 5] should still be in the buffer.
	for i := 1; i <= 5; i++ {
		buf.Add(ctx, i)
	}

	// Give the goroutine a tiny bit of time to process
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(received) != 1 {
		t.Errorf("expected 1 flush, got %d", len(received))
	}
	if len(received[0]) != 3 {
		t.Errorf("expected batch size 3, got %d", len(received[0]))
	}
	mu.Unlock()
}

// TestBuffer_FlushInterval verifies that items are flushed even if Capacity isn't met.
func TestBuffer_FlushInterval(t *testing.T) {
	var mu sync.Mutex
	var received [][]int

	cfg := Config[int]{
		Capacity:      100,
		FlushInterval: 100 * time.Millisecond,
		Flush: func(ctx context.Context, batch []int) error {
			mu.Lock()
			received = append(received, append([]int(nil), batch...))
			mu.Unlock()
			return nil
		},
	}

	buf := mustNewBuffer(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // Automatically fires when the test finishes (or panics)

	go buf.Run(ctx)

	buf.Add(ctx, 42) // Only one item, won't trigger Capacity flush

	// Wait longer than FlushInterval
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if len(received) != 1 {
		t.Errorf("expected timer-based flush, but got %d flushes", len(received))
	}
	mu.Unlock()
}

// TestBuffer_Close verifies that closing the channel drains the remaining items.
func TestBuffer_Close(t *testing.T) {
	var mu sync.Mutex
	var received [][]int

	cfg := Config[int]{
		Capacity: 10,
		Flush: func(ctx context.Context, batch []int) error {
			mu.Lock()
			received = append(received, append([]int(nil), batch...))
			mu.Unlock()
			return nil
		},
	}

	buf := mustNewBuffer(t, cfg)

	// We use a channel to signal when Run() is finished
	done := make(chan struct{})
	go func() {
		buf.Run(context.Background())
		close(done)
	}()

	buf.Add(context.Background(), 1)
	buf.Add(context.Background(), 2)

	buf.Close() // This should trigger a flush of [1, 2]
	<-done

	if len(received) != 1 || len(received[0]) != 2 {
		t.Errorf("expected final drain flush of 2 items, got %v", received)
	}
}

func TestBuffer_WillOverflow(t *testing.T) {
	var mu sync.Mutex
	var received [][]int

	cfg := Config[int]{
		Capacity: 100,
		WillOverflow: func(batch []int, item int) bool {
			sum := 0
			for _, v := range batch {
				sum += v
			}
			return sum+item > 10
		},
		Flush: func(ctx context.Context, batch []int) error {
			mu.Lock()
			received = append(received, append([]int(nil), batch...))
			mu.Unlock()
			return nil
		},
	}

	buf := mustNewBuffer(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // Automatically fires when the test finishes (or panics)

	done := make(chan struct{})
	go func() {
		buf.Run(ctx)
		close(done)
	}()

	buf.Add(ctx, 4)
	buf.Add(ctx, 5)
	buf.Add(ctx, 3)

	// Close the buffer and wait for Run() to fully exit before asserting.
	// This guarantees the final batch containing [3] has been flushed.
	buf.Close()
	<-done

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 2 {
		t.Fatalf("expected 2 flushes, got %d", len(received))
	}
	if len(received[0]) != 2 || received[0][0] != 4 || received[0][1] != 5 {
		t.Errorf("expected first batch [4, 5], got %v", received[0])
	}
	// This is the assertion that was missing — verifies the overflow-triggering
	// item was not silently dropped and correctly landed in the next batch.
	if len(received[1]) != 1 || received[1][0] != 3 {
		t.Errorf("expected second batch [3], got %v", received[1])
	}
}

func TestBuffer_OnReject(t *testing.T) {
	var mu sync.Mutex
	var rejectedItem int
	var rejectCalled bool

	cfg := Config[int]{
		// CanAdd: Only allow even numbers
		CanAdd: func(batch []int, item int) bool {
			return item%2 == 0
		},
		OnReject: func(item int) {
			mu.Lock()
			defer mu.Unlock()
			rejectedItem = item
			rejectCalled = true
		},
		Flush: func(ctx context.Context, batch []int) error { return nil },
	}

	buf := mustNewBuffer(t, cfg)
	ctx := context.Background()

	// Start the buffer and immediately add an odd number
	go buf.Run(ctx)
	buf.Add(ctx, 7)

	// Small sleep for the goroutine to process the internal channel
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if !rejectCalled {
		t.Error("expected OnReject to be called for odd number")
	}
	if rejectedItem != 7 {
		t.Errorf("expected rejected item 7, got %d", rejectedItem)
	}
}

func TestBuffer_OnFlushError(t *testing.T) {
	var mu sync.Mutex
	var capturedErr error
	var capturedBatch []int

	cfg := Config[int]{
		Capacity: 2,
		Flush: func(ctx context.Context, batch []int) error {
			return fmt.Errorf("kinesis service unavailable")
		},
		OnFlushError: func(err error, batch []int) {
			mu.Lock()
			defer mu.Unlock()
			capturedErr = err
			capturedBatch = append([]int(nil), batch...)
		},
	}

	buf := mustNewBuffer(t, cfg)
	ctx := context.Background()
	go buf.Run(ctx)

	// Trigger a flush
	buf.Add(ctx, 1)
	buf.Add(ctx, 2)

	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if capturedErr == nil || capturedErr.Error() != "kinesis service unavailable" {
		t.Errorf("expected specific error, got %v", capturedErr)
	}
	if len(capturedBatch) != 2 {
		t.Errorf("expected batch of 2 to be passed to error handler, got %d", len(capturedBatch))
	}
}

func TestBuffer_ConcurrencyCorrectness(t *testing.T) {
	var flushCount atomic.Int64
	var itemCount atomic.Int64
	const totalItems = 1000
	const capacity = 100

	cfg := Config[int]{
		Capacity: capacity,
		Flush: func(ctx context.Context, batch []int) error {
			flushCount.Add(1)
			itemCount.Add(int64(len(batch)))
			return nil
		},
	}

	buf := mustNewBuffer(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // Automatically fires when the test finishes (or panics)

	doneChan := make(chan struct{})

	go func() {
		buf.Run(ctx)
		close(doneChan)
	}()

	// Hammer the buffer from 10 different goroutines
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				buf.Add(ctx, i)
			}
		}()
	}
	wg.Wait()

	// Wait for the final items to be processed
	time.Sleep(100 * time.Millisecond)
	buf.Close()
	<-doneChan

	if itemCount.Load() != totalItems {
		t.Errorf("expected %d total items, got %d", totalItems, itemCount.Load())
	}
	// Note: flushCount might be slightly higher than 10 if timer flushes
	// happened, but it should be at least 10.
	if flushCount.Load() < 10 {
		t.Errorf("expected at least 10 flushes, got %d", flushCount.Load())
	}
}

func TestBuffer_Backpressure(t *testing.T) {
	// 1. Create a buffer with a tiny channel and a slow Flush
	cfg := Config[int]{
		Capacity: 1,
		ChanSize: 1, // Tiny channel
		Flush: func(ctx context.Context, batch []int) error {
			time.Sleep(100 * time.Millisecond) // Artificial slowness
			return nil
		},
	}

	buf := mustNewBuffer(t, cfg)
	ctx := context.Background()
	go buf.Run(ctx)

	// 2. Fill the channel
	buf.Add(ctx, 1) // Item 1: Processing in Flush
	buf.Add(ctx, 2) // Item 2: Sitting in the channel

	// 3. The 3rd Add should block because the channel is full and Flush is sleeping
	start := time.Now()
	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err := buf.Add(timeoutCtx, 3)

	if err == nil {
		t.Error("expected Add to block and return context deadline exceeded")
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Error("Add returned too fast; it should have been blocked by the full channel")
	}
}

func TestBuffer_FlushContextCancellation(t *testing.T) {
	var mu sync.Mutex
	var ctxCanceledDuringFlush bool

	cfg := Config[int]{
		Capacity: 1,
		Flush: func(ctx context.Context, batch []int) error {
			<-ctx.Done() // Wait for the context to be canceled
			mu.Lock()
			defer mu.Unlock()
			ctxCanceledDuringFlush = true
			return ctx.Err()
		},
	}

	buf := mustNewBuffer(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())

	go buf.Run(ctx)

	buf.Add(ctx, 1) // Triggers flush, which hangs waiting for cancel

	time.Sleep(20 * time.Millisecond)
	cancel() // Cancel the master context

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if !ctxCanceledDuringFlush {
		t.Error("expected Flush to notice context cancellation")
	}
}

func TestBuffer_NoEmptyFlushes(t *testing.T) {
	var mu sync.Mutex
	var flushCount int

	cfg := Config[int]{
		FlushInterval: 10 * time.Millisecond,
		Flush: func(ctx context.Context, batch []int) error {
			mu.Lock()
			defer mu.Unlock()
			flushCount++
			return nil
		},
	}

	buf := mustNewBuffer(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // Automatically fires when the test finishes (or panics)

	done := make(chan struct{})
	go func() {
		buf.Run(ctx)
		close(done)
	}()

	// Wait for several ticks of the FlushInterval with an empty buffer.
	time.Sleep(50 * time.Millisecond)

	// Wait for Run() to fully exit before asserting, so we're
	// not racing against a flush that hasn't been counted yet.
	buf.Close()
	<-done

	mu.Lock()
	defer mu.Unlock()

	if flushCount > 0 {
		t.Errorf("expected 0 flushes for empty buffer, got %d", flushCount)
	}
}
