package channelbus

// MIT License
//
// Copyright (c) 2023-2026 Seth Osher
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

import (
	"context"
	"sync"
	"sync/atomic"
)

// DropPolicy controls what happens when an unbounded subscription's buffer
// reaches MaxBufferSize.
type DropPolicy int

const (
	// DropNewest discards the incoming message (same as bounded Subscribe).
	// This is the zero value and the default policy.
	DropNewest DropPolicy = iota
	// DropOldest removes the oldest message from the buffer to make room.
	DropOldest
)

// SlowConsumerFunc is called when an unbounded subscription's internal buffer
// depth crosses the SlowConsumerThreshold. It receives the subscription (for
// identification) and the current buffer depth. The callback is invoked in
// a separate goroutine and will not block publishers.
type SlowConsumerFunc[T any] func(sub *Subscription[T], depth int)

// UnboundedConfig configures an unbounded subscription.
type UnboundedConfig[T any] struct {
	// SlowConsumerThreshold is the buffer depth that triggers the
	// OnSlowConsumer callback. Zero value means use default (1000).
	// Set to -1 to disable.
	SlowConsumerThreshold int

	// MaxBufferSize is the hard limit on internal buffer size.
	// When reached, DropPolicy determines behavior.
	// Zero value means no limit (truly unbounded, use with caution).
	MaxBufferSize int

	// DropPolicy controls what happens when MaxBufferSize is reached.
	// Zero value (DropNewest) is the default.
	DropPolicy DropPolicy

	// OnSlowConsumer is called when the buffer depth exceeds
	// SlowConsumerThreshold. Called in a separate goroutine so it
	// will not block publishers. May be called multiple times (each
	// time depth crosses the threshold after dropping back below it).
	OnSlowConsumer SlowConsumerFunc[T]
}

// unboundedState holds the internal state for an unbounded subscription.
// It manages the goroutine and growing buffer between Publish and sub.Ch.
type unboundedState[T any] struct {
	mu       sync.Mutex
	buf      []T
	notifyCh chan struct{} // signals the drain goroutine that buf has items
	cancel   context.CancelFunc
	done     chan struct{} // closed when drain goroutine exits
	closed   atomic.Bool
	depth    atomic.Int64 // current buffer depth (atomic for lock-free reads)
	peak     atomic.Int64 // peak buffer depth ever observed

	config UnboundedConfig[T]
	sub    *Subscription[T]

	// slowConsumerTriggered tracks whether the callback has fired for the
	// current slow episode. Reset when depth drops back below threshold.
	slowConsumerTriggered atomic.Bool
}

// enqueue adds a message to the internal buffer. Called from Publish (must not block).
// Returns true if the message was accepted, false if dropped.
func (u *unboundedState[T]) enqueue(msg T) bool {
	if u.closed.Load() {
		return false
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	// Check hard limit
	if u.config.MaxBufferSize > 0 && len(u.buf) >= u.config.MaxBufferSize {
		switch u.config.DropPolicy {
		case DropOldest:
			// Shift elements left in-place to reuse the backing array
			copy(u.buf, u.buf[1:])
			u.buf[len(u.buf)-1] = msg
			u.sub.errCnt.Add(1)
		case DropNewest:
			u.sub.errCnt.Add(1)
			return false
		}
	} else {
		u.buf = append(u.buf, msg)
	}

	depth := u.updateDepthAndPeak()
	u.notifyAndCheckSlowConsumer(depth)
	return true
}

// updateDepthAndPeak updates depth and peak atomics from the current buffer length.
// Must be called while u.mu is held.
func (u *unboundedState[T]) updateDepthAndPeak() int64 {
	depth := int64(len(u.buf))
	u.depth.Store(depth)
	if depth > u.peak.Load() {
		u.peak.Store(depth)
	}
	return depth
}

// notifyAndCheckSlowConsumer signals the drain goroutine and fires the slow
// consumer callback if the threshold is exceeded. Called outside the mutex;
// the callback contract says it must not block.
func (u *unboundedState[T]) notifyAndCheckSlowConsumer(depth int64) {
	// Non-blocking signal to drain goroutine
	select {
	case u.notifyCh <- struct{}{}:
	default:
	}

	// Slow consumer detection — fire callback in a goroutine so a
	// misbehaving callback can never block the Publish path.
	threshold := u.config.SlowConsumerThreshold
	if threshold > 0 && depth >= int64(threshold) {
		if u.config.OnSlowConsumer != nil && u.slowConsumerTriggered.CompareAndSwap(false, true) {
			go u.config.OnSlowConsumer(u.sub, int(depth))
		}
	}
}

// drain is the background goroutine that moves messages from buf to sub.Ch.
// It exits when the context is cancelled.
func (u *unboundedState[T]) drain(ctx context.Context) {
	defer close(u.done)

	for {
		// Wait for notification or cancellation.
		// notifyCh is never closed; shutdown is handled by ctx cancellation.
		select {
		case <-ctx.Done():
			return
		case <-u.notifyCh:
		}

		// Drain all available messages in batches to reduce lock contention.
		for {
			// Check for cancellation before processing next batch
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Grab all pending messages in one lock acquisition
			u.mu.Lock()
			if len(u.buf) == 0 {
				u.depth.Store(0)
				u.mu.Unlock()

				// Reset slow consumer flag once buffer is fully empty
				threshold := u.config.SlowConsumerThreshold
				if threshold > 0 {
					u.slowConsumerTriggered.Store(false)
				}
				break
			}
			batch := u.buf
			u.buf = nil
			u.depth.Store(0)
			u.mu.Unlock()

			// Send each message to the consumer.
			// Uses select with ctx.Done so Close() unblocks a stuck send.
			for _, msg := range batch {
				select {
				case u.sub.Ch <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// close cancels the drain goroutine and waits for it to exit.
// After close returns, no new publishes are accepted and the drain goroutine
// has stopped. Already-buffered messages that have not been drained are discarded.
func (u *unboundedState[T]) close() {
	if u.closed.CompareAndSwap(false, true) {
		u.cancel()
		<-u.done // wait for drain goroutine to exit
	}
}

// SubscribeUnbounded adds a new subscription with an internal unbounded buffer.
// Unlike Subscribe, messages are never dropped due to a full channel buffer
// (unless MaxBufferSize is set and reached). An internal goroutine drains the
// buffer into sub.Ch.
//
// The subscription's Ch has a buffer of 1; the real buffering happens in the
// internal slice. Use Unsubscribe or sub.Close() to clean up the goroutine.
//
// Use BufferDepth() and PeakBufferDepth() on the returned subscription to
// monitor backpressure. Configure OnSlowConsumer for proactive detection.
func (b *ChannelBus[T]) SubscribeUnbounded(topic string, config ...UnboundedConfig[T]) *Subscription[T] {
	b.lock.Lock()
	defer b.lock.Unlock()

	cfg := UnboundedConfig[T]{
		SlowConsumerThreshold: 1000,
		// DropPolicy defaults to DropNewest (zero value)
	}
	if len(config) > 0 {
		c := config[0]
		// Merge provided config onto defaults. Zero values mean "use default".
		// Use -1 for SlowConsumerThreshold to explicitly disable.
		if c.SlowConsumerThreshold != 0 {
			cfg.SlowConsumerThreshold = c.SlowConsumerThreshold
		}
		if c.MaxBufferSize != 0 {
			cfg.MaxBufferSize = c.MaxBufferSize
		}
		if c.DropPolicy != 0 {
			cfg.DropPolicy = c.DropPolicy
		}
		if c.OnSlowConsumer != nil {
			cfg.OnSlowConsumer = c.OnSlowConsumer
		}
	}

	sub := &Subscription[T]{
		Prefix: topic,
		Ch:     make(chan T, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())

	state := &unboundedState[T]{
		notifyCh: make(chan struct{}, 1),
		cancel:   cancel,
		done:     make(chan struct{}),
		config:   cfg,
		sub:      sub,
	}

	sub.unbounded = state

	// Start drain goroutine
	go state.drain(ctx)

	b.subs = append(b.subs, sub)
	return sub
}
