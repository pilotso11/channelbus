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
	"sync"
	"sync/atomic"
)

// DropPolicy controls what happens when an unbounded subscription's buffer
// reaches MaxBufferSize.
type DropPolicy int

const (
	// DropOldest removes the oldest message from the buffer to make room.
	DropOldest DropPolicy = iota
	// DropNewest discards the incoming message (same as bounded Subscribe).
	DropNewest
)

// SlowConsumerFunc is called when an unbounded subscription's internal buffer
// depth crosses the SlowConsumerThreshold. It receives the subscription (for
// identification) and the current buffer depth. The callback is invoked from
// the Publish goroutine and must not block.
type SlowConsumerFunc[T any] func(sub *Subscription[T], depth int)

// UnboundedConfig configures an unbounded subscription.
type UnboundedConfig[T any] struct {
	// SlowConsumerThreshold is the buffer depth that triggers the
	// OnSlowConsumer callback. Default: 1000. Set to 0 to disable.
	SlowConsumerThreshold int

	// MaxBufferSize is the hard limit on internal buffer size.
	// When reached, DropPolicy determines behavior.
	// Default: 0 (no limit — truly unbounded, use with caution).
	MaxBufferSize int

	// DropPolicy controls what happens when MaxBufferSize is reached.
	// Default: DropNewest.
	DropPolicy DropPolicy

	// OnSlowConsumer is called when the buffer depth exceeds
	// SlowConsumerThreshold. Called from the Publish goroutine;
	// must not block. May be called multiple times (each time depth
	// crosses the threshold after dropping back below it).
	OnSlowConsumer SlowConsumerFunc[T]
}

// unboundedState holds the internal state for an unbounded subscription.
// It manages the goroutine and growing buffer between Publish and sub.Ch.
type unboundedState[T any] struct {
	mu       sync.Mutex
	buf      []T
	notifyCh chan struct{} // signals the drain goroutine that buf has items
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

	// Check hard limit
	if u.config.MaxBufferSize > 0 && len(u.buf) >= u.config.MaxBufferSize {
		switch u.config.DropPolicy {
		case DropOldest:
			// Copy to a new slice so the old backing array can be GC'd
			newBuf := make([]T, len(u.buf)-1, len(u.buf))
			copy(newBuf, u.buf[1:])
			u.buf = append(newBuf, msg)
			u.depth.Store(int64(len(u.buf)))
			u.mu.Unlock()
			u.sub.errCnt.Add(1)
			return true
		case DropNewest:
			u.mu.Unlock()
			u.sub.errCnt.Add(1)
			return false
		}
	}

	u.buf = append(u.buf, msg)
	currentDepth := int64(len(u.buf))
	u.depth.Store(currentDepth)
	u.mu.Unlock()

	// Update peak (lock-free CAS loop)
	for {
		oldPeak := u.peak.Load()
		if currentDepth <= oldPeak || u.peak.CompareAndSwap(oldPeak, currentDepth) {
			break
		}
	}

	// Slow consumer detection
	threshold := u.config.SlowConsumerThreshold
	if threshold > 0 && currentDepth >= int64(threshold) {
		if u.config.OnSlowConsumer != nil && !u.slowConsumerTriggered.Load() {
			u.slowConsumerTriggered.Store(true)
			u.config.OnSlowConsumer(u.sub, int(currentDepth))
		}
	}

	// Non-blocking signal to drain goroutine
	select {
	case u.notifyCh <- struct{}{}:
	default:
	}

	return true
}

// drain is the background goroutine that moves messages from buf to sub.Ch.
func (u *unboundedState[T]) drain() {
	for {
		// Wait for notification or close
		_, ok := <-u.notifyCh
		if !ok {
			return
		}

		// Drain all available messages in batches to reduce lock contention.
		for {
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

			// Send each message to the consumer (blocking — backpressure is intentional)
			for _, msg := range batch {
				u.sub.Ch <- msg
			}
		}
	}
}

// close stops the drain goroutine and releases resources.
func (u *unboundedState[T]) close() {
	if u.closed.CompareAndSwap(false, true) {
		close(u.notifyCh)
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
		DropPolicy:            DropNewest,
	}
	if len(config) > 0 {
		cfg = config[0]
	}

	sub := &Subscription[T]{
		Prefix: topic,
		Ch:     make(chan T, 1),
	}

	state := &unboundedState[T]{
		notifyCh: make(chan struct{}, 1),
		config:   cfg,
		sub:      sub,
	}

	sub.unbounded = state

	// Start drain goroutine
	go state.drain()

	b.subs = append(b.subs, sub)
	return sub
}
