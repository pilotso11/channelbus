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
	"bufio"
	"sync"
	"sync/atomic"
)

// Subscription holds the subscription details.  Use it to listen on Ch and unsubscribe.
// ErrCnt can be used to detect the presence of dropped messages.
type Subscription[T any] struct {
	// Prefix for selecting messages
	Prefix string
	// The channel to listen on
	Ch chan T
	// Count of errors received on the channel
	errCnt atomic.Int64
	// The most recent error on the channel
	err atomic.Value

	// unbounded holds the internal state for unbounded subscriptions (nil for bounded).
	unbounded *unboundedState[T]
}

// ErrCnt is the count of errors received on the channel
func (s *Subscription[T]) ErrCnt() int64 {
	return s.errCnt.Load()
}

// Err is the most recent error on the channel
func (s *Subscription[T]) Err() (err error) {
	val := s.err.Load()
	if val != nil {
		return val.(error)
	}
	return
}

// BufferDepth returns the current buffer depth.
// For bounded subscriptions this is len(Ch).
// For unbounded subscriptions this is the internal buffer depth
// (messages waiting to be drained into Ch).
func (s *Subscription[T]) BufferDepth() int {
	if s.unbounded == nil {
		return len(s.Ch)
	}
	return int(s.unbounded.depth.Load())
}

// PeakBufferDepth returns the highest internal buffer depth ever observed.
// Only tracked for unbounded subscriptions; returns 0 for bounded
// (tracking peak for bounded would add overhead to the zero-allocation publish path).
func (s *Subscription[T]) PeakBufferDepth() int {
	if s.unbounded == nil {
		return 0
	}
	return int(s.unbounded.peak.Load())
}

// IsUnbounded returns true if this subscription uses an unbounded internal buffer.
func (s *Subscription[T]) IsUnbounded() bool {
	return s.unbounded != nil
}

// Close stops the internal drain goroutine for unbounded subscriptions and
// waits for it to exit. After Close returns, no new publishes are accepted
// and no further sends to Ch will occur. Already-buffered messages that have
// not been drained are discarded. For bounded subscriptions this is a no-op.
// Close is idempotent.
func (s *Subscription[T]) Close() {
	if s.unbounded != nil {
		s.unbounded.close()
	}
}

type ChannelBus[T any] struct {
	lock sync.RWMutex
	subs []*Subscription[T]
}

// NewChannelBus creates a new empty message bus.
func NewChannelBus[T any]() *ChannelBus[T] {
	return &ChannelBus[T]{}
}

// Subscribe adds a new subscription for topic and type T.
// It returns the subscription which contains the channel to select on.
// The default buffer size in Ch is 100.  An optional bufferSize can be passed in.
// Because non-blocking publish to the channel is used, setting the buffer size to be small
// can mean messages will be dropped if the consumer is slower than the fastest pace of arriving events.
func (b *ChannelBus[T]) Subscribe(topic string, bufferSize ...int) *Subscription[T] {
	b.lock.Lock()
	defer b.lock.Unlock()

	// Default buffer size is 100
	bufSize := 100
	if len(bufferSize) > 0 && bufferSize[0] > 0 {
		bufSize = bufferSize[0]
	}
	// Create the new subscription, with a buffer size of 100
	sub := &Subscription[T]{
		Prefix: topic,
		Ch:     make(chan T, bufSize),
	}
	// Save it in the list
	b.subs = append(b.subs, sub)

	return sub
}

// Unsubscribe removes an existing subscription.  It returns true if a subscription was found and removed.
// For unbounded subscriptions, this also stops the internal drain goroutine.
func (b *ChannelBus[T]) Unsubscribe(subscription *Subscription[T]) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	// Remove the subscription from the list
	newSubs := remove(b.subs, subscription)
	if len(newSubs) < len(b.subs) {
		b.subs = newSubs
		subscription.Close()
		return true
	}
	return false
}

// Publish a message of type T to all subscribers channels whose Prefix matches topic.
// Non-blocking channel write is used so if no one is actually waiting on the channel
func (b *ChannelBus[T]) Publish(topic string, msg T) (n int) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	// Loop over all the subscriptions
	for _, sub := range b.subs {
		// Prefix match the topic
		if match(topic, sub.Prefix) {
			if sub.unbounded != nil {
				// Unbounded: route through internal buffer
				if sub.unbounded.enqueue(msg) {
					n++
				}
			} else {
				// Bounded: non-blocking send directly to channel
				select {
				case sub.Ch <- msg:
					n++ // increment number sent
				default:
					// Channel is either not being listened to or the buffer is full, record the error and continue
					sub.errCnt.Add(1)
					sub.err.Store(bufio.ErrBufferFull)
				}
			}
		}
	}
	return
}

func match(topic string, prefix string) bool {
	if len(topic) < len(prefix) {
		return false
	}
	return prefix == topic[:len(prefix)]
}

func remove[T comparable](l []T, item T) []T {
	for i, other := range l {
		if other == item {
			return append(l[:i], l[i+1:]...)
		}
	}
	return l
}
