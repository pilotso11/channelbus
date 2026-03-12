package channelbus

// MIT License
//
// Copyright (c) 2023-2026 Seth Osher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// drainChan reads all available messages from ch until no message arrives within timeout.
func drainChan[T any](ch <-chan T, timeout time.Duration) []T {
	var result []T
	for {
		select {
		case msg := <-ch:
			result = append(result, msg)
		case <-time.After(timeout):
			return result
		}
	}
}

func TestSubscribeUnbounded_BasicDelivery(t *testing.T) {
	bus := NewChannelBus[string]()
	sub := bus.SubscribeUnbounded("test.")
	defer bus.Unsubscribe(sub)

	assert.True(t, sub.IsUnbounded())

	n := bus.Publish("test.hello", "message1")
	assert.Equal(t, 1, n)

	msg := <-sub.Ch
	assert.Equal(t, "message1", msg)
}

func TestSubscribeUnbounded_NoDrops(t *testing.T) {
	bus := NewChannelBus[string]()
	// Use a very small channel buffer (1) to prove the internal buffer handles overflow
	sub := bus.SubscribeUnbounded("test.")
	defer bus.Unsubscribe(sub)

	// Publish many messages without reading — they should all be buffered
	const count = 500
	for i := 0; i < count; i++ {
		n := bus.Publish("test.msg", "hello")
		assert.Equal(t, 1, n, "message %d should be accepted", i)
	}

	// Now read them all back
	for i := 0; i < count; i++ {
		select {
		case msg := <-sub.Ch:
			assert.Equal(t, "hello", msg)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}

	assert.EqualValues(t, 0, sub.ErrCnt(), "no errors expected")
}

func TestSubscribeUnbounded_MixedWithBounded(t *testing.T) {
	bus := NewChannelBus[string]()
	bounded := bus.Subscribe("test.", 5)
	unbounded := bus.SubscribeUnbounded("test.")
	defer bus.Unsubscribe(bounded)
	defer bus.Unsubscribe(unbounded)

	assert.False(t, bounded.IsUnbounded())
	assert.True(t, unbounded.IsUnbounded())

	// Publish more than the bounded buffer can hold
	for i := 0; i < 20; i++ {
		bus.Publish("test.msg", "hello")
	}

	// Bounded should have dropped some
	boundedCount := 0
drain:
	for {
		select {
		case <-bounded.Ch:
			boundedCount++
		default:
			break drain
		}
	}
	assert.Equal(t, 5, boundedCount, "bounded should only have 5")
	assert.EqualValues(t, 15, bounded.ErrCnt(), "bounded should have 15 drops")

	// Unbounded should have all 20
	for i := 0; i < 20; i++ {
		select {
		case msg := <-unbounded.Ch:
			assert.Equal(t, "hello", msg)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for unbounded message %d", i)
		}
	}
	assert.EqualValues(t, 0, unbounded.ErrCnt(), "unbounded should have no drops")
}

func TestSubscribeUnbounded_SlowConsumerCallback(t *testing.T) {
	var callbackDepth atomic.Int64
	var callbackCount atomic.Int64

	bus := NewChannelBus[string]()
	sub := bus.SubscribeUnbounded("test.", UnboundedConfig[string]{
		SlowConsumerThreshold: 10,
		OnSlowConsumer: func(s *Subscription[string], depth int) {
			callbackDepth.Store(int64(depth))
			callbackCount.Add(1)
		},
	})
	defer bus.Unsubscribe(sub)

	// Publish a large burst without reading. The drain goroutine blocks on Ch
	// after sending 1 message (Ch buffer=1), so the internal buffer grows.
	// Use a large count to guarantee the threshold is exceeded regardless of scheduling.
	const count = 500
	for i := 0; i < count; i++ {
		bus.Publish("test.msg", "hello")
	}

	assert.Eventually(t, func() bool {
		return callbackCount.Load() >= 1
	}, 1*time.Second, 5*time.Millisecond, "slow consumer callback should fire")
	assert.GreaterOrEqual(t, int(callbackDepth.Load()), 10, "callback should report depth >= threshold")

	// Drain all messages to unblock the drain goroutine and re-arm the flag
	for i := 0; i < count; i++ {
		select {
		case <-sub.Ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out draining at message %d", i)
		}
	}

	// Wait for buffer to fully empty (re-arms the slow consumer flag)
	assert.Eventually(t, func() bool {
		return sub.BufferDepth() == 0
	}, 1*time.Second, 5*time.Millisecond, "buffer should drain")

	// Allow the drain goroutine to fully settle and reset the flag
	time.Sleep(50 * time.Millisecond)

	// Publish again — callback should re-fire
	prevCount := callbackCount.Load()
	for i := 0; i < count; i++ {
		bus.Publish("test.msg", "hello2")
	}

	assert.Eventually(t, func() bool {
		return callbackCount.Load() > prevCount
	}, 1*time.Second, 5*time.Millisecond, "slow consumer callback should re-fire after reset")
}

func TestSubscribeUnbounded_MaxBufferSize_DropNewest(t *testing.T) {
	bus := NewChannelBus[int]()
	sub := bus.SubscribeUnbounded("test.", UnboundedConfig[int]{
		MaxBufferSize:         10,
		DropPolicy:            DropNewest,
		SlowConsumerThreshold: -1, // disable
	})
	defer bus.Unsubscribe(sub)

	// Publish more than MaxBufferSize
	for i := 0; i < 20; i++ {
		bus.Publish("test.msg", i)
	}

	// Should have errCnt for dropped messages
	assert.Eventually(t, func() bool {
		return sub.ErrCnt() > 0
	}, 1*time.Second, 5*time.Millisecond, "should have drops")

	// Read what we can — should be first 10 (plus maybe 1 in Ch buffer)
	received := drainChan(sub.Ch, 500*time.Millisecond)
	// The first messages should be the earliest ones (0, 1, 2, ...)
	require.NotEmpty(t, received)
	assert.Equal(t, 0, received[0], "first received should be oldest message")
}

func TestSubscribeUnbounded_MaxBufferSize_DropOldest(t *testing.T) {
	bus := NewChannelBus[int]()
	sub := bus.SubscribeUnbounded("test.", UnboundedConfig[int]{
		MaxBufferSize:         10,
		DropPolicy:            DropOldest,
		SlowConsumerThreshold: -1, // disable
	})
	defer bus.Unsubscribe(sub)

	// Publish more than MaxBufferSize
	for i := 0; i < 20; i++ {
		bus.Publish("test.msg", i)
	}

	// Should have errCnt for dropped messages
	assert.Eventually(t, func() bool {
		return sub.ErrCnt() > 0
	}, 1*time.Second, 5*time.Millisecond, "should have drops")

	// Read all — the newest messages should be present
	received := drainChan(sub.Ch, 500*time.Millisecond)
	require.NotEmpty(t, received)
	// Last received should be 19 (the newest)
	assert.Equal(t, 19, received[len(received)-1], "last received should be newest message")
}

func TestSubscribeUnbounded_BufferDepthAndPeak(t *testing.T) {
	bus := NewChannelBus[string]()
	sub := bus.SubscribeUnbounded("test.", UnboundedConfig[string]{
		SlowConsumerThreshold: -1, // disable
	})
	defer bus.Unsubscribe(sub)

	assert.Equal(t, 0, sub.BufferDepth())
	assert.Equal(t, 0, sub.PeakBufferDepth())

	// Publish a large burst without reading. The drain goroutine blocks on Ch
	// after sending at most 1 message (Ch buffer=1), so messages accumulate.
	// Peak is recorded in enqueue() before the batch drain grabs them.
	const count = 500
	for i := 0; i < count; i++ {
		bus.Publish("test.msg", "hello")
	}

	// Peak should be > 0 (some messages buffered before drain catches up)
	assert.Eventually(t, func() bool {
		return sub.PeakBufferDepth() > 0
	}, 1*time.Second, 5*time.Millisecond)

	peak := sub.PeakBufferDepth()
	t.Logf("peak buffer depth: %d", peak)

	// Drain all
	for i := 0; i < count; i++ {
		select {
		case <-sub.Ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out at message %d", i)
		}
	}

	// Depth should be 0, but peak should remain at its high-water mark
	assert.Eventually(t, func() bool {
		return sub.BufferDepth() == 0
	}, 1*time.Second, 5*time.Millisecond)
	assert.Equal(t, peak, sub.PeakBufferDepth(), "peak should not decrease after drain")
}

func TestSubscribeUnbounded_BoundedBufferDepthIsLenCh(t *testing.T) {
	bus := NewChannelBus[string]()
	sub := bus.Subscribe("test.", 10)
	defer bus.Unsubscribe(sub)

	assert.False(t, sub.IsUnbounded())
	assert.Equal(t, 0, sub.BufferDepth())

	bus.Publish("test.msg", "a")
	bus.Publish("test.msg", "b")
	assert.Equal(t, 2, sub.BufferDepth())

	<-sub.Ch
	assert.Equal(t, 1, sub.BufferDepth())

	assert.Equal(t, 0, sub.PeakBufferDepth()) // peak only tracked for unbounded
}

func TestSubscribeUnbounded_UnsubscribeStopsDrain(t *testing.T) {
	bus := NewChannelBus[string]()
	sub := bus.SubscribeUnbounded("test.")

	// Publish a message
	bus.Publish("test.msg", "hello")
	<-sub.Ch

	// Unsubscribe should stop the drain goroutine
	assert.True(t, bus.Unsubscribe(sub))

	// Publishing should no longer reach this subscription
	n := bus.Publish("test.msg", "after-unsub")
	assert.Equal(t, 0, n)
}

func TestSubscribeUnbounded_CloseIsIdempotent(t *testing.T) {
	bus := NewChannelBus[string]()
	sub := bus.SubscribeUnbounded("test.")

	sub.Close()
	sub.Close() // should not panic
	bus.Unsubscribe(sub)
}

func TestSubscribeUnbounded_CloseUnblocksStuckDrain(t *testing.T) {
	bus := NewChannelBus[string]()
	sub := bus.SubscribeUnbounded("test.")

	// Fill the Ch buffer (1) and leave messages in the internal buffer.
	// The drain goroutine will be blocked trying to send the 2nd message.
	bus.Publish("test.msg", "msg1")
	bus.Publish("test.msg", "msg2")
	bus.Publish("test.msg", "msg3")

	// Give drain goroutine time to block on the Ch send
	time.Sleep(50 * time.Millisecond)

	// Close should cancel the context and unblock the drain goroutine
	done := make(chan struct{})
	go func() {
		sub.Close()
		close(done)
	}()

	select {
	case <-done:
		// Good — Close returned promptly
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked — drain goroutine did not exit")
	}
}

func TestSubscribeUnbounded_ConcurrentPublish(t *testing.T) {
	bus := NewChannelBus[int]()
	sub := bus.SubscribeUnbounded("test.")
	defer bus.Unsubscribe(sub)

	const numGoroutines = 10
	const msgsPerGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < msgsPerGoroutine; i++ {
				bus.Publish("test.msg", i)
			}
		}()
	}
	wg.Wait()

	// Should receive all messages
	total := numGoroutines * msgsPerGoroutine
	for i := 0; i < total; i++ {
		select {
		case <-sub.Ch:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for message %d/%d", i, total)
		}
	}

	assert.EqualValues(t, 0, sub.ErrCnt())
}

func TestSubscribeUnbounded_DefaultConfig(t *testing.T) {
	bus := NewChannelBus[string]()
	// No config provided — should use defaults
	sub := bus.SubscribeUnbounded("test.")
	defer bus.Unsubscribe(sub)

	bus.Publish("test.msg", "hello")
	msg := <-sub.Ch
	assert.Equal(t, "hello", msg)
}

func TestSubscribeUnbounded_PrefixMatching(t *testing.T) {
	bus := NewChannelBus[string]()
	sub := bus.SubscribeUnbounded("blue.")
	defer bus.Unsubscribe(sub)

	assert.Equal(t, 1, bus.Publish("blue.car", "yes"))
	assert.Equal(t, 0, bus.Publish("red.car", "no"))
	assert.Equal(t, 1, bus.Publish("blue.van", "yes2"))

	msg1 := <-sub.Ch
	assert.Equal(t, "yes", msg1)
	msg2 := <-sub.Ch
	assert.Equal(t, "yes2", msg2)
}
