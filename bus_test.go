package channelbus

// MIT License
//
// Copyright (c) 2023 Seth Osher
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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestChannelBus_Subscribe(t *testing.T) {
	bus := NewChannelBus[string]()
	sub := bus.Subscribe("blue.")
	assert.Equal(t, 1, bus.Publish("blue.car", "test message"), "publish return value")
	msg := <-sub.Ch
	assert.Equal(t, "test message", msg)
	msg = ""
	select {
	case msg = <-sub.Ch:
	default:
	}
	assert.Equal(t, "", msg, "no second message is queued")
}

func TestChannelBus_Unsubscribe(t *testing.T) {
	bus := NewChannelBus[string]()
	sub := bus.Subscribe("blue.")
	bus.Publish("blue.car", "test message")
	msg := <-sub.Ch
	assert.Equal(t, "test message", msg)
	assert.True(t, bus.Unsubscribe(sub), "unsubscribe return value")
	assert.Equal(t, 0, bus.Publish("blue.car", "test message"), "no subscriber, 0 publishes")
	msg = ""
	select {
	case msg = <-sub.Ch:
	default:
	}
	assert.Equal(t, "", msg, "no second message is queued")
	assert.False(t, bus.Unsubscribe(sub), "unsubscribe second time return value")

}

func TestChannelBus_PrefixMatches(t *testing.T) {
	assert.NotPanics(t, func() {
		bus := NewChannelBus[string]()
		sub := bus.Subscribe("blue.")
		assert.Equal(t, 1, bus.Publish("blue.car", "test message 0"), "publish return value")
		assert.Equal(t, 0, bus.Publish("red.car", "test message 4"), "publish return value")
		assert.Equal(t, 1, bus.Publish("blue.van", "test message 1"), "publish return value")
		assert.Equal(t, 1, bus.Publish("blue.", "test message 2"), "publish return value")
		assert.Equal(t, 0, bus.Publish("blue", "test message 2"), "publish return value")
		assert.Equal(t, 0, bus.Publish("b", "test message 2"), "publish return value")
		assert.Equal(t, 0, bus.Publish("", "test message 2"), "publish return value")

		for i := 0; i < 3; i++ {
			msg := ""
			select {
			case msg = <-sub.Ch:
			default:
			}
			assert.Equal(t, fmt.Sprintf("test message %d", i), msg, "correct message received")
		}
	})
}

func TestChannelBus_MultiSubscribe(t *testing.T) {
	bus := NewChannelBus[string]()
	sub1 := bus.Subscribe("blue.")
	assert.NotNil(t, sub1)
	sub2 := bus.Subscribe("red.")
	assert.NotNil(t, sub2)
	sub3 := bus.Subscribe("blue.")
	assert.NotNil(t, sub3)
	assert.Equal(t, 2, bus.Publish("blue.car", "test message 1"), "publish return value")
	assert.Equal(t, 1, bus.Publish("red.car", "test message 2"), "publish return value")
	assert.Equal(t, 0, bus.Publish("yellow.car", "test message 3"), "publish return value")

}

func TestChannelBus_MultiSubscribeAndUnsubscribe(t *testing.T) {
	bus := NewChannelBus[string]()
	sub1 := bus.Subscribe("blue.")
	assert.NotNil(t, sub1)
	sub2 := bus.Subscribe("red.")
	assert.NotNil(t, sub2)
	sub3 := bus.Subscribe("blue.")
	assert.NotNil(t, sub3)
	assert.Equal(t, 2, bus.Publish("blue.car", "test message 1"), "publish return value")
	assert.Equal(t, 1, bus.Publish("red.car", "test message 2"), "publish return value")
	assert.Equal(t, 0, bus.Publish("yellow.car", "test message 3"), "publish return value")
	bus.Unsubscribe(sub1)
	assert.Equal(t, 1, bus.Publish("blue.van", "test message 4"), "blue1 unsubscribed")
	assert.Equal(t, 1, bus.Publish("red.van", "test message 5"), "red still subscribed")
	bus.Unsubscribe(sub2)
	assert.Equal(t, 0, bus.Publish("red.truck", "test message 6"), "red unsubscribed")
	assert.Equal(t, 1, bus.Publish("blue.truck", "test message 7"), "blue2 still subscribed")
	bus.Unsubscribe(sub3)
	assert.Equal(t, 0, bus.Publish("blue.fish", "test message 7"), "blue2 unsubscribed")
}

func TestChannelBus_NonBlockOnFullBuffer(t *testing.T) {
	bus := NewChannelBus[string]()
	sub1 := bus.Subscribe("blue.", 5)
	sub2 := bus.Subscribe("blue.", 20)
	assert.Equal(t, 5, cap(sub1.Ch), "channel buffer has correct size")

	assert.Eventually(t, func() bool {
		for i := 0; i < 10; i++ {
			bus.Publish("blue.car", "message")
		}
		return true
	}, 1*time.Second, 2*time.Millisecond, "we did not block")

	n := 0
	n2 := 0
	for i := 0; i < 10; i++ {
		select {
		case msg := <-sub1.Ch:
			n++
			assert.Equal(t, "message", msg)
		default:
		}

		select {
		case msg := <-sub2.Ch:
			n2++
			assert.Equal(t, "message", msg)
		default:
		}
	}
	assert.Equal(t, 5, n, "only receive 5 copies on sub1")
	assert.Equal(t, 10, n2, "all 10 on sub2")
	assert.Equal(t, 5, sub1.ErrCnt, "Err count is correct")
	assert.Equal(t, bufio.ErrBufferFull, sub1.Err)
	assert.Equal(t, 0, sub2.ErrCnt, "Err count is correct")
}

func addBenchSubscriber(bus *ChannelBus[string], topic string) (*Subscription[string], *atomic.Int64, chan bool) {
	sub := bus.Subscribe(topic, 5)
	kill := make(chan bool)
	rcvCnt := atomic.Int64{}
	go func() {
		for {
			select {
			case _ = <-sub.Ch:
				rcvCnt.Add(1)
			case k := <-kill:
				if k {
					return
				}
			}
		}
	}()
	return sub, &rcvCnt, kill
}

// 1 Publisher, 1 Subscriber - Publish only time, ignore receive
func Benchmark1Pub1Sub_PublishOnlyTime(b *testing.B) {
	subjects := []string{"blue"}
	testPubSub(b, 1, subjects, false)
}

// 1 Publisher, 1 Subscriber - Wait for receiver to be complete
func Benchmark1Pub1Sub_ToAsyncRcvTime(b *testing.B) {
	subjects := []string{"blue"}
	testPubSub(b, 1, subjects, true)
}

// 1 Publisher, 10 Subscriber, All messages to all - ignore receive
func Benchmark1Pub10SubToAll_PublishOnlyTime(b *testing.B) {
	subjects := []string{"blue"}
	testPubSub(b, 10, subjects, false)
}

// 1 Publisher, 10 Subscriber, Messages to 1/3 - ignore receive
func Benchmark1Pub10SubToSome_PublishOnlyTime(b *testing.B) {
	subjects := []string{"blue", "red", "yellow"}
	testPubSub(b, 10, subjects, false)
}

// 1 Publisher, 20 Subscriber, All messages to all - ignore receive
func Benchmark1Pub20SubToAll_PublishOnlyTime(b *testing.B) {
	subjects := []string{"blue"}
	testPubSub(b, 20, subjects, false)
}

// 1 Publisher, 20 Subscriber, Messages to 1/4 - ignore receive
func Benchmark1Pub20SubToSome_PublishOnlyTime(b *testing.B) {
	subjects := []string{"blue", "red", "yellow", "green"}
	testPubSub(b, 20, subjects, false)
}

// 1 Publisher, 20 Subscriber, Messages to 1/4 - Wait for receive to complete
func Benchmark1Pub20SubToSome_ToAsyncRcvTime(b *testing.B) {
	subjects := []string{"blue", "red", "yellow", "green"}
	testPubSub(b, 20, subjects, true)

}

// 1 Publisher, 20 Subscriber, Messages to 1 - ignore receive
func Benchmark1Pub20SubTo1_PublishOnlyTime(b *testing.B) {
	var subjects []string
	for i := 0; i < 20; i++ {
		subjects = append(subjects, fmt.Sprintf("topic_%03d", i))
	}
	testPubSub(b, 20, subjects, false)
}

// 1 Publisher, 20 Subscriber, Messages to 1 - Wait for receive to complete
func Benchmark1Pub20SubTo1_ToAsyncRcvTime(b *testing.B) {
	var subjects []string
	for i := 0; i < 20; i++ {
		subjects = append(subjects, fmt.Sprintf("topic_%03d", i))
	}
	testPubSub(b, 20, subjects, true)
}

// Core of the behcmark test
func testPubSub(b *testing.B, nSub int, subjects []string, waitAsync bool) {
	// Setup the publisher
	bus := NewChannelBus[string]()

	// Arrays to hold per subscriber details
	kills := make([]chan bool, nSub)            // stop channels
	counts := make([]*atomic.Int64, nSub)       // receive counts
	exp := make([]int, nSub)                    // expected counts
	subs := make([]*Subscription[string], nSub) // subscribers

	// Setup subscribers
	for i := 0; i < nSub; i++ {
		s := i % len(subjects)
		sub, cnt, kill := addBenchSubscriber(bus, subjects[s])
		kills[i] = kill
		counts[i] = cnt
		subs[i] = sub
	}

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		s := i % len(subjects)
		bus.Publish(subjects[s], "message")
	}

	// Are we waiting for the subscribers to complete?
	if waitAsync {
		// Update expected counts for each subscriber, this ignores the overrun from modulo len(subjects)
		for n := 0; n < nSub; n++ {
			exp[n] = b.N / len(subjects)
		}

		// Don't wait forever!
		assert.Eventually(b, func() bool {
			done := true
			for i, val := range exp {
				// Check for expected still > act + errors  - with this rate of publication there may be some drops
				if int64(val) > counts[i].Load()+int64(subs[i].ErrCnt) {
					done = false
				}
			}
			return done
		}, 10*time.Second, 2*time.Millisecond)
	}

	// cleanup
	for _, k := range kills {
		k <- true // stop the goroutine
	}

	b.ReportAllocs()
}
