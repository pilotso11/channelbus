# GOLANG Internal message bus using channels
ChannelBus is a minimal message bus/event bus implementation in GO for
internal communication within a project.  It's influenced by the 
java [guava EventBus](https://github.com/google/guava/wiki/EventBusExplained) as
a useful component for manging notifications within a single component.

The ChannelBus implementation is zero-allocation and uses buffered channels for non-blocking distribution 
as opposed to using in-line blocking callbacks.  Making it asynchronous by default is nice convenience,
but does come with one caveat: the buffered channels have a limit.  If the limit is reached messages 
will be dropped to preserve the non-blocking behavior.   ChanelBus does keep a count of dropped messages
allowing slow consumers to decide what to do.

ChannelBus is implemented using generics so requires go1.18.  This makes nice readable code without the need to
typecast from interface{}. It would be trivial to create an alternative version using interface{} instead.

# Installation
```shell
go get gitub.com/pilotso11/channelbus
```
The only dependencies are for the unit tests, `github.com/stretchr/testify/assert`.

# Usage
See the sample in [/example/example.go](/example/example.go) for a more complete example.

```go
package main

import (
	fmt
	time
	
	"github.com/pilotso11/channelbus"
)

func main() {

	// Setup publisher and subscriber
	bus := channelbus.NewChannelBus[string]()
	sub1 := bus.Subscribe("my_interest")
	sub2 := bus.Subscribe("my_interest")

	// Receive something on both channels
	go func() {
		msg1 := <- sub1.Ch
		fmt.Printf("On sub1: %s", msg1)
		
    }()
	go func() {
		msg2 := <- sub2.Ch
		fmt.Printf("On sub2: %s", msg2)
    }

	// Send something
	bus.Publish("my_interest.data", "sample message")

	// give the goroutines time to work
    time.Sleep(1 * time.Second) 
}
```


# Unbounded Subscriptions

For consumers that must never miss a message, `SubscribeUnbounded` provides an internal
growing buffer between the publisher and the subscription channel. Messages are never dropped
due to a full channel buffer (unless a hard limit is configured).

```go
bus := channelbus.NewChannelBus[string]()

// Basic unbounded subscription (default slow consumer threshold: 1000)
sub := bus.SubscribeUnbounded("events.")

// With configuration
sub := bus.SubscribeUnbounded("events.", channelbus.UnboundedConfig[string]{
    SlowConsumerThreshold: 500,        // warn when buffer exceeds 500
    MaxBufferSize:         10000,      // limit enqueue buffer size (0 = no limit)
    DropPolicy:            channelbus.DropOldest,  // or DropNewest (default)
    OnSlowConsumer: func(s *channelbus.Subscription[string], depth int) {
        log.Printf("slow consumer detected: buffer depth %d", depth)
    },
})

// Monitor buffer health
fmt.Println(sub.BufferDepth())      // current internal buffer size
fmt.Println(sub.PeakBufferDepth())  // highest buffer depth ever observed
fmt.Println(sub.IsUnbounded())      // true

// Cleanup (also called automatically by Unsubscribe)
sub.Close()
```

### Safety features
- **Slow consumer detection**: Configurable threshold with callback when buffer depth crosses it.
  The callback re-arms once the buffer is fully drained, firing at most once per slow episode.
- **Buffer size limit**: Optional `MaxBufferSize` limits the internal enqueue buffer. When reached,
  `DropPolicy` determines whether the oldest or newest message is discarded. Note: the drain
  goroutine processes messages in batches, so total in-flight messages (enqueue buffer + batch
  being delivered) may briefly exceed `MaxBufferSize`. Dropped messages increment `ErrCnt()`.
- **Observable state**: `BufferDepth()` and `PeakBufferDepth()` for monitoring/metrics.
- **Clean shutdown**: `Close()` stops the internal drain goroutine. Called automatically by `Unsubscribe`.

### When to use unbounded vs bounded
- **Bounded** (`Subscribe`): Best for fire-and-forget notifications where occasional drops are acceptable
  (e.g., UI refresh hints, metric events).
- **Unbounded** (`SubscribeUnbounded`): Use when every message must be delivered (e.g., SSE streaming,
  audit logs, command queues). Always configure `MaxBufferSize` in production to prevent runaway memory growth.

# Benchmarks
Performance scales close to linearly to two factors:
* A small increment with the total number of subscribers (i.e. subscriptions to check).  This is the string compares.
* A larger increment with the number of subscribers actual being sent to.  This is the time to write to the channel.

goos: windows
goarch: amd64
pkg: github.com/pilotso11/channelbus
cpu: AMD Ryzen 5 3600X 6-Core Processor

| Benchmark                                   | Iterations |        Time |  Bytes |      Allocs |
|---------------------------------------------|-----------:|------------:|-------:|------------:|
| Benchmark1Pub1Sub_PublishOnlyTime-12        |   40432107 | 29.25 ns/op | 0 B/op | 0 allocs/op |
| Benchmark1Pub1Sub_ToAsyncRcvTime-12         |   39405501 | 33.09 ns/op | 0 B/op | 0 allocs/op |
| Benchmark1Pub10SubToAll_PublishOnlyTime-12  |    1334673 |  1637 ns/op | 0 B/op | 0 allocs/op |
| Benchmark1Pub10SubToSome_PublishOnlyTime-12 |    1796287 | 684.3 ns/op | 0 B/op | 0 allocs/op |
| Benchmark1Pub20SubToAll_PublishOnlyTime-12  |     352429 |  4174 ns/op | 0 B/op | 0 allocs/op |
| Benchmark1Pub20SubToSome_PublishOnlyTime-12 |    1000000 |  1131 ns/op | 0 B/op | 0 allocs/op |
| Benchmark1Pub20SubToSome_ToAsyncRcvTime-12  |    1072503 |  1205 ns/op | 0 B/op | 0 allocs/op |
| Benchmark1Pub20SubTo1_PublishOnlyTime-12    |    3370746 | 370.6 ns/op | 0 B/op | 0 allocs/op |
| Benchmark1Pub20SubTo1_ToAsyncRcvTime-12     |    3634978 | 355.0 ns/op | 0 B/op | 0 allocs/op |
