package main

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
	"fmt"
	"time"

	"github.com/pilotso11/channelbus"
)

// Just loop on the channel untilI get the kill signal, publish a message ever 500 ms
func publisher(stop chan bool, bus *channelbus.ChannelBus[string]) {
	cnt := 0
	for {
		select {
		case <-stop:
			return
		default:
			bus.Publish("my_interest", fmt.Sprintf("message %d", cnt))
			cnt++
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// Just loop on the channel untilI get the kill signal, print every message I receive
func listener(stop chan bool, subscription *channelbus.Subscription[string]) {
	for {
		select {
		case <-stop:
			return
		case msg := <-subscription.Ch:
			fmt.Printf("Received: %s\n", msg)
		}
	}
}

func main() {

	// Setup publisher and subscriber
	bus := channelbus.NewChannelBus[string]()
	sub1 := bus.Subscribe("my_interest")
	sub2 := bus.Subscribe("my_interest")
	stopPub := make(chan bool)
	stopSub1 := make(chan bool)
	stopSub2 := make(chan bool)
	go listener(stopSub1, sub1)
	go listener(stopSub2, sub2)
	go publisher(stopPub, bus)

	// Wait for a bit
	time.Sleep(10 * time.Second)

	// Be a good go citizen and cleanup
	stopPub <- true
	stopSub1 <- true
	stopSub2 <- true
}
