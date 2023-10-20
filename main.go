package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

func main() {
	// Set the number of messages to send or receive for benchmarking
	messageCount := 10000
	runPublisherBenchmark := true
	runSubscriberBenchmark := false // there must be a separate publisher application publishing messages to the topic "subcriber-benchmark" for this to work

	fmt.Println("connecting to server...")

	conn, err := net.Dial("tcp", "127.0.0.1:1883")
	if err != nil {
		panic(err)
	}

	clientID := "test-client"
	messageReceivedChan := make(chan struct{}, messageCount)

	client := paho.NewClient(paho.ClientConfig{
		Conn:          conn,
		ClientID:      clientID,
		PacketTimeout: 10 * time.Minute,
		Router: paho.NewSingleHandlerRouter(func(packet *paho.Publish) {
			messageReceivedChan <- struct{}{}
		}),
	})

	// client.SetDebugLogger(log.Default())
	// client.SetErrorLogger(log.Default())

	fmt.Println("sending MQTT connect...")
	_, err = client.Connect(context.Background(), &paho.Connect{
		KeepAlive:  30,
		CleanStart: true,
		ClientID:   clientID,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("connected")

	subscriberBenchmark := func() time.Duration {
		_, err := client.Subscribe(context.Background(), &paho.Subscribe{
			Subscriptions: map[string]paho.SubscribeOptions{
				"subscriber-benchmark": {
					QoS: 1,
				},
			},
		})
		if err != nil {
			panic(err)
		}

		start := time.Now()

		// wait for all messages to be received
		for i := 0; i < messageCount; i++ {
			<-messageReceivedChan
		}

		elapsed := time.Since(start)
		return elapsed
	}

	publisherBenchmark := func() time.Duration {
		var wg sync.WaitGroup

		start := time.Now()
		for i := 0; i < messageCount; i++ {
			wg.Add(1)
			go func() {
				_, err := client.Publish(context.Background(), &paho.Publish{
					Topic:   "publisher-benchmark",
					Payload: []byte("Hello World!"),
					QoS:     1,
				})
				if err != nil {
					panic(err)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		return elapsed
	}

	if runPublisherBenchmark {
		duration := publisherBenchmark()
		fmt.Printf("sent %d messages in %d ms (%f messages/second)", messageCount, duration.Milliseconds(), float64(messageCount)/duration.Seconds())
	}
	if runSubscriberBenchmark {
		duration := subscriberBenchmark()
		fmt.Printf("received %d messages in %d ms (%f messages/second)", messageCount, duration.Milliseconds(), float64(messageCount)/duration.Seconds())
	}

	client.Disconnect(&paho.Disconnect{ReasonCode: 0})

}
