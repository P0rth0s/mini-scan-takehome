package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/scanning"
)

func main() {
	projectId := flag.String("project", "test-project", "GCP Project ID")
	topicId := flag.String("topic", "scan-topic", "GCP PubSub Topic ID")
	subId := flag.String("sub", "scan-sub", "GCP PubSub Subscription ID")
	flag.Parse()

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, *projectId)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	topic := client.Topic(*topicId)
	sub, err := client.CreateSubscription(ctx, *subId, pubsub.SubscriptionConfig{Topic: topic, AckDeadline: 20 * time.Second})
	if err != nil {
		log.Printf("CreateSubscription: %v", err)
		sub = client.Subscription(*subId)
	}

	// TODO - Need to loop on this
	// TODO - Need to track processing and not shutdown till done
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		go processMessage(m)
		log.Printf("Got message: %s", string(m.Data))
		m.Ack()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("sub.Receive: %v", err)
	}
}

// blocks until an interrupt or termination signal is received,
// TODO do not shutdown till all processes complete. Do not consume new messages during shutdown.
func waitForExitSignal() context.Context {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	sig := <-sigChan
	log.Printf("Received exit signal: %v. Initiating graceful shutdown...", sig)

	// Create a context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	// Cleanup function to be called when shutdown completes
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("Shutdown deadline exceeded, forcing exit")
		} else {
			log.Printf("Graceful shutdown completed")
		}
		cancel()
	}()

	return ctx
}

func processMessage(m *pubsub.Message) {

	var scan scanning.Scan
	if err := json.Unmarshal(m.Data, &scan); err != nil {
		log.Printf("json.Unmarshal scan: %v", err)
		return
	}

	log.Printf("Parsed scan header: ip=%s port=%d service=%s data_version=%d", scan.Ip, scan.Port, scan.Service, scan.DataVersion)

	switch scan.DataVersion {
	case scanning.V1:
		// []byte
		var data scanning.V1Data
		dataBytes, ok := scan.Data.([]byte)
		if !ok {
			log.Printf("expected []byte for V1Data, got %T", scan.Data)
			return
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			log.Printf("json.Unmarshal scan: %v", err)
			return
		}
	case scanning.V2:
		// string
		var data scanning.V2Data
		dataStr, ok := scan.Data.(string)
		if !ok {
			log.Printf("expected string for V2Data, got %T", scan.Data)
			return
		}
		if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
			log.Printf("json.Unmarshal scan: %v", err)
			return
		}
	default:
		log.Printf("unknown data version: %d", scan.DataVersion)
	}
}
