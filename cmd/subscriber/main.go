package main

import (
	"context"
	"errors"
	"flag"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/database"
)

func main() {
	log.Printf("Starting subscriber...")

	projectId := flag.String("project", "test-project", "GCP Project ID")
	subId := flag.String("sub", "scan-sub", "GCP PubSub Subscription ID")
	flag.Parse()

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, *projectId)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
		return
	}
	sub := client.Subscription(*subId)

	writer := &database.SQLiteWriter{}
	err = writer.Init()
	if err != nil {
		log.Fatalf("database.Init: %v", err)
		return
	}

	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		// Sub receive automatically spawns goroutines for each message
		// We dont need wait groups because if the process doesnt finish the message will just be retried after the ack deadline
		log.Printf("Got message: %s", string(m.Data))
		err := processMessage(m, writer)
		if err != nil {
			log.Printf("ERROR processing message: %v", err)
			m.Nack()
			return
		}
		m.Ack()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("sub.Receive: %v", err)
		return
	}
}
