package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/database"
	"github.com/censys/scan-takehome/pkg/scanning"
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

	err = database.Init()
	if err != nil {
		log.Fatalf("database.Init: %v", err)
		return
	}

	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		// Sub receive automatically spawns goroutines for each message
		// We dont need wait groups because if the process doesnt finish the message will just be retried after the ack deadline
		log.Printf("Got message: %s", string(m.Data))
		err := processMessage(m)
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

func processMessage(m *pubsub.Message) error {
	var scan scanning.Scan
	if err := json.Unmarshal(m.Data, &scan); err != nil {
		log.Printf("json.Unmarshal scan: %v", err)
		return err
	}

	var dataString string

	jsonBytes, err := json.Marshal(scan.Data)
	if err != nil {
		return err
	}

	switch scan.DataVersion {
	case scanning.V1: // []byte
		var data scanning.V1Data
		if err := json.Unmarshal(jsonBytes, &data); err != nil {
			return err
		}
		str := string(data.ResponseBytesUtf8)
		log.Printf("V1 Data: %s", str)
		dataString = str
	case scanning.V2: // string
		var data scanning.V2Data
		if err := json.Unmarshal(jsonBytes, &data); err != nil {
			return err
		}
		log.Printf("V2 Data: %+v", data)
		dataString = data.ResponseStr
	default:
		return errors.New("unknown data version")
	}
	err = database.WriteScan(scan.Ip, scan.Port, scan.Service, scan.Timestamp, scan.DataVersion, dataString)
	if err != nil {
		// In a realistic scenario, probably want to have retries for transient errors
		return err
	}
	log.Printf("Wrote scan to database for ip: %s port: %d service: %s timestamp: %d dataVersion: %d data: %s", scan.Ip, scan.Port, scan.Service, scan.Timestamp, scan.DataVersion, dataString)
	return nil
}
