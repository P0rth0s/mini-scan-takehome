package main

import (
	"encoding/json"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/database"
	"github.com/censys/scan-takehome/pkg/scanning"
)

func TestProcessMessage(t *testing.T) {
	mockWriter := &database.MockWriter{}
	// Process message type 1
	t.Run("MessageType1", func(t *testing.T) {
		var messageType1 = &pubsub.Message{
			Data: func() []byte {
				scan := scanning.Scan{
					Ip:          "192.168.1.1",
					Port:        80,
					Service:     "http",
					Timestamp:   1234567890,
					DataVersion: scanning.V1,
					Data: scanning.V1Data{
						ResponseBytesUtf8: []byte("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html>Hello</html>"),
					},
				}
				data, _ := json.Marshal(scan)
				return data
			}(),
		}
		err := processMessage(messageType1, mockWriter)
		if err != nil {
			t.Errorf("Expected no error for V1 message, got: %v", err)
		}
	})
	// Process message type 2
	t.Run("MessageType2", func(t *testing.T) {
		var messageType2 = &pubsub.Message{
			Data: func() []byte {
				scan := scanning.Scan{
					Ip:          "192.168.1.2",
					Port:        443,
					Service:     "https",
					Timestamp:   1234567891,
					DataVersion: scanning.V2,
					Data: scanning.V2Data{
						ResponseStr: "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ok\"}",
					},
				}
				data, _ := json.Marshal(scan)
				return data
			}(),
		}
		err := processMessage(messageType2, mockWriter)
		if err != nil {
			t.Errorf("Expected no error for V2 message, got: %v", err)
		}
	})
	// Process invalid message
	t.Run("InvalidMessage", func(t *testing.T) {
		var invalidMessage = &pubsub.Message{
			Data: []byte(`{"invalid": "json structure"`),
		}
		err := processMessage(invalidMessage, mockWriter)
		if err == nil {
			t.Errorf("Expected error for invalid message, got nil")
		}
	})
}
