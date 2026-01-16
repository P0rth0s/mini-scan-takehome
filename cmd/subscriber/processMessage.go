package main

import (
	"encoding/json"
	"errors"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/database"
	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/go-playground/validator/v10"
)

func processMessage(m *pubsub.Message, writer database.ScanWriter) error {
	var scan scanning.Scan
	if err := json.Unmarshal(m.Data, &scan); err != nil {
		log.Printf("json.Unmarshal scan: %v", err)
		return err
	}

	validate := validator.New()
	if err := validate.Struct(scan); err != nil {
		log.Printf("validation error for scan: %v", err)
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
		if err := validate.Struct(data); err != nil {
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
		if err := validate.Struct(data); err != nil {
			return err
		}
		log.Printf("V2 Data: %+v", data)
		dataString = data.ResponseStr
	default:
		return errors.New("unknown data version")
	}
	err = writer.WriteScan(scan.Ip, scan.Port, scan.Service, scan.Timestamp, scan.DataVersion, dataString)
	if err != nil {
		// In a realistic scenario, probably want to have retries for transient errors
		return err
	}
	log.Printf("Wrote scan to database for ip: %s port: %d service: %s timestamp: %d dataVersion: %d data: %s", scan.Ip, scan.Port, scan.Service, scan.Timestamp, scan.DataVersion, dataString)
	return nil
}
