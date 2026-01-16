package database

import (
	"database/sql"
	"testing"
	"time"

	"github.com/censys/scan-takehome/pkg/scanning"
	_ "github.com/mattn/go-sqlite3"
)

func setupTestDB(t *testing.T) *SQLiteWriter {
	testDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}

	// Create the schema
	_, err = testDB.Exec(`
        CREATE TABLE IF NOT EXISTS scans (
            Ip          TEXT,
            Port        INTEGER,
            Service     TEXT,
            Timestamp   INTEGER,
            DataVersion INTEGER,
            Data        TEXT,
            PRIMARY KEY (Ip, Port, Service)
        );
    `)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	writer := &SQLiteWriter{db: testDB}
	return writer
}

var scan = scanning.Scan{
	Ip:          "192.0.0.1",
	Port:        8080,
	Service:     "http",
	Timestamp:   time.Now().Unix(),
	DataVersion: scanning.V1,
}

func TestSQLiteInsertAndUpdate(t *testing.T) {
	// Test Insert
	t.Run("Insert", func(t *testing.T) {
		writer := setupTestDB(t)
		defer writer.db.Close()

		err := writer.WriteScan(scan.Ip, scan.Port, scan.Service, scan.Timestamp, scan.DataVersion, "initial data")
		if err != nil {
			t.Fatalf("Failed to insert scan: %v", err)
		}

		// Verify insertion
		var data string
		err = writer.db.QueryRow("SELECT Data FROM scans WHERE Ip = ? AND Port = ? AND Service = ?",
			scan.Ip, scan.Port, scan.Service).Scan(&data)
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		if data != "initial data" {
			t.Errorf("Expected 'initial data', got '%s'", data)
		}
	})

	// Test Update with newer timestamp
	t.Run("UpdateNewerTimestamp", func(t *testing.T) {
		writer := setupTestDB(t)
		defer writer.db.Close()

		writer.WriteScan(scan.Ip, scan.Port, scan.Service, 1000, 1, "old data")
		err := writer.WriteScan(scan.Ip, scan.Port, scan.Service, 2000, 2, "new data")
		if err != nil {
			t.Fatalf("Failed to update scan: %v", err)
		}

		var data string
		writer.db.QueryRow("SELECT Data FROM scans WHERE Ip = ? AND Port = ? AND Service = ?",
			scan.Ip, scan.Port, scan.Service).Scan(&data)
		if data != "new data" {
			t.Errorf("Expected 'new data', got '%s'", data)
		}
	})

	// Test Update with older timestamp
	t.Run("UpdateOlderTimestamp", func(t *testing.T) {
		writer := setupTestDB(t)
		defer writer.db.Close()

		writer.WriteScan(scan.Ip, scan.Port, scan.Service, 2000, 2, "new data")
		writer.WriteScan(scan.Ip, scan.Port, scan.Service, 1000, 1, "old data")

		var data string
		writer.db.QueryRow("SELECT Data FROM scans WHERE Ip = ? AND Port = ? AND Service = ?",
			scan.Ip, scan.Port, scan.Service).Scan(&data)
		if data != "new data" {
			t.Errorf("Expected 'new data' (should not update), got '%s'", data)
		}
	})
}
