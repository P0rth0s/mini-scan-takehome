package database

import (
	"database/sql"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

const INSERT_SCAN_SQL = `
        INSERT INTO scans (Ip, Port, Service, Timestamp, DataVersion, Data) 
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(Ip, Port, Service) 
        DO UPDATE SET 
            Timestamp = excluded.Timestamp,
            DataVersion = excluded.DataVersion,
            Data = excluded.Data
        WHERE excluded.Timestamp > scans.Timestamp;
		`

// Interface for testing/mocking
type ScanWriter interface {
	WriteScan(ip string, port uint32, service string, timestamp int64, version int, data string) error
}

type MockWriter struct{}

func (m *MockWriter) WriteScan(ip string, port uint32, service string, timestamp int64, version int, data string) error {
	return nil
}

type SQLiteWriter struct {
	db *sql.DB
}

func (w *SQLiteWriter) Init() error {
	var err error
	dbPath := os.Getenv("SQLITE_DB_PATH")
	if dbPath == "" {
		dbPath = "/root/db/scans.db" // Default path
	}
	w.db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	if err = w.db.Ping(); err != nil {
		return err
	}
	return nil
}

func (w *SQLiteWriter) WriteScan(ip string, port uint32, service string, timestamp int64, version int, data string) error {
	// Insert hardcoded values for now
	_, err := w.db.Exec(INSERT_SCAN_SQL,
		ip, port, service, timestamp, version, data)
	if err != nil {
		return err
	}
	return nil
}
