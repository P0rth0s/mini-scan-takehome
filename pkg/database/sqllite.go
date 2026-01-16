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

var db *sql.DB

func Init() error {
	var err error
	dbPath := os.Getenv("SQLITE_DB_PATH")
	if dbPath == "" {
		dbPath = "/root/db/scans.db" // Default path
	}
	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	if err = db.Ping(); err != nil {
		return err
	}
	return nil
}

func WriteScan(ip string, port uint32, service string, timestamp int64, version int, data string) error {
	// Insert hardcoded values for now
	_, err := db.Exec(INSERT_SCAN_SQL,
		ip, port, service, timestamp, version, data)
	if err != nil {
		return err
	}
	return nil
}
