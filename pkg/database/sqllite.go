package database

/*
import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/censys/scan-takehome/pkg/scanning"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

func init() {

}

func Write(scan scanning.Scan) error {
	if db == nil {
		return fmt.Errorf("database not initialized")
	}
	b, err := json.Marshal(scan.Data)
	if err != nil {
		return fmt.Errorf("marshal scan data: %w", err)
	}

	_, err = db.Exec(`INSERT INTO scans (ip, port, service, timestamp, data_version, data) VALUES (?, ?, ?, ?, ?, ?)`,
		scan.Ip, scan.Port, scan.Service, scan.Timestamp, scan.DataVersion, b)
	if err != nil {
		return fmt.Errorf("insert scan: %w", err)
	}
	return nil
}
*/
