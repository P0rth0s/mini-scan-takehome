CREATE TABLE IF NOT EXISTS scans (
    Ip          string      `json:"ip"`
	Port        uint32      `json:"port"`
	Service     string      `json:"service"`
	Timestamp   int64       `json:"timestamp"`
	DataVersion int         `json:"data_version"`
	Data        interface{} `json:"data"`

    PRIMARY KEY (Ip, Port, Service)
);