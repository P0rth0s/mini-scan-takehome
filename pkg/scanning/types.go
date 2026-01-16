package scanning

const (
	Version = iota
	V1
	V2
)

type Scan struct {
	Ip          string      `json:"ip" validate:"required"`
	Port        uint32      `json:"port" validate:"required"`
	Service     string      `json:"service" validate:"required"`
	Timestamp   int64       `json:"timestamp" validate:"required"`
	DataVersion int         `json:"data_version" validate:"required"`
	Data        interface{} `json:"data" validate:"required"`
}

type V1Data struct {
	ResponseBytesUtf8 []byte `json:"response_bytes_utf8" validate:"required"`
}

type V2Data struct {
	ResponseStr string `json:"response_str" validate:"required"`
}
