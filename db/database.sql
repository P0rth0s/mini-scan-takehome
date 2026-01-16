CREATE TABLE IF NOT EXISTS scans (
    Ip          TEXT,
    Port        INTEGER,
    Service     TEXT,
    Timestamp   INTEGER,
    DataVersion INTEGER,
    Data        TEXT,

    PRIMARY KEY (Ip, Port, Service)
);