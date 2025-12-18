CREATE TABLE IF NOT EXISTS file_tracker (
    file_name TEXT,
    url TEXT,
    last_modified DATE,
    source_name TEXT,
    size_bytes BIGINT,
    date_discovered TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    downloaded BOOLEAN DEFAULT FALSE,
    date_downloaded TIMESTAMP NULL
)