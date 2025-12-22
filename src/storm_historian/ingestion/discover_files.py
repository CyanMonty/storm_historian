from dataclasses import dataclass
from datetime import datetime
import requests
import yaml
import polars as pl
from pathlib import Path
from bs4 import BeautifulSoup
import duckdb

from storm_historian.ingestion.source_model import SourceModel

@dataclass(frozen=True)
class FileRow:
    file_name: str
    source: str
    url: str
    modified: datetime
    size_bytes: int


def load_source_config(config_path: Path) -> list[SourceModel]:
    """
    Load source configuration from a JSON file.
    Args:
        config_path (Path): The path to the JSON configuration file.
    Returns:
        list[SourceModel]: A list of source configuration dictionaries.
    """
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    sources = [SourceModel(**source) for source in config["sources"]]
    return sources

def setup_state_db(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Set up a DuckDB database at the specified path. Used for state management
    Args:
        db_path (str): The path to the DuckDB database file.
    """
    conn = duckdb.connect(db_path)
    conn.execute("""
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
    """)
    return conn

def log_discovered_files(
        conn: duckdb.DuckDBPyConnection, 
        files: list[FileRow]
) -> None:
    """
    Log discovered files into the DuckDB state database.
    Args:
        conn (duckdb.DuckDBPyConnection): The DuckDB connection.
        files (list[FileRow]): List of discovered files.
    """
    discovered_files_df = pl.DataFrame(files)
    print(f"Logging {discovered_files_df.height} discovered files to the database.")
    conn.register("discovered_files", discovered_files_df)
    conn.execute("""
        MERGE INTO file_tracker AS target
        USING discovered_files AS source
            ON target.file_name = source.file_name 
            AND target.source_name = source.source
        WHEN MATCHED 
            AND target.last_modified <> source.modified 
            AND target.size_bytes <> source.size_bytes
            THEN 
                UPDATE SET 
                    last_modified = source.modified,
                    size_bytes = source.size_bytes,
                    downloaded = FALSE
        WHEN NOT MATCHED THEN
            INSERT (file_name, url, last_modified, source_name, size_bytes)
            VALUES (source.file_name, source.url, source.modified, source.source, source.size_bytes)
    """)
    conn.unregister("discovered_files")

def discover_files_at_source(
        source: SourceModel, 
        session: requests.Session
) -> None:
    """
    Discover available files at the given source. Logs the discovered fiels to a duckdb table
    """
    if source.type != "directory":
        raise ValueError("Source type must be 'directory' to discover files.")
    print(f"Discovering files at source: {source.name} ({source.url})")
    files_discovered = []
    response = session.get(source.url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        file_name = tds[0].find("a").get("href")
        # Filter files based on source prefix and suffixes
        if (
            not file_name 
            or not any(file_name.endswith(suffix) for suffix in source.suffixes) 
            or not file_name.startswith(source.prefix)
        ):
            continue
        date_modified = tds[1].text.strip()
        size_str = tds[2].text.strip()
        files_discovered.append(FileRow(
            file_name=file_name,
            source=source.name,
            url=source.url + file_name,
            modified=datetime.strptime(date_modified, "%Y-%m-%d %H:%M"),
            size_bytes=int(size_str) if size_str.isdigit() else 0
        ))
    return files_discovered


def main():
    script_dir = Path(__file__).parent.resolve() / "source_info.yml"
    data_dir = Path(__file__).parent.resolve() / "../../../" / "data"
    sources = load_source_config(script_dir)
    state_conn = setup_state_db(db_path=data_dir / "duckdb" / "state.duckdb")
    session = requests.Session()
    for source in sources:
        print(f"Processing source: {source.name}")
        if source.type == "directory":
            files_discovered = discover_files_at_source(source, session)
            log_discovered_files(state_conn, files_discovered)

if __name__ == "__main__":
    main()