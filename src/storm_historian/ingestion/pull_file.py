import requests
import yaml
from pathlib import Path
from bs4 import BeautifulSoup
import duckdb

from storm_historian.ingestion.source_model import SourceModel


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

def discover_files_at_source(
        source: SourceModel, 
        session: requests.Session,
        state_db: duckdb.DuckDBPyConnection,
) -> None:
    """
    Discover available files at the given source. Logs the discovered fiels to a duckdb table
    """
    if source.type != "directory":
        raise ValueError("Source type must be 'directory' to discover files.")
    print(f"Discovering files at source: {source.name} ({source.url})")
    response = session.get(source.url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    for a in soup.find_all("a"):
        href = a.get("href")
        if href and href.startswith(source.prefix) and any(href.endswith(suffix) for suffix in source.suffixes):
            file_url = source.url + href


def pull_file(url: str, destination_path: str) -> None:
    """
    Pull a file from a given URL and save it to the specified destination path.
    Args:
        url (str): The URL of the file to be downloaded.
        destination_path (str): The local path where the file will be saved.
    """
    response = requests.get(url)
    response.raise_for_status()
    with open(destination_path, "wb") as file:
        file.write(response.content)

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
        last_modified DATE,
        source_name TEXT,
        date_discovered TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        downloaded BOOLEAN DEFAULT FALSE,
        date_downloaded TIMESTAMP NULL
    )
    """)
    return conn


def main():
    script_dir = Path(__file__).parent.resolve() / "source_info.yml"
    data_dir = Path(__file__).parent.resolve() / "../../../" / "data"
    sources = load_source_config(script_dir)
    state_conn = setup_state_db(db_path=data_dir / "state.db")
    session = requests.Session()
    for source in sources:
        print(f"Processing source: {source.name}")
        if source.type == "directory":
            discover_files_at_source(source, session)


if __name__ == "__main__":
    main()
