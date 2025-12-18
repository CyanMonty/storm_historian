from pathlib import Path
import duckdb
import requests

def download_file(
    session: requests.Session,
    url: str,
    destination_path: Path,
    file_name: str
) -> None:
    """
    Download a file from a given URL and save it to the specified destination path.
    Args:
        session (requests.Session): The requests session to use for downloading.
        url (str): The URL of the file to download.
        destination_path (Path): The local path where the file will be saved.
        file_name (str): The name of the file being downloaded.
    """
    destination_path.mkdir(parents=True, exist_ok=True)
    full_path = destination_path / file_name
    tmp_path = destination_path.with_suffix(full_path.suffix +".partial")
    try:
        with session.get(url, stream=True) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        tmp_path.replace(full_path)
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"Connection error downloading {url}") from e
    except requests.exceptions.RequestException as e:
        print(e)
        raise RuntimeError(f"Request failed for {url}") from e
    finally:
        # Clean up partial file if something went wrong
        if tmp_path.exists() and not full_path.exists():
            tmp_path.unlink(missing_ok=True)

def pull_undownloaded_files(
    conn: duckdb.DuckDBPyConnection,
    destination_path: str
):
    """
    Pull a file from a given URL and save it to the specified destination path.
    Args:
        conn (duckdb.DuckDBPyConnection): The DuckDB connection to the state database.
        destination_path (str): The local path where the file will be saved.
    """
    undiscovered_files = conn.execute(
        """
        SELECT file_name, url, source_name
        FROM file_tracker
        WHERE downloaded = FALSE
        """
    ).fetchall()
    print(f"Found {len(undiscovered_files)} undownloaded files")

    session = requests.Session()
    for file in undiscovered_files:
        file_name, url, source_name = file
        print(f"Downloading file: {file_name} from {url}")
        download_file(
            session=session,
            url=url,
            destination_path=Path(destination_path) / source_name,
            file_name=file_name
        )
        conn.execute(
            f"""
            UPDATE file_tracker
            SET downloaded = TRUE,
                date_downloaded = CURRENT_TIMESTAMP
            WHERE file_name = '{file_name}'
            """
        )

def main():
    data_dir = Path(__file__).parent.resolve() / "../../../" / "data"
    state_conn = duckdb.connect(data_dir / "duckdb" / "state.db")
    pull_undownloaded_files(state_conn, data_dir / "raw")


if __name__ == "__main__":
    main()
