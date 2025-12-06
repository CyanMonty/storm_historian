import requests


def pull_file(url: str) -> None:
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


def main():
    