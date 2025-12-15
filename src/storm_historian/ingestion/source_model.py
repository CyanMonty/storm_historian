from dataclasses import dataclass


@dataclass
class SourceModel:
    name: str
    url: str
    prefix: str
    suffixes: list[str]
    description: str
    type: str  # e.g., 'directory' or 'file'