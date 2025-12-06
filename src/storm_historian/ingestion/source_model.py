from pydantic import BaseModel


class SourceModel(BaseModel):
    name: str
    url: str
    prefix: str
    extension: str
    description: str