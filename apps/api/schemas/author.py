from typing import Optional
from pydantic import BaseModel


class AuthorSchema(BaseModel):
    author_id: str
    name: str

    class Config:
        orm_mode = True
