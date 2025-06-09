from typing import Optional
from pydantic import BaseModel


class PublicationSchema(BaseModel):
    paper_id: int
    title: str
    doi: Optional[str] = None
    year: Optional[int] = None
    n_citation: Optional[int] = 0

    class Config:
        orm_mode = True
