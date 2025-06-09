from pydantic import BaseModel


class FieldOfStudySchema(BaseModel):
    fos_id: str
    name: str

    class Config:
        orm_mode = True
