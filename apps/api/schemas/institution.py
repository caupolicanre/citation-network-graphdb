from pydantic import BaseModel


class InstitutionSchema(BaseModel):
    inst_id: str
    name: str

    class Config:
        orm_mode = True
