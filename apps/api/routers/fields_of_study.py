from typing import List

from fastapi import APIRouter, Query, HTTPException

from apps.api.schemas.field_of_study import FieldOfStudySchema
from database.utils.db_connection import get_neo4j_driver



router = APIRouter()


@router.get("/fields_of_study", response_model=List[FieldOfStudySchema])
async def get_fields_of_study(skip: int = Query(0, ge=0), limit: int = Query(10, gt=0)):
    driver, db_name = get_neo4j_driver()
    query = """
    MATCH (f:FieldOfStudy)
    RETURN f.fos_id AS fos_id, f.name AS name
    SKIP $skip LIMIT $limit
    """
    with driver.session(database=db_name) as session:
        result = session.run(query, skip=skip, limit=limit)
        fields = [FieldOfStudySchema(**record) for record in result]
    return fields

@router.get("/fields_of_study/{field_name}", response_model=FieldOfStudySchema)
async def get_field_of_study(field_name: str):
    driver, db_name = get_neo4j_driver()
    query = """
    MATCH (f:FieldOfStudy {name: $field_name})
    RETURN f.fos_id AS fos_id, f.name AS name
    """
    with driver.session(database=db_name) as session:
        record = session.run(query, field_name=field_name).single()
        if not record:
            raise HTTPException(status_code=404, detail="Field of study not found")
        return FieldOfStudySchema(**record)
