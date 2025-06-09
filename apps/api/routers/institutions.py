from typing import List

from fastapi import APIRouter, Query, HTTPException

from apps.api.schemas.institution import InstitutionSchema
from database.utils.db_connection import get_neo4j_driver



router = APIRouter()


@router.get("/institutions", response_model=List[InstitutionSchema])
async def get_institutions(skip: int = Query(0, ge=0), limit: int = Query(10, gt=0)):
    driver, db_name = get_neo4j_driver()
    query = """
    MATCH (i:Institution)
    RETURN i.inst_id AS inst_id, i.name AS name
    SKIP $skip LIMIT $limit
    """
    with driver.session(database=db_name) as session:
        result = session.run(query, skip=skip, limit=limit)
        institutions = [InstitutionSchema(**record) for record in result]
    return institutions

@router.get("/institutions/{institution_id}", response_model=InstitutionSchema)
async def get_institution(institution_id: str):
    driver, db_name = get_neo4j_driver()
    query = """
    MATCH (i:Institution {inst_id: $institution_id})
    RETURN i.inst_id AS inst_id, i.name AS name
    """
    with driver.session(database=db_name) as session:
        record = session.run(query, institution_id=institution_id).single()
        if not record:
            raise HTTPException(status_code=404, detail="Institution not found")
        return InstitutionSchema(**record)
