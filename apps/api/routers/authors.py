from typing import List

from fastapi import APIRouter, Query, HTTPException

from apps.api.schemas.author import AuthorSchema
from database.utils.db_connection import get_neo4j_driver



router = APIRouter()


@router.get('', response_model=List[AuthorSchema])
async def get_authors(skip: int = Query(0, ge=0), limit: int = Query(10, gt=0)):
    driver, db_name = get_neo4j_driver()
    query = """
    MATCH (a:Author)
    RETURN a.author_id AS author_id, a.name AS name
    SKIP $skip LIMIT $limit
    """
    with driver.session(database=db_name) as session:
        result = session.run(query, skip=skip, limit=limit)
        authors = [AuthorSchema(**record) for record in result]
    return authors

@router.get('/{author_id}', response_model=AuthorSchema)
async def get_author(author_id: str):
    driver, db_name = get_neo4j_driver()
    query = """
    MATCH (a:Author {author_id: $author_id})
    RETURN a.author_id AS author_id, a.name AS name
    """
    with driver.session(database=db_name) as session:
        record = session.run(query, author_id=author_id).single()
        if not record:
            raise HTTPException(status_code=404, detail='Author not found')
        return AuthorSchema(**record)
