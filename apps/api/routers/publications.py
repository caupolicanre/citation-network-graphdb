from typing import List

from fastapi import APIRouter, Query, HTTPException

from apps.api.schemas.publication import PublicationSchema
from database.utils.db_connection import get_neo4j_driver



router = APIRouter()


@router.get("/publications", response_model=List[PublicationSchema])
async def get_publications(skip: int = Query(0, ge=0), limit: int = Query(10, gt=0)):
    driver, db_name = get_neo4j_driver()
    query = """
    MATCH (p:Paper)
    RETURN p.paper_id AS paper_id, p.title AS title, p.doi AS doi, p.year AS year, p.n_citation AS n_citation
    SKIP $skip LIMIT $limit
    """
    with driver.session(database=db_name) as session:
        result = session.run(query, skip=skip, limit=limit)
        publications = [PublicationSchema(**record) for record in result]
    return publications

@router.get("/authors/{author_id}/publications", response_model=List[PublicationSchema])
async def get_publications_by_author(author_id: int, skip: int = Query(0, ge=0), limit: int = Query(10, gt=0)):
    driver, db_name = get_neo4j_driver()
    query = """
    MATCH (a:Author {author_id: $author_id})<-[:AUTHORED_BY]-(p:Paper)
    RETURN p.paper_id AS paper_id, p.title AS title, p.doi AS doi, p.year AS year, p.n_citation AS n_citation
    SKIP $skip LIMIT $limit
    """
    with driver.session(database=db_name) as session:
        result = session.run(query, author_id=author_id, skip=skip, limit=limit)
        publications = [PublicationSchema(**record) for record in result]
    return publications

@router.get("/institutions/{institution_id}/publications", response_model=List[PublicationSchema])
async def get_publications_by_institution(institution_id: str, skip: int = Query(0, ge=0), limit: int = Query(10, gt=0)):
    driver, db_name = get_neo4j_driver()
    query = """
    MATCH (i:Institution {inst_id: $institution_id})<-[:PUBLISHED_BY]-(p:Paper)
    RETURN p.paper_id AS paper_id, p.title AS title, p.doi AS doi, p.year AS year, p.n_citation AS n_citation
    SKIP $skip LIMIT $limit
    """
    with driver.session(database=db_name) as session:
        result = session.run(query, institution_id=institution_id, skip=skip, limit=limit)
        publications = [PublicationSchema(**record) for record in result]
    return publications

@router.get("/fields_of_study/{field_name}/publications", response_model=List[PublicationSchema])
async def get_publications_by_field_of_study(field_name: str, skip: int = Query(0, ge=0), limit: int = Query(10, gt=0)):
    driver, db_name = get_neo4j_driver()
    query = """
    MATCH (f:FieldOfStudy {name: $field_name})<-[:RELATED_TO]-(p:Paper)
    RETURN p.paper_id AS paper_id, p.title AS title, p.doi AS doi, p.year AS year, p.n_citation AS n_citation
    SKIP $skip LIMIT $limit
    """
    with driver.session(database=db_name) as session:
        result = session.run(query, field_name=field_name, skip=skip, limit=limit)
        publications = [PublicationSchema(**record) for record in result]
    return publications
