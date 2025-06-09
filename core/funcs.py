from typing import List, Optional

import chardet
from fastapi import HTTPException

from apps.author.models import Author
from apps.paper.models import Paper
from apps.institution.models import Institution

from core.enums.db_enums import DatabaseType
from database.utils.db_connection import neo4j_connect



def detect_encoding(file_path: str) -> str:
    '''
    Detect the encoding of a file.

    Parameters
    ----------
    file_path : str
        The path to the file.

    Returns
    -------
    str
        The encoding of the file.
    '''
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)

    result = chardet.detect(raw_data)
    return result['encoding']


def get_authors(page: int = 1, limit: int = 10) -> List[Author]:
    authors = Author.nodes.all()
    start = (page - 1) * limit
    end = start + limit
    return authors[start:end]

def get_author_publications(author_id: int, page: int = 1, limit: int = 10) -> List[Paper]:
    author = Author.nodes.get(author_id=author_id)
    if not author:
        raise HTTPException(status_code=404, detail="Author not found")
    papers = author.paper
    start = (page - 1) * limit
    end = start + limit
    return papers[start:end]

def get_institution_publications(institution_id: str, page: int = 1, limit: int = 10) -> List[Paper]:
    institution = Institution.nodes.get(inst_id=institution_id)
    if not institution:
        raise HTTPException(status_code=404, detail="Institution not found")
    papers = institution.paper
    start = (page - 1) * limit
    end = start + limit
    return papers[start:end]

def get_papers_by_field(field_name: str, page: int = 1, limit: int = 10) -> List[Paper]:
    papers = Paper.nodes.filter(field_of_study__name=field_name)
    start = (page - 1) * limit
    end = start + limit
    return papers[start:end]
