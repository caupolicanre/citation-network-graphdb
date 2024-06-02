from typing import Union
from neomodel import config, db

from core.app_enums import AuthorApp, InstitutionApp, PaperApp


def count_nodes(label: Union[AuthorApp, InstitutionApp, PaperApp], database_url: str, database_name: str) -> int:
    '''
    Count the number of nodes with a specific label in the database.
    Using the neomodel library.

    Parameters
    ----------
    label : Union[AuthorApp, InstitutionApp, PaperApp]
        Label of the nodes to be counted.
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    
    Returns
    -------
    int
        Number of nodes with the specified label.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    query = f"MATCH (n:{label.value}) RETURN count(n) as count"
    results, meta = db.cypher_query(query)
    
    return results[0][0]