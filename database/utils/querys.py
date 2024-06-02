from typing import Union, Optional
from neomodel import config, db

from core.enums.app_enums import AuthorApp, InstitutionApp, PaperApp


def count_nodes(database_url: str, database_name: str,
                label: Optional[Union[AuthorApp, InstitutionApp, PaperApp]] = None) -> int:
    '''
    Count the number of nodes with the specified label in the database.
    If no label is specified, count all nodes in the database.
    Using the neomodel library.

    Parameters
    ----------
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    label : Optional[Union[AuthorApp, InstitutionApp, PaperApp]], optional
        Label of the nodes to be counted, by default None.
    
    Returns
    -------
    results[0][0] : int
        Number of nodes with the specified label in the database.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    if label:
        query = f"MATCH (n:{label.value}) RETURN count(n)"
    else:
        query = "MATCH (n) RETURN count(n)"
    
    results, meta = db.cypher_query(query)
    
    return results[0][0]