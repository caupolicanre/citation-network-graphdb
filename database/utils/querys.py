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


def count_relationships(database_url: str, database_name: str,
                        node_a: Optional[Union[AuthorApp, InstitutionApp, PaperApp]] = None,
                        node_b: Optional[Union[AuthorApp, InstitutionApp, PaperApp]] = None) -> int:
    '''
    Count the number of relationships within the specified nodes in the database.
    If only one node is specified, count all relationships with that node.
    If no nodes are specified, count all relationships in the database.
    Using the neomodel library.

    Parameters
    ----------
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    node_a : Optional[Union[AuthorApp, InstitutionApp, PaperApp]], optional
        Label of the first node in the relationship, by default None.
    node_b : Optional[Union[AuthorApp, InstitutionApp, PaperApp]], optional
        Label of the second node in the relationship, by default None.
    
    Returns
    -------
    results[0][0] : int
        Number of relationships with the specified label in the database.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    if node_a and node_b:
        query = f"MATCH (n:{node_a.value})-[r]->(m:{node_b.value}) RETURN count(r)"
    elif node_a:
        query = f"MATCH (n:{node_a.value})-[r]->() RETURN count(r)"
    elif node_b:
        query = f"MATCH ()-[r]->(n:{node_b.value}) RETURN count(r)"
    else:
        query = "MATCH ()-[r]->() RETURN count(r)"
    
    results, meta = db.cypher_query(query)
    
    return results[0][0]


def search_node_by_name(database_url: str, database_name: str,
                        label: Union[AuthorApp.AUTHOR, PaperApp.PAPER, PaperApp.FIELD_OF_STUDY,
                                     InstitutionApp.ORGANIZATION, InstitutionApp.PUBLISHER, InstitutionApp.VENUE],
                        name: str) -> dict:
    '''
    Search for nodes with the specified name in the database.
    Using the neomodel library.

    Parameters
    ----------
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    label : Union[AuthorApp.AUTHOR, PaperApp.PAPER, PaperApp.FIELD_OF_STUDY, InstitutionApp.ORGANIZATION, InstitutionApp.PUBLISHER, InstitutionApp.VENUE]
        Label of the node to be searched.
    name : str
        Name of the node to be searched.
    
    Returns
    -------
    results
        Nodes with the specified name in the database.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    query = f"MATCH (n:{label.value}) WHERE n.name CONTAINS '{name}' RETURN n"

    results, meta = db.cypher_query(query)

    return results


def search_path(database_url: str, database_name: str,
                node_a: Union[AuthorApp.AUTHOR, PaperApp.PAPER, InstitutionApp.ORGANIZATION,
                              InstitutionApp.PUBLISHER, InstitutionApp.VENUE],
                node_b: Union[AuthorApp.AUTHOR, PaperApp.PAPER, InstitutionApp.ORGANIZATION,
                              InstitutionApp.PUBLISHER, InstitutionApp.VENUE],
                name_a: str, name_b: str) -> dict:
    '''
    Search for a path between two nodes with the specified names in the database.
    Using the neomodel library.

    Parameters
    ----------
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    node_a : Union[AuthorApp.AUTHOR, PaperApp.PAPER, InstitutionApp.ORGANIZATION, InstitutionApp.PUBLISHER, InstitutionApp.VENUE]
        Label of the first node in the path.
    node_b : Union[AuthorApp.AUTHOR, PaperApp.PAPER, InstitutionApp.ORGANIZATION, InstitutionApp.PUBLISHER, InstitutionApp.VENUE]
        Label of the second node in the path.
    name_a : str
        Name of the first node in the path.
    name_b : str
        Name of the second node in the path.
    
    Returns
    -------
    results
        Path between the two nodes with the specified names in the database.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    query = f"MATCH p = shortestPath((n:{node_a.value})-[*]-(m:{node_b.value})) WHERE n.name CONTAINS '{name_a}' AND m.name CONTAINS '{name_b}' RETURN p"

    results, meta = db.cypher_query(query)

    return results