from neomodel import config, db

def count_nodes(label: str, database_url: str, database_name: str) -> int:
    '''
    Count the number of nodes with a specific label in the database.
    Using the neomodel library.

    Parameters
    ----------
    label : str
        Node label.
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

    query = f"MATCH (n:{label}) RETURN count(n) as count"
    results, meta = db.cypher_query(query)
    
    return results[0][0]