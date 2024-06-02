import gc

import pandas as pd

import neo4j
from neo4j import GraphDatabase

from database.utils.db_connection import neo4j_connect



def create_index(node_label: str, field_name: str, database_url:str, database_name: str, auth: tuple) -> None:
    '''
    Create index for a field in a node.

    Parameters
    ----------
    node_label : str
        Label of the node.
    field_name : str
        Field to create index for.
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    auth : tuple
        Authentication tuple. (username, password)
    '''
    driver = GraphDatabase.driver(database_url, auth=auth)

    with driver.session(database=database_name) as session:
        try:
            print(f'\nCreating index for {node_label}.{field_name}')
            session.run(f"CREATE INDEX FOR (n:{node_label}) ON (n.{field_name})")

        except neo4j.exceptions.ClientError as e:
            if 'EquivalentSchemaRuleAlreadyExists' in str(e):
                print('\nAn equivalent index already exists.')
                print(f'Error: {e}')
            else:
                raise e
    
    session.close()
    driver.close()
    gc.collect()


def get_indexes(database_url: str, database_name: str, auth: tuple) -> pd.DataFrame:
    '''
    Get indexes in the database.

    Parameters
    ----------
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    auth : tuple
        Authentication tuple. (username, password)
    
    Returns
    -------
    df : pd.DataFrame
        Indexes in the database.
    '''
    driver = GraphDatabase.driver(database_url, auth=auth)

    with driver.session(database=database_name) as session:
        result = session.run('SHOW INDEXES')
        indexes = result.data()
        df = pd.DataFrame(indexes)
    
    session.close()
    driver.close()
    gc.collect()

    return df

        


if __name__ == '__main__':
    print('=============================')
    print(' Create Indexes for Database')
    print('=============================')
    print('Choose Database:\n1. Production\n2. Test')

    db_option = None
    while db_option not in ['Production', 'Test']:
        db_option = input('\nDatabase: ')

        if db_option not in ['Production', 'Test']:
            print('Invalid Database. Please choose between \'Production\' and \'Test\'.')

    database_url, database_name, auth = neo4j_connect(db_option)

    print(f'Database: {database_name}')



    nodes_to_index = [
        {'node_label': 'Paper', 'field_name': 'paper_id'},
        {'node_label': 'Paper', 'field_name': 'title'},

        {'node_label': 'Author', 'field_name': 'author_id'},
        {'node_label': 'Author', 'field_name': 'name'},

        {'node_label': 'Organization', 'field_name': 'name'},
        {'node_label': 'Venue', 'field_name': 'name'},

        {'node_label': 'FieldOfStudy', 'field_name': 'name'}
    ]


    print(f'\nCreating indexes for \'{db_option}\' Database...')

    for node in nodes_to_index:
        create_index(node['node_label'], node['field_name'], database_url, database_name, auth)

    print('\nIndexes created successfully.')


    show_indexes = input('\nShow indexes? (y/n): ')

    if show_indexes.lower() in ['y', 'yes']:
        indexes = get_indexes(database_url, database_name, auth)
        print(f'\nIndexes in \'{db_option}\' Database:')
        print(indexes)