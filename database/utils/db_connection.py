import os
from os.path import join, dirname

import dotenv
from typing import Union


dotenv_path = join(dirname(__file__), '.env')
dotenv.load_dotenv(dotenv_path)



def neomodel_connect(db_option: str='Test') -> tuple[str, str]:
    '''
    Set up environment variables for connecting to Neo4j database using neomodel.
    Use this function when using neomodel as the OGM.
    Variables returned are used in the 'config.DATABASE_URL' and 'config.DATABASE_NAME' of neomodel configuration.

    Parameters
    ----------
    db_option : str, optional
        Choose between 'Production' or 'Test' database.
        Default is 'Test'.
    
    Returns
    -------
    tuple[str, str]
        database_url : str
            URL for connecting to Neo4j database.
        database_name : str
            Name of the database.
    
    Raises
    ------
    ValueError
        If 'db_option' is not 'Production' or 'Test'.
    '''
    if not db_option in ['Production', 'Test']:
        raise ValueError('Invalid Database. Please choose between \'Production\' and \'Test\'.')

    URI = os.environ.get('DB_URI')

    if db_option == 'Test':
        TEST_DB_NAME = os.environ.get('TEST_DB_NAME')
        TEST_DB_USER = os.environ.get('TEST_DB_USER')
        TEST_DB_PASS = os.environ.get('TEST_DB_PASS')
        AUTH = (TEST_DB_USER, TEST_DB_PASS)

        database_url = f'bolt://{AUTH[0]}:{AUTH[1]}@{URI}'
        database_name = TEST_DB_NAME

    elif db_option == 'Production':
        DB_NAME = os.environ.get('DB_NAME')
        DB_USER = os.environ.get('DB_USER')
        DB_PASS = os.environ.get('DB_PASS')
        AUTH = (DB_USER, DB_PASS)

        database_url = f'bolt://{AUTH[0]}:{AUTH[1]}@{URI}'
        database_name = DB_NAME

    return database_url, database_name


def neo4j_connect(db_option: str='Test') -> tuple[str, str, tuple[str, str]]:
    '''
    Set up environment variables for connecting to Neo4j database using neo4j.
    Use this function when using neo4j GraphDatabase.
    Variables returned are used in the 'driver' and 'session' of neo4j GraphDatabase.

    Parameters
    ----------
    db_option : str, optional
        Choose between 'Production' or 'Test' database.
        Default is 'Test'.
    
    Returns
    -------
    tuple[str, str]
        database_url : str
            URL for connecting to Neo4j database.
        database_name : str
            Name of the database.
        auth : tuple[str, str]
            Username and password for authentication.
    
    Raises
    ------
    ValueError
        If 'db_option' is not 'Production' or 'Test'.
    '''
    if not db_option in ['Production', 'Test']:
        raise ValueError('Invalid Database. Please choose between \'Production\' and \'Test\'.')

    URI = os.environ.get('DB_URI')

    if db_option == 'Test':
        TEST_DB_NAME = os.environ.get('TEST_DB_NAME')
        TEST_DB_USER = os.environ.get('TEST_DB_USER')
        TEST_DB_PASS = os.environ.get('TEST_DB_PASS')
        AUTH = (TEST_DB_USER, TEST_DB_PASS)

        database_url = f'bolt://{URI}/{TEST_DB_NAME}'
        database_name = TEST_DB_NAME
        auth = AUTH
    
    elif db_option == 'Production':
        DB_NAME = os.environ.get('DB_NAME')
        DB_USER = os.environ.get('DB_USER')
        DB_PASS = os.environ.get('DB_PASS')
        AUTH = (DB_USER, DB_PASS)

        database_url = f'bolt://{URI}/{DB_NAME}'
        database_name = DB_NAME
        auth = AUTH

    return database_url, database_name, auth