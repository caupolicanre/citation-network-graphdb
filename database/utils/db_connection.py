import os
from os.path import join, dirname

import dotenv
from typing import Union

from core.enums.db_enums import DatabaseType


dotenv_path = join(dirname(__file__), '.env')
dotenv.load_dotenv(dotenv_path)



def neomodel_connect(db_option: DatabaseType = DatabaseType.TEST) -> tuple[str, str]:
    '''
    Set up environment variables for connecting to Neo4j database using neomodel.
    Use this function when using neomodel as the OGM.
    Variables returned are used in the 'config.DATABASE_URL' and 'config.DATABASE_NAME' of neomodel configuration.

    Parameters
    ----------
    db_option : DatabaseType
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
        If environment variables for Test or Production database are not found.
    '''
    URI = os.environ.get('DB_URI', None)
    if not URI:
        raise ValueError('Environment variable for URI not found.')

    if db_option == DatabaseType.TEST:
        TEST_DB_NAME = os.environ.get('TEST_DB_NAME', None)
        TEST_DB_USER = os.environ.get('TEST_DB_USER', None)
        TEST_DB_PASS = os.environ.get('TEST_DB_PASS', None)
        AUTH = (TEST_DB_USER, TEST_DB_PASS)

        if not (TEST_DB_NAME and TEST_DB_USER and TEST_DB_PASS):
            raise ValueError('Environment variables for Test database not found.')

        database_url = f'bolt://{AUTH[0]}:{AUTH[1]}@{URI}'
        database_name = TEST_DB_NAME

    elif db_option == DatabaseType.PRODUCTION:
        DB_NAME = os.environ.get('DB_NAME', None)
        DB_USER = os.environ.get('DB_USER', None)
        DB_PASS = os.environ.get('DB_PASS', None)
        AUTH = (DB_USER, DB_PASS)

        if not (DB_NAME and DB_USER and DB_PASS):
            raise ValueError('Environment variables for Production database not found.')

        database_url = f'bolt://{AUTH[0]}:{AUTH[1]}@{URI}'
        database_name = DB_NAME

    return database_url, database_name


def neo4j_connect(db_option: DatabaseType = DatabaseType.TEST) -> tuple[str, str, tuple[str, str]]:
    '''
    Set up environment variables for connecting to Neo4j database using neo4j.
    Use this function when using neo4j GraphDatabase.
    Variables returned are used in the 'driver' and 'session' of neo4j GraphDatabase.

    Parameters
    ----------
    db_option : DatabaseType
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
        If environment variables for Test or Production database are not found.
    '''
    URI = os.environ.get('DB_URI', None)
    if not URI:
        raise ValueError('Environment variable for URI not found.')

    if db_option == DatabaseType.TEST:
        TEST_DB_NAME = os.environ.get('TEST_DB_NAME', None)
        TEST_DB_USER = os.environ.get('TEST_DB_USER', None)
        TEST_DB_PASS = os.environ.get('TEST_DB_PASS', None)
        AUTH = (TEST_DB_USER, TEST_DB_PASS)

        if not (TEST_DB_NAME and TEST_DB_USER and TEST_DB_PASS):
            raise ValueError('Environment variables for Test database not found.')

        database_url = f'bolt://{URI}/{TEST_DB_NAME}'
        database_name = TEST_DB_NAME
        auth = AUTH
    
    elif db_option == DatabaseType.PRODUCTION:
        DB_NAME = os.environ.get('DB_NAME', None)
        DB_USER = os.environ.get('DB_USER', None)
        DB_PASS = os.environ.get('DB_PASS', None)
        AUTH = (DB_USER, DB_PASS)

        if not (DB_NAME and DB_USER and DB_PASS):
            raise ValueError('Environment variables for Production database not found.')

        database_url = f'bolt://{URI}/{DB_NAME}'
        database_name = DB_NAME
        auth = AUTH

    return database_url, database_name, auth