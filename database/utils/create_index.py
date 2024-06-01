import os
from os.path import join, dirname

import dotenv
import time

import pandas as pd

import neo4j
from neo4j import GraphDatabase


dotenv_path = join(dirname(__file__), '.env')
dotenv.load_dotenv(dotenv_path)


print('=============================')
print('Create Indexes for Database')
print('=============================')
print('Choose Database:\n1. Production\n2. Test')

db = None
while db not in ['Production', 'Test']:
    db = input('\nDatabase: ')

    if db not in ['Production', 'Test']:
        print('Invalid Database. Please choose between \'Production\' and \'Test\'.')

TEST = True if db == 'Test' else False

URI = os.environ.get('DB_URI')

if TEST:
    print('\nDatabase selected: Test')
    TEST_DB_NAME = os.environ.get('TEST_DB_NAME')
    TEST_DB_USER = os.environ.get('TEST_DB_USER')
    TEST_DB_PASS = os.environ.get('TEST_DB_PASS')
    AUTH = (TEST_DB_USER, TEST_DB_PASS)

    DB_URL = f'bolt://{URI}/{TEST_DB_NAME}'

    driver = GraphDatabase.driver(DB_URL, auth=AUTH)
    session = driver.session(database=TEST_DB_NAME)

else:
    print('\nDatabase selected: Production')
    DB_NAME = os.environ.get('DB_NAME')
    DB_USER = os.environ.get('DB_USER')
    DB_PASS = os.environ.get('DB_PASS')
    AUTH = (DB_USER, DB_PASS)

    DB_URL = f'bolt://{URI}/{DB_NAME}'

    driver = GraphDatabase.driver(DB_URL, auth=AUTH)
    session = driver.session(database=DB_NAME)



def create_index(node_label: str, field_name: str):
    with driver.session() as session:
        try:
            print(f'\nCreating index for {node_label}.{field_name}')
            session.run(f"CREATE INDEX FOR (n:{node_label}) ON (n.{field_name})")

        except neo4j.exceptions.ClientError as e:
            if 'EquivalentSchemaRuleAlreadyExists' in str(e):
                print('\nAn equivalent index already exists.')
                print(f'Error: {e}')
            else:
                raise e


def get_indexes(uri: str, auth: tuple) -> pd.DataFrame:
    with driver.session() as session:
        result = session.run('SHOW INDEXES')
        indexes = result.data()
        df = pd.DataFrame(indexes)

    return df

        


nodes_to_index = [
    {'node_label': 'Paper', 'field_name': 'paper_id'},
    {'node_label': 'Paper', 'field_name': 'title'},

    {'node_label': 'Author', 'field_name': 'author_id'},
    {'node_label': 'Author', 'field_name': 'name'},

    {'node_label': 'Organization', 'field_name': 'name'},
    {'node_label': 'Venue', 'field_name': 'name'},

    {'node_label': 'FieldOfStudy', 'field_name': 'name'}
]


print('\nCreating indexes for the database...')

for node in nodes_to_index:
    create_index(node['node_label'], node['field_name'])

print('\nIndexes created successfully.')


indexes = get_indexes(DB_URL, AUTH)

print('\nIndexes in the database:')
print(indexes)


session.close()
driver.close()