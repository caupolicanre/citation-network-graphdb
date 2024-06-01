import os
from os.path import join, dirname

import dotenv
import time

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

else:
    print('\nDatabase selected: Production')
    DB_NAME = os.environ.get('DB_NAME')
    DB_USER = os.environ.get('DB_USER')
    DB_PASS = os.environ.get('DB_PASS')
    AUTH = (DB_USER, DB_PASS)

    DB_URL = f'bolt://{URI}/{DB_NAME}'



def create_indexes(uri: str, auth: tuple):
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        try:
            session.run("CREATE INDEX FOR (p:Paper) ON (p.paper_id)")
            session.run("CREATE INDEX FOR (p:Paper) ON (p.title)")

            session.run("CREATE INDEX FOR (a:Author) ON (a.author_id)")
            session.run("CREATE INDEX FOR (a:Author) ON (a.name)")

            session.run("CREATE INDEX FOR (o:Organization) ON (o.name)")
            session.run("CREATE INDEX FOR (v:Venue) ON (v.name)")

            session.run("CREATE INDEX FOR (f:FieldOfStudy) ON (f.name)")
        
        except neo4j.exceptions.ClientError as e:
            if 'EquivalentSchemaRuleAlreadyExists' in str(e):
                print('\nAn equivalent index already exists.')
                print(f'Error: {e}')
            else:
                raise e

    driver.close()


create_indexes(DB_URL, AUTH)