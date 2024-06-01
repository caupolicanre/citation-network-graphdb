import os
from os.path import join, dirname
import gc

import time
import dotenv
import ijson
from tqdm import tqdm

from neomodel import config, db

from database.utils.funcs import detect_encoding

from apps.paper.models import DocumentType



dotenv_path = join(dirname(__file__), '.env')
dotenv.load_dotenv(dotenv_path)

dataset_path = './dataset/dblp.v12.json'
encoding = detect_encoding(dataset_path)

BATCH_SIZE = 10000
nodes_batch = []



print('==================================')
print(' Populate Database: Document Type')
print('==================================')
print('Choose Database:\n1. Production\n2. Test')

db_option = None
while db_option not in ['Production', 'Test']:
    db_option = input('\nDatabase: ')

    if db_option not in ['Production', 'Test']:
        print('Invalid Database. Please choose between \'Production\' and \'Test\'.')

TEST = True if db_option == 'Test' else False

URI = os.environ.get('DB_URI')

if TEST:
    TEST_DB_NAME = os.environ.get('TEST_DB_NAME')
    TEST_DB_USER = os.environ.get('TEST_DB_USER')
    TEST_DB_PASS = os.environ.get('TEST_DB_PASS')
    AUTH = (TEST_DB_USER, TEST_DB_PASS)

    config.DATABASE_URL = f'bolt://{AUTH[0]}:{AUTH[1]}@{URI}'
    config.DATABASE_NAME = TEST_DB_NAME

else:
    DB_NAME = os.environ.get('DB_NAME')
    DB_USER = os.environ.get('DB_USER')
    DB_PASS = os.environ.get('DB_PASS')
    AUTH = (DB_USER, DB_PASS)

    config.DATABASE_URL = f'bolt://{AUTH[0]}:{AUTH[1]}@{URI}'
    config.DATABASE_NAME = DB_NAME




def create_nodes_batch(nodes):
    with db.transaction:
        for obj in nodes:
            doc_type = obj.get('doc_type', None)

            if doc_type:
                doc_type_node = DocumentType.nodes.get_or_none(type=doc_type)
            
                if not doc_type_node:
                    doc_type_node = DocumentType(type=doc_type).save()

    time.sleep(0.1) # Sleep to free up resources
    gc.collect()  # Garbage collection to free up memory



print('\nLoading Document Types to Graph Database...')


with open(dataset_path, 'r', encoding=encoding) as f:
    objects = ijson.items(f, 'item')

    for obj in tqdm(objects, desc='Creating nodes', unit=' papers'):
        nodes_batch.append(obj)
        if len(nodes_batch) >= BATCH_SIZE:
            create_nodes_batch(nodes_batch)
            nodes_batch = []

    if nodes_batch:
        create_nodes_batch(nodes_batch)


print('\nDocument Types loaded to Graph Database.')