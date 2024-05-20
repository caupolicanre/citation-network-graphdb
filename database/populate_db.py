import os
from os.path import join, dirname

import dotenv

from neo4j import GraphDatabase
from neomodel import config


dotenv_path = join(dirname(__file__), '.env')
dotenv.load_dotenv(dotenv_path)

URI = os.environ.get('DB_URI')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
AUTH = (DB_USER, DB_PASS)

config.DATABASE_URL = f'bolt://{AUTH[0]}:{AUTH[1]}@{URI}'
config.DATABASE_NAME = DB_NAME


