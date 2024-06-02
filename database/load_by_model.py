import os
from os.path import join, dirname

import dotenv
import gc
from datetime import datetime
import ijson
from tqdm import tqdm

from neomodel import config, db

from database.utils.funcs import detect_encoding
from database.utils import querys
from database.utils.db_connection import neomodel_connect

from apps.author.models import Author, AuthorOrganizationRel
from apps.institution.models import Organization, Publisher, Venue, VenueType
from apps.paper.models import Paper, FieldOfStudy, PaperFieldOfStudyRel, DocumentType



def create_document_type_nodes(nodes: list, database_url: str, database_name: str) -> None:
    '''
    Create nodes for each document type in the dataset with neomodel.
    Using the DocumentType model.
    
    Parameters
    ----------
    nodes : list
        A list of dictionaries containing the document type's information.
        Required fields:
            - doc_type: str (required)
                Document type.
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    with db.transaction:
        for obj in nodes:
            doc_type = obj.get('doc_type', None)

            if doc_type:
                doc_type_node = DocumentType.nodes.get_or_none(type=doc_type)
            
                if not doc_type_node:
                    doc_type_node = DocumentType(type=doc_type).save()

    gc.collect()


def create_publisher_nodes(nodes: list, database_url: str, database_name: str) -> None:
    '''
    Create nodes for each publisher in the dataset with neomodel.
    Using the Publisher model.

    Parameters
    ----------
    nodes : list
        A list of dictionaries containing the publisher's information.
        Required fields:
            - publisher: str (required)
                Publisher name.
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    with db.transaction:
        for obj in nodes:
            publisher = obj.get('publisher', None)

            if publisher:
                publisher_node = Publisher.nodes.get_or_none(name=publisher)

                if not publisher_node:
                    publisher_node = Publisher(name=publisher).save()

    gc.collect()


def create_venue_nodes(nodes: list, database_url: str, database_name: str) -> None:
    '''
    Create nodes for each venue and venue type in the dataset with neomodel.
    Using the Venue model and VenueType model.

    Parameters
    ----------
    nodes : list
        A list of dictionaries containing the venue's information.
        Required fields:
            - venue: dict (required)
                Venue information.
                Required fields:
                    - raw: str (required)
                        Venue name.
                    - type: str (optional)
                        Venue type.
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    with db.transaction:
        for obj in nodes:
            venue = obj.get('venue', None)

            if venue:
                venue_name = venue.get('raw', None)
                venue_type = venue.get('type', None)

                venue_node = Venue.nodes.get_or_none(name=venue_name)

                if not venue_node:
                    venue_node = Venue(name=venue_name).save()

                    if venue_type:
                        venue_type_node = VenueType.nodes.get_or_none(type=venue_type)

                        if not venue_type_node:
                            venue_type_node = VenueType(type=venue_type).save()

                        if not venue_node.type.is_connected(venue_type_node):
                            venue_node.type.connect(venue_type_node)

    gc.collect()


def create_author_org_nodes(nodes: list, database_url: str, database_name: str) -> None:
    '''
    Create nodes for each author and organization in the dataset with neomodel.
    Using the Author model and Organization model.

    Parameters
    ----------
    nodes : list
        A list of dictionaries containing the author's and organization's information.
        Required fields:
            - authors: list (required)
                List of authors.
                Required fields:
                    - id: int (optional)
                        Author ID.
                    - name: str (required)
                        Author name.
                    - org: str
                        Organization name.
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    with db.transaction:
        for obj in nodes:
            authors = obj.get('authors', [])

            for author in authors:
                author_id = author.get('id', None)
                author_name = author.get('name', None)
                org_name = author.get('org', None)

                author_node = Author.nodes.get_or_none(name=author_name)

                if not author_node:
                    author_node = Author(author_id=author_id, name=author_name).save()

                if org_name:
                    organization_node = Organization.nodes.get_or_none(name=org_name)

                    if not organization_node:
                        organization_node = Organization(name=org_name).save()

                    if not author_node.organization.is_connected(organization_node):
                        author_node.organization.connect(organization_node)

    gc.collect()


def create_fos_nodes(nodes: list, database_url: str, database_name: str) -> None:
    '''
    Create nodes for each field of study in the dataset with neomodel.
    Using the FieldOfStudy model.
    The weight of the field of study is saved when creating Paper node with the PaperFieldOfStudyRel relationship.
    
    Parameters
    ----------
    nodes : list
        A list of dictionaries containing the field of study's information.
        Required fields:
            - fos: list (required)
                List of field of study.
                Required fields:
                    - name: str (required)
                        Field of study name.
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    with db.transaction:
        for obj in nodes:
            fields_of_study = obj.get('fos', [])

            for fos in fields_of_study:
                fos_name = fos.get('name', None)

                fos_node = FieldOfStudy.nodes.get_or_none(name=fos_name)

                if not fos_node:
                    fos_node = FieldOfStudy(name=fos_name).save()

    gc.collect()




if __name__ == '__main__':
    dotenv_path = join(dirname(__file__), '.env')
    dotenv.load_dotenv(dotenv_path)

    dataset_path = './dataset/dblp.v12.json'
    encoding = detect_encoding(dataset_path)

    BATCH_SIZE_PAPER_NODES = int(os.environ.get('BATCH_SIZE_PAPER_NODES', 5000))
    paper_nodes_batch = []

    BATCH_SIZE_REQUIRED_NODES = int(os.environ.get('BATCH_SIZE_REQUIRED_NODES', 10000))
    required_nodes_batch = []



    print('=======================')
    print(' Load Model by Batches')
    print('=======================')
    print('Choose Database:\n1. Production\n2. Test')

    db_option = None
    while db_option not in ['Production', 'Test']:
        db_option = input('\nDatabase: ')

        if db_option not in ['Production', 'Test']:
            print('Invalid Database. Please choose between \'Production\' and \'Test\'.')

    database_url, database_name = neomodel_connect(db_option)

    print(f'Database: {database_name}')

    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name


    print('\n===================')
    print(' Create Model nodes')
    print('====================')
    print('Select the model nodes to create:')
    print('1. Document Type')
    print('2. Publisher')
    print('3. Venue')
    print('4. Author and Organization')
    print('5. Field of Study')
    print('6. Papers')

    model_option = input('\nModel (select by number, just one model): ')

    while model_option not in ['1', '2', '3', '4', '5', '6'] \
          or len(model_option) != 1:
        print('Invalid input. Please select the Model to create nodes.')
        model_option = input('Model (select by number, just one model): ')

    model_option = int(model_option)

    if model_option == 1:
        create_nodes = 'y'

        count_doc_type_nodes = querys.count_nodes('DocumentType', database_url, database_name)
        if count_doc_type_nodes > 0:
            print(f'\nDocument Type has {count_doc_type_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input('Do you still want to create Document Type nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':

            print('\nCreating Document Type nodes')

            with open(dataset_path, 'r', encoding=encoding) as f:
                objects = ijson.items(f, 'item')

                for obj in tqdm(objects, desc='Creating Document Type nodes', unit=' papers'):
                    required_nodes_batch.append(obj)

                    if len(required_nodes_batch) >= BATCH_SIZE_REQUIRED_NODES:
                        create_document_type_nodes(required_nodes_batch, database_url, database_name)
                        required_nodes_batch = []

                if required_nodes_batch:
                    create_document_type_nodes(required_nodes_batch, database_url, database_name)

            required_nodes_batch = []
            print('\nDocument Types loaded to Graph Database.')

            count_doc_type_nodes = querys.count_nodes('DocumentType', database_url, database_name)
            print(f'Total Document Type Nodes: {count_doc_type_nodes}')


    if model_option == 2:
        create_nodes = 'y'

        count_publisher_nodes = querys.count_nodes('Publisher', database_url, database_name)
        if count_publisher_nodes > 0:
            print(f'\nPublisher has {count_publisher_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input('Do you still want to create Publisher nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':

            print('\nCreating Publisher nodes')

            with open(dataset_path, 'r', encoding=encoding) as f:
                objects = ijson.items(f, 'item')

                for obj in tqdm(objects, desc='Creating Publisher nodes', unit=' papers'):
                    required_nodes_batch.append(obj)

                    if len(required_nodes_batch) >= BATCH_SIZE_REQUIRED_NODES:
                        create_publisher_nodes(required_nodes_batch, database_url, database_name)
                        required_nodes_batch = []

                if required_nodes_batch:
                    create_publisher_nodes(required_nodes_batch, database_url, database_name)

            required_nodes_batch = []
            print('\nPublishers loaded to Graph Database.')

            count_publisher_nodes = querys.count_nodes('Publisher', database_url, database_name)
            print(f'Total Publisher Nodes: {count_publisher_nodes}')


    if model_option == 3:
        create_nodes = 'y'

        count_venue_nodes = querys.count_nodes('Venue', database_url, database_name)
        if count_venue_nodes > 0:
            print(f'\nVenue has {count_venue_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input('Do you still want to create Venue nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':

            print('\nCreating Venue nodes')

            with open(dataset_path, 'r', encoding=encoding) as f:
                objects = ijson.items(f, 'item')

                for obj in tqdm(objects, desc='Creating Venue nodes', unit=' papers'):
                    required_nodes_batch.append(obj)

                    if len(required_nodes_batch) >= BATCH_SIZE_REQUIRED_NODES:
                        create_venue_nodes(required_nodes_batch, database_url, database_name)
                        required_nodes_batch = []

                if required_nodes_batch:
                    create_venue_nodes(required_nodes_batch, database_url, database_name)

            required_nodes_batch = []
            print('\nVenues loaded to Graph Database.')

            count_venue_nodes = querys.count_nodes('Venue', database_url, database_name)
            print(f'Total Venue Nodes: {count_venue_nodes}')


    if model_option == 4:
        create_nodes = 'y'

        count_author_nodes = querys.count_nodes('Author', database_url, database_name)
        count_org_nodes = querys.count_nodes('Organization', database_url, database_name)
        if count_author_nodes > 0 or count_org_nodes > 0:
            print(f'\nAuthor has {count_author_nodes} nodes and Organization has {count_org_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input('Do you still want to create Author and Organization nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':

            print('\nCreating Author and Organization nodes')

            with open(dataset_path, 'r', encoding=encoding) as f:
                objects = ijson.items(f, 'item')

                for obj in tqdm(objects, desc='Creating Author and Org nodes', unit=' papers'):
                    required_nodes_batch.append(obj)

                    if len(required_nodes_batch) >= BATCH_SIZE_REQUIRED_NODES:
                        create_author_org_nodes(required_nodes_batch, database_url, database_name)
                        required_nodes_batch = []

                if required_nodes_batch:
                    create_author_org_nodes(required_nodes_batch, database_url, database_name)

            required_nodes_batch = []
            print('\nAuthors and Organizations loaded to Graph Database.')

            count_author_nodes = querys.count_nodes('Author', database_url, database_name)
            count_organization_nodes = querys.count_nodes('Organization', database_url, database_name)
            print(f'Total Author Nodes: {count_author_nodes}')
            print(f'Total Organization Nodes: {count_organization_nodes}')


    if model_option == 5:
        create_nodes = 'y'

        count_fos_nodes = querys.count_nodes('FieldOfStudy', database_url, database_name)
        if count_fos_nodes > 0:
            print(f'\nField of Study has {count_fos_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input('Do you still want to create Field of Study nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':

            print('\nCreating Field of Study nodes')

            with open(dataset_path, 'r', encoding=encoding) as f:
                objects = ijson.items(f, 'item')

                for obj in tqdm(objects, desc='Creating FOS nodes', unit=' papers'):
                    required_nodes_batch.append(obj)

                    if len(required_nodes_batch) >= BATCH_SIZE_REQUIRED_NODES:
                        create_fos_nodes(required_nodes_batch, database_url, database_name)
                        required_nodes_batch = []

                if required_nodes_batch:
                    create_fos_nodes(required_nodes_batch, database_url, database_name)

            required_nodes_batch = []
            print('\nFields of Study loaded to Graph Database.')

            count_fos_nodes = querys.count_nodes('FieldOfStudy', database_url, database_name)
            print(f'Total Field of Study Nodes: {count_fos_nodes}')
    

    if model_option == 6:
        pass