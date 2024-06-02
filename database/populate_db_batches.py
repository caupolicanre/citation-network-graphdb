import os
from os.path import join, dirname
import gc

from typing import Union

import time
import dotenv
import ijson
from tqdm import tqdm

from core.app_enums import AuthorApp, InstitutionApp, PaperApp
from core.funcs import detect_encoding

from database.utils import querys
from database.utils.db_connection import neomodel_connect

from database.load_by_model import create_document_type_nodes, create_publisher_nodes, create_venue_nodes, create_author_org_nodes, create_fos_nodes, create_paper_nodes



def populate_db(model: Union[AuthorApp, InstitutionApp, PaperApp],
                dataset_path: str, dataset_encoding: str, batch_size: int,
                database_url: str, database_name: str) -> None:
    '''
    Populate the database with the nodes of the selected model.

    Parameters
    ----------
    model : Union[AuthorApp, InstitutionApp, PaperApp]
        Model to populate.
    dataset_path : str
        Path to the dataset.
    dataset_encoding : str
        Encoding of the dataset.
    batch_size : int
        Batch size to load the nodes.
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    '''
    batch = []

    print(f'\nCreating {model.value} nodes')

    with open(dataset_path, 'r', encoding=dataset_encoding) as f:
        objects = ijson.items(f, 'item')

        for obj in tqdm(objects, desc=f'Creating {model.value} nodes', unit=' papers'):
            batch.append(obj)

            if len(batch) >= batch_size:
                if model == PaperApp.DOCUMENT_TYPE:
                    create_document_type_nodes(batch, database_url, database_name)

                elif model == InstitutionApp.PUBLISHER:
                    create_publisher_nodes(batch, database_url, database_name)

                elif model == InstitutionApp.VENUE:
                    create_venue_nodes(batch, database_url, database_name)

                elif model == AuthorApp.AUTHOR or model == InstitutionApp.ORGANIZATION:
                    create_author_org_nodes(batch, database_url, database_name)

                elif model == PaperApp.FIELD_OF_STUDY:
                    create_fos_nodes(batch, database_url, database_name)

                elif model == PaperApp.PAPER:
                    create_paper_nodes(batch)

                batch = []

        if batch:
            if model == PaperApp.DOCUMENT_TYPE:
                create_document_type_nodes(batch, database_url, database_name)

            elif model == InstitutionApp.PUBLISHER:
                create_publisher_nodes(batch, database_url, database_name)

            elif model == InstitutionApp.VENUE:
                create_venue_nodes(batch, database_url, database_name)

            elif model == AuthorApp.AUTHOR or model == InstitutionApp.ORGANIZATION:
                create_author_org_nodes(batch, database_url, database_name)

            elif model == PaperApp.FIELD_OF_STUDY:
                create_fos_nodes(batch, database_url, database_name)

            elif model == PaperApp.PAPER:
                create_paper_nodes(batch)

    print(f'\n{model.value} nodes loaded to {database_name} database.')

    gc.collect()




if __name__ == '__main__':
    time_start = None
    time_end = None
    time_elapsed = None

    dotenv_path = join(dirname(__file__), '.env')
    dotenv.load_dotenv(dotenv_path)

    dataset_path = './dataset/dblp.v12.json'
    dataset_encoding = detect_encoding(dataset_path)

    BATCH_SIZE_PAPER_NODES = int(os.environ.get('BATCH_SIZE_PAPER_NODES', 5000))
    paper_nodes_batch = []

    BATCH_SIZE_REQUIRED_NODES = int(os.environ.get('BATCH_SIZE_REQUIRED_NODES', 10000))
    required_nodes_batch = []



    print('==============================')
    print(' Populate Database by Batches')
    print('==============================')
    print('Choose Database:\n1. Production\n2. Test')

    db_option = None
    while db_option not in ['Production', 'Test']:
        db_option = input('\nDatabase: ')

        if db_option not in ['Production', 'Test']:
            print('Invalid Database. Please choose between \'Production\' and \'Test\'.')

    database_url, database_name = neomodel_connect(db_option)

    print(f'Database: {database_name}')



    print('\n=============================================')
    print(f' Create Model nodes - {database_name}')
    print('=============================================')
    print('Select the models nodes to create:')
    print(f'1. {PaperApp.DOCUMENT_TYPE.value}')
    print(f'2. {InstitutionApp.PUBLISHER.value}')
    print(f'3. {InstitutionApp.VENUE.value}')
    print(f'4. {AuthorApp.AUTHOR.value} and {InstitutionApp.ORGANIZATION.value}')
    print(f'5. {PaperApp.FIELD_OF_STUDY.value}')
    print(f'6. {PaperApp.PAPER.value} (It is required to create the previous nodes first)')
    print('7. All')

    model_options = input('\nModels (select by number, separated by comma. Ex: 1,2,3):\n')
    model_options = model_options.split(',')

    while not all([opt in ['1', '2', '3', '4', '5', '6', '7'] for opt in model_options]) \
          or len(model_options) == 0 \
          or ('7' in model_options and len(model_options) > 1):
        print('Invalid input. Please select the model nodes to create.')
        model_options = input('\nModels (select by number, separated by comma. Ex: 1,2,3):\n')
        model_options = model_options.split(',')

        if 6 in model_options:
            pass

    model_options = [int(opt) for opt in model_options]


    time_start = time.time()


    if 1 in model_options or 7 in model_options:
        create_nodes = 'y'

        count_doc_type_nodes = querys.count_nodes(database_url, database_name, PaperApp.DOCUMENT_TYPE)
        if count_doc_type_nodes > 0:
            print(f'\n{PaperApp.DOCUMENT_TYPE.value} has {count_doc_type_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input(f'Do you still want to create {PaperApp.DOCUMENT_TYPE.value} nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':
            populate_db(PaperApp.DOCUMENT_TYPE, dataset_path, dataset_encoding, BATCH_SIZE_REQUIRED_NODES, database_url, database_name)

            count_doc_type_nodes = querys.count_nodes(database_url, database_name, PaperApp.DOCUMENT_TYPE)
            print(f'Total {PaperApp.DOCUMENT_TYPE.value} Nodes: {count_doc_type_nodes}')


    if 2 in model_options or 7 in model_options:
        create_nodes = 'y'

        count_publisher_nodes = querys.count_nodes(database_url, database_name, InstitutionApp.PUBLISHER)
        if count_publisher_nodes > 0:
            print(f'\n{InstitutionApp.PUBLISHER.value} has {count_publisher_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input(f'Do you still want to create {InstitutionApp.PUBLISHER.value} nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':
            populate_db(InstitutionApp.PUBLISHER, dataset_path, dataset_encoding, BATCH_SIZE_REQUIRED_NODES, database_url, database_name)

            count_publisher_nodes = querys.count_nodes(database_url, database_name, InstitutionApp.PUBLISHER)
            print(f'Total {InstitutionApp.PUBLISHER.value} Nodes: {count_publisher_nodes}')


    if 3 in model_options or 7 in model_options:
        create_nodes = 'y'

        count_venue_nodes = querys.count_nodes(database_url, database_name, InstitutionApp.VENUE)
        if count_venue_nodes > 0:
            print(f'\n{InstitutionApp.VENUE.value} has {count_venue_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input(f'Do you still want to create {InstitutionApp.VENUE.value} nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':
            populate_db(InstitutionApp.VENUE, dataset_path, dataset_encoding, BATCH_SIZE_REQUIRED_NODES, database_url, database_name)

            count_venue_nodes = querys.count_nodes(database_url, database_name, InstitutionApp.VENUE)
            print(f'Total {InstitutionApp.VENUE.value} Nodes: {count_venue_nodes}')


    if 4 in model_options or 7 in model_options:
        create_nodes = 'y'

        count_author_nodes = querys.count_nodes(database_url, database_name, AuthorApp.AUTHOR)
        count_org_nodes = querys.count_nodes(database_url, database_name, InstitutionApp.ORGANIZATION)
        if count_author_nodes > 0 or count_org_nodes > 0:
            print(f'\n{AuthorApp.AUTHOR.value} has {count_author_nodes} nodes and {InstitutionApp.ORGANIZATION.value} has {count_org_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input(f'Do you still want to create {AuthorApp.AUTHOR.value} and {InstitutionApp.ORGANIZATION.value} nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':
            populate_db(AuthorApp.AUTHOR, dataset_path, dataset_encoding, BATCH_SIZE_REQUIRED_NODES, database_url, database_name)

            count_author_nodes = querys.count_nodes(database_url, database_name, AuthorApp.AUTHOR)
            count_organization_nodes = querys.count_nodes(database_url, database_name, InstitutionApp.ORGANIZATION)
            print(f'Total {AuthorApp.AUTHOR.value} Nodes: {count_author_nodes}')
            print(f'Total {InstitutionApp.ORGANIZATION.value} Nodes: {count_organization_nodes}')


    if 5 in model_options or 7 in model_options:
        create_nodes = 'y'

        count_fos_nodes = querys.count_nodes(database_url, database_name, PaperApp.FIELD_OF_STUDY)
        if count_fos_nodes > 0:
            print(f'\n{PaperApp.FIELD_OF_STUDY.value} has {count_fos_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input(f'Do you still want to create {PaperApp.FIELD_OF_STUDY.value} nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':
            populate_db(PaperApp.FIELD_OF_STUDY, dataset_path, dataset_encoding, BATCH_SIZE_REQUIRED_NODES, database_url, database_name)

            count_fos_nodes = querys.count_nodes(database_url, database_name, PaperApp.FIELD_OF_STUDY)
            print(f'Total {PaperApp.FIELD_OF_STUDY.value} Nodes: {count_fos_nodes}')
    

    if 6 in model_options or 7 in model_options:
        create_nodes = 'y'

        count_paper_nodes = querys.count_nodes(database_url, database_name, PaperApp.PAPER)
        if count_paper_nodes > 0:
            print(f'\n{PaperApp.PAPER.value} has {count_paper_nodes} nodes created.')

            create_nodes = None
            while create_nodes not in ['y', 'n']:
                create_nodes = input(f'Do you still want to create {PaperApp.PAPER.value} nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':
            populate_db(PaperApp.PAPER, dataset_path, dataset_encoding, BATCH_SIZE_PAPER_NODES, database_url, database_name)

            count_paper_nodes = querys.count_nodes(database_url, database_name, PaperApp.PAPER)
            print(f'Total {PaperApp.PAPER.value} Nodes: {count_paper_nodes}')
    

    time_end = time.time()
    time_elapsed = time_end - time_start
    time_elapsed_hours = time_elapsed // 3600


    print('\n==============================')
    print(' Database Population Finished')
    print('==============================')
    print(f'Database: {database_name}')
    print(f'Time Elapsed: {time_elapsed_hours:.0f} hours {time_elapsed % 3600:.0f} minutes')
    print(f'Total Nodes in Database: {querys.count_nodes(database_url, database_name)}')
    print(f'Total Relationships in Database: {querys.count_relationships(database_url, database_name)}')

    if any([option in model_options for option in [1, 5, 6, 7]]):
        print('\nPaper App Nodes:')
        print(f'\n{PaperApp.PAPER.value} Nodes: {count_paper_nodes}') if 6 in model_options or 7 in model_options else None
        print(f'{PaperApp.DOCUMENT_TYPE.value} Nodes: {count_doc_type_nodes}') if 1 in model_options or 7 in model_options else None
        print(f'{PaperApp.FIELD_OF_STUDY.value} Nodes: {count_fos_nodes}') if 5 in model_options or 7 in model_options else None

    if any([option in model_options for option in [4, 7]]):
        print('\nAuthor App Nodes:')
        print(f'\n{AuthorApp.AUTHOR.value} Nodes: {count_author_nodes}')

    if any([option in model_options for option in [2, 3, 4, 7]]):
        print('\nInstitution App Nodes:')
        print(f'{InstitutionApp.ORGANIZATION.value} Nodes: {count_organization_nodes}') if 4 in model_options or 7 in model_options else None
        print(f'\n{InstitutionApp.PUBLISHER.value} Nodes: {count_publisher_nodes}') if 2 in model_options or 7 in model_options else None
        print(f'{InstitutionApp.VENUE.value} Nodes: {count_venue_nodes}') if 3 in model_options or 7 in model_options else None