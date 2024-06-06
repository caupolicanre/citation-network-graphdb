import os
from os.path import join, dirname
import gc

from typing import Union

import time
import dotenv
import ijson
from tqdm import tqdm

from core.enums.app_enums import AuthorApp, InstitutionApp, PaperApp
from core.enums.db_enums import DatabaseType
from core.funcs import detect_encoding

from database.utils import querys
from database.utils.db_connection import neomodel_connect

from database.load_by_model import create_document_type_nodes, create_publisher_nodes, create_venue_nodes, create_author_org_nodes, create_fos_nodes, create_paper_nodes, create_paper_connections



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
                    create_paper_nodes(batch, database_url, database_name)

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
                create_paper_nodes(batch, database_url, database_name)

    print(f'\n{model.value} nodes loaded to {database_name} database.')

    gc.collect()


def menu_create_models_nodes(database_url: str, database_name: str,
                             dataset_path: str, dataset_encoding: str, batch_size: int,
                             model: Union[AuthorApp, InstitutionApp, PaperApp]) -> None:
    '''
    Menu to create the nodes of the selected model.
    
    Parameters
    ----------
    database_url : str
        URL of the database.
    database_name : str
        Name of the database.
    dataset_path : str
        Path to the dataset.
    dataset_encoding : str
        Encoding of the dataset.
    batch_size : int
        Batch size to load the nodes.
    model : Union[AuthorApp, InstitutionApp, PaperApp]
        Model to populate.
    '''
    create_nodes = 'y'
    count_nodes = querys.count_nodes(database_url, database_name, model)

    if count_nodes > 0:
        print(f'\n{model.value} has {count_nodes} nodes created.')

        create_nodes = None
        while create_nodes not in ['y', 'n']:
            create_nodes = input(f'Do you still want to create {model.value} nodes? (y/n): ')
        
        if create_nodes.lower() == 'y':
            populate_db(model, dataset_path, dataset_encoding, batch_size, database_url, database_name)

            count_nodes = querys.count_nodes(database_url, database_name, model)
            print(f'Total {model.value} Nodes: {count_nodes}')




if __name__ == '__main__':
    time_start = None
    time_end = None
    time_elapsed = None

    dotenv_path = join(dirname(__file__), '.env')
    dotenv.load_dotenv(dotenv_path)

    dataset_path = os.environ.get('DATASET_PATH', './dataset/dblp.v12.json')
    dataset_encoding = detect_encoding(dataset_path)

    BATCH_SIZE_REQUIRED_NODES = int(os.environ.get('BATCH_SIZE_REQUIRED_NODES', 10000))
    BATCH_SIZE_PAPER_NODES = int(os.environ.get('BATCH_SIZE_PAPER_NODES', 5000))
    paper_nodes_batch = []



    print('==============================')
    print(' Populate Database by Batches')
    print('==============================')
    print('Choose Database:')
    print(f'1. {DatabaseType.PRODUCTION.value}')
    print(f'2. {DatabaseType.TEST.value}')

    db_option = None
    while db_option not in [DatabaseType.PRODUCTION.value, DatabaseType.TEST.value]:
        db_option = input('\nDatabase: ')

        if db_option not in [DatabaseType.PRODUCTION.value, DatabaseType.TEST.value]:
            print(f'Invalid Database. Please choose between \'{DatabaseType.PRODUCTION.value}\' and \'{DatabaseType.TEST.value}\'.')

    db_option = DatabaseType(db_option)
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
    print(f'6. {PaperApp.PAPER.value}')
    print(f'7. Create {PaperApp.PAPER.value} connections (It is required to have all previous nodes created first)')
    print('8. All')

    model_options = input('\nModels (select by number, separated by comma. Ex: 1,2,3):\n')
    model_options = model_options.split(',')

    while not all([opt in ['1', '2', '3', '4', '5', '6', '7', '8'] for opt in model_options]) \
          or len(model_options) == 0 \
          or ('8' in model_options and len(model_options) > 1):
            print('Invalid input. Please select the model nodes to create.')
            model_options = input('\nModels (select by number, separated by comma. Ex: 1,2,3):\n')
            model_options = model_options.split(',')

    model_options = [int(opt) for opt in model_options]


    if 7 in model_options or 8 in model_options:
        print('\n=============================================')
        print(f' Create {PaperApp.PAPER.value} connections')
        print('=============================================')
        print(f'Batch size: {BATCH_SIZE_PAPER_NODES}')
        print(f'Select the {PaperApp.PAPER.value} connections to create:')
        print(f'1. {PaperApp.DOCUMENT_TYPE.value}')
        print(f'2. {InstitutionApp.PUBLISHER.value}')
        print(f'3. {InstitutionApp.VENUE.value}')
        print(f'4. {AuthorApp.AUTHOR.value}')
        print(f'5. {PaperApp.FIELD_OF_STUDY.value}')
        print(f'6. {PaperApp.PAPER_CITES_REL.value} (Paper references)')
        print(f'7. All')

        paper_connections_options = input('\nConnections (select by number, separated by comma. Ex: 1,2,3):\n')
        paper_connections_options = paper_connections_options.split(',')

        while not all([opt in ['1', '2', '3', '4', '5', '6', '7'] for opt in paper_connections_options]) \
              or len(paper_connections_options) == 0 \
              or ('7' in paper_connections_options and len(paper_connections_options) > 1):
                print('Invalid input. Please select the connections to create.')
                paper_connections_options = input('\nConnections (select by number, separated by comma. Ex: 1,2,3):\n')
                paper_connections_options = paper_connections_options.split(',')
        
        paper_connections_options = [int(opt) for opt in paper_connections_options]
        paper_connections_models = {
            1: PaperApp.DOCUMENT_TYPE,
            2: InstitutionApp.PUBLISHER,
            3: InstitutionApp.VENUE,
            4: AuthorApp.AUTHOR,
            5: PaperApp.FIELD_OF_STUDY,
            6: PaperApp.PAPER_CITES_REL,
        }

        if 7 in paper_connections_options:
            paper_connections_models_selected = list(paper_connections_models.values())
        else:
            paper_connections_models_selected = [paper_connections_models[opt] for opt in paper_connections_options]



    time_start = time.time()

    if 1 in model_options or 8 in model_options:
        menu_create_models_nodes(database_url, database_name, dataset_path, dataset_encoding, BATCH_SIZE_REQUIRED_NODES, PaperApp.DOCUMENT_TYPE)

    if 2 in model_options or 8 in model_options:
        menu_create_models_nodes(database_url, database_name, dataset_path, dataset_encoding, BATCH_SIZE_REQUIRED_NODES, InstitutionApp.PUBLISHER)

    if 3 in model_options or 8 in model_options:
        menu_create_models_nodes(database_url, database_name, dataset_path, dataset_encoding, BATCH_SIZE_REQUIRED_NODES, InstitutionApp.VENUE)

    if 4 in model_options or 8 in model_options:
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

    if 5 in model_options or 8 in model_options:
        menu_create_models_nodes(database_url, database_name, dataset_path, dataset_encoding, BATCH_SIZE_REQUIRED_NODES, PaperApp.FIELD_OF_STUDY)
    
    if 6 in model_options or 8 in model_options:
        menu_create_models_nodes(database_url, database_name, dataset_path, dataset_encoding, BATCH_SIZE_REQUIRED_NODES, PaperApp.PAPER)
    
    if 7 in model_options or 8 in model_options:
        paper_nodes_batch = []

        print(f'\nCreating {PaperApp.PAPER.value} connections')
        print(f'Models selected: {", ".join([model.value for model in paper_connections_models_selected])}')

        with open(dataset_path, 'r', encoding=dataset_encoding) as f:
            objects = ijson.items(f, 'item')

            for obj in tqdm(objects, desc=f'Creating {PaperApp.PAPER.value} connections', unit=' papers'):
                paper_nodes_batch.append(obj)

                if len(paper_nodes_batch) >= BATCH_SIZE_PAPER_NODES:
                    create_paper_connections(paper_nodes_batch, database_url, database_name, paper_connections_models_selected)
                    paper_nodes_batch = []

            if paper_nodes_batch:
                create_paper_connections(paper_nodes_batch, database_url, database_name, paper_connections_models_selected)

        print(f'\n{PaperApp.PAPER.value} relationships loaded to {database_name} database.')



    time_end = time.time()
    time_elapsed = time_end - time_start
    time_elapsed_hours = time_elapsed // 3600


    print('\n==============================')
    print(' Database Population Finished')
    print('==============================')
    print(f'Database: {database_name}')
    print(f'Time Elapsed: {time_elapsed_hours:.0f} hours {time_elapsed % 3600:.0f} minutes')
    print(f'Total Nodes in Database: {querys.count_nodes(database_url, database_name)}')
    # print(f'Total Relationships in Database: {querys.count_relationships(database_url, database_name)}')

    if any([option in model_options for option in [1, 5, 6, 7, 8]]):
        print('\nPaper App Nodes:')
        if 8 in model_options:
            print(f'\n{PaperApp.PAPER.value} Nodes: {querys.count_nodes(database_url, database_name, PaperApp.PAPER)}')
            print(f'{PaperApp.DOCUMENT_TYPE.value} Nodes: {querys.count_nodes(database_url, database_name, PaperApp.DOCUMENT_TYPE)}')
            print(f'{PaperApp.FIELD_OF_STUDY.value} Nodes: {querys.count_nodes(database_url, database_name, PaperApp.FIELD_OF_STUDY)}')

        elif 6 in model_options:
            print(f'\n{PaperApp.PAPER.value} Nodes: {querys.count_nodes(database_url, database_name, PaperApp.PAPER)}')

        elif 1 in model_options:
            print(f'\n{PaperApp.DOCUMENT_TYPE.value} Nodes: {querys.count_nodes(database_url, database_name, PaperApp.DOCUMENT_TYPE)}')

        elif 5 in model_options:
            print(f'\n{PaperApp.FIELD_OF_STUDY.value} Nodes: {querys.count_nodes(database_url, database_name, PaperApp.FIELD_OF_STUDY)}')


    if any([option in model_options for option in [4, 8]]):
        print('\nAuthor App Nodes:')
        print(f'\n{AuthorApp.AUTHOR.value} Nodes: {count_author_nodes}')


    if any([option in model_options for option in [2, 3, 4, 8]]):
        print('\nInstitution App Nodes:')
        if 8 in model_options:
            print(f'\n{InstitutionApp.ORGANIZATION.value} Nodes: {count_organization_nodes}')
            print(f'{InstitutionApp.PUBLISHER.value} Nodes: {querys.count_nodes(database_url, database_name, InstitutionApp.PUBLISHER)}')
            print(f'{InstitutionApp.VENUE.value} Nodes: {querys.count_nodes(database_url, database_name, InstitutionApp.VENUE)}')
        
        elif 4 in model_options:
            print(f'\n{InstitutionApp.ORGANIZATION.value} Nodes: {count_organization_nodes}')
        
        elif 2 in model_options:
            print(f'\n{InstitutionApp.PUBLISHER.value} Nodes: {querys.count_nodes(database_url, database_name, InstitutionApp.PUBLISHER)}')
        
        elif 3 in model_options:
            print(f'\n{InstitutionApp.VENUE.value} Nodes: {querys.count_nodes(database_url, database_name, InstitutionApp.VENUE)}')