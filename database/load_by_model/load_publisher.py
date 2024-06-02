import gc
import ijson
from tqdm import tqdm

from neomodel import config, db

from database.utils.funcs import detect_encoding
from database.utils.db_connection import neomodel_connect

from apps.institution.models import Publisher



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




if __name__ == '__main__':
    dataset_path = './dataset/dblp.v12.json'
    encoding = detect_encoding(dataset_path)

    BATCH_SIZE = 10000
    nodes_batch = []



    print('==============================')
    print(' Populate Database: Publisher')
    print('==============================')
    print('Choose Database:\n1. Production\n2. Test')

    db_option = None
    while db_option not in ['Production', 'Test']:
        db_option = input('\nDatabase: ')

        if db_option not in ['Production', 'Test']:
            print('Invalid Database. Please choose between \'Production\' and \'Test\'.')

    database_url, database_name = neomodel_connect(db_option)



    print('\nLoading Publishers to Graph Database...')

    with open(dataset_path, 'r', encoding=encoding) as f:
        objects = ijson.items(f, 'item')

        for obj in tqdm(objects, desc='Creating nodes', unit=' papers'):
            nodes_batch.append(obj)

            if len(nodes_batch) >= BATCH_SIZE:
                create_publisher_nodes(nodes_batch, database_url, database_name)
                nodes_batch = []

        if nodes_batch:
            create_publisher_nodes(nodes_batch, database_url, database_name)

    print('\nPublishers loaded to Graph Database.')