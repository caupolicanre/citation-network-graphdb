import gc
import ijson
from tqdm import tqdm

from neomodel import config, db

from database.utils.funcs import detect_encoding
from database.utils.db_connection import neomodel_connect

from apps.paper.models import FieldOfStudy



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
    dataset_path = './dataset/dblp.v12.json'
    encoding = detect_encoding(dataset_path)

    BATCH_SIZE = 10000
    nodes_batch = []



    print('===================================')
    print(' Populate Database: Field of Study')
    print('===================================')
    print('Choose Database:\n1. Production\n2. Test')

    db_option = None
    while db_option not in ['Production', 'Test']:
        db_option = input('\nDatabase: ')

        if db_option not in ['Production', 'Test']:
            print('Invalid Database. Please choose between \'Production\' and \'Test\'.')

    database_url, database_name = neomodel_connect(db_option)



    print('\nLoading Fields of Study to Graph Database...')

    with open(dataset_path, 'r', encoding=encoding) as f:
        objects = ijson.items(f, 'item')

        for obj in tqdm(objects, desc='Creating nodes', unit=' papers'):
            nodes_batch.append(obj)

            if len(nodes_batch) >= BATCH_SIZE:
                create_fos_nodes(nodes_batch, database_url, database_name)
                nodes_batch = []

        if nodes_batch:
            create_fos_nodes(nodes_batch, database_url, database_name)

    print('\nFields of Study loaded to Graph Database.')