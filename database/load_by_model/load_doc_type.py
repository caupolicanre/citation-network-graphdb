import gc
import ijson
from tqdm import tqdm

from neomodel import config, db

from database.utils.funcs import detect_encoding
from database.utils.db_connection import neomodel_connect

from apps.paper.models import DocumentType



def create_document_type_nodes(nodes: list, database_url: str, database_name: str) -> None:
    '''
    Create nodes for each document type in the dataset with neomodel.
    Using the DocumentType model.
    
    Parameters
    ----------
    nodes : list
        A list of dictionaries containing the document type's information.
        Required fields:
            - doc_type: string (required)
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




if __name__ == '__main__':
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

    database_url, database_name = neomodel_connect(db_option)



    print('\nLoading Document Types to Graph Database...')

    with open(dataset_path, 'r', encoding=encoding) as f:
        objects = ijson.items(f, 'item')

        for obj in tqdm(objects, desc='Creating nodes', unit=' papers'):
            nodes_batch.append(obj)

            if len(nodes_batch) >= BATCH_SIZE:
                create_document_type_nodes(nodes_batch, database_url, database_name)
                nodes_batch = []

        if nodes_batch:
            create_document_type_nodes(nodes_batch, database_url, database_name)

    print('\nDocument Types loaded to Graph Database.')