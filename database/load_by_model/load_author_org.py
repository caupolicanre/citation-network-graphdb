import gc
import ijson
from tqdm import tqdm

from neomodel import config, db

from database.utils.funcs import detect_encoding
from database.utils import querys
from database.utils.db_connection import neomodel_connect

from apps.author.models import Author, AuthorOrganizationRel
from apps.institution.models import Organization



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




if __name__ == '__main__':
    dataset_path = './dataset/dblp.v12.json'
    encoding = detect_encoding(dataset_path)

    BATCH_SIZE = 10000
    nodes_batch = []



    print('============================================')
    print(' Populate Database: Author and Organization')
    print('============================================')
    print('Choose Database:\n1. Production\n2. Test')

    db_option = None
    while db_option not in ['Production', 'Test']:
        db_option = input('\nDatabase: ')

        if db_option not in ['Production', 'Test']:
            print('Invalid Database. Please choose between \'Production\' and \'Test\'.')

    database_url, database_name = neomodel_connect(db_option)



    print('\nLoading Authors and Organizations to Graph Database...')

    with open(dataset_path, 'r', encoding=encoding) as f:
        objects = ijson.items(f, 'item')

        for obj in tqdm(objects, desc='Creating Author and Organization nodes', unit=' papers'):
            nodes_batch.append(obj)

            if len(nodes_batch) >= BATCH_SIZE:
                create_author_org_nodes(nodes_batch, database_url, database_name)
                nodes_batch = []

        if nodes_batch:
            create_author_org_nodes(nodes_batch, database_url, database_name)

    print('\nAuthors and Organizations loaded to Graph Database.')

    count_author_nodes = querys.count_nodes('Author', database_url, database_name)
    count_organization_nodes = querys.count_nodes('Organization', database_url, database_name)
    print(f'Total Author Nodes: {count_author_nodes}')
    print(f'Total Organization Nodes: {count_organization_nodes}')