import gc
import ijson
from tqdm import tqdm

from neomodel import config, db

from database.utils.funcs import detect_encoding
from database.utils.db_connection import neomodel_connect

from apps.institution.models import Venue, VenueType



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




if __name__ == '__main__':
    dataset_path = './dataset/dblp.v12.json'
    encoding = detect_encoding(dataset_path)

    BATCH_SIZE = 10000
    nodes_batch = []



    print('==========================')
    print(' Populate Database: Venue')
    print('==========================')
    print('Choose Database:\n1. Production\n2. Test')

    db_option = None
    while db_option not in ['Production', 'Test']:
        db_option = input('\nDatabase: ')

        if db_option not in ['Production', 'Test']:
            print('Invalid Database. Please choose between \'Production\' and \'Test\'.')

    database_url, database_name = neomodel_connect(db_option)



    print('\nLoading Venues to Graph Database...')

    with open(dataset_path, 'r', encoding=encoding) as f:
        objects = ijson.items(f, 'item')

        for obj in tqdm(objects, desc='Creating Venue nodes', unit=' papers'):
            nodes_batch.append(obj)

            if len(nodes_batch) >= BATCH_SIZE:
                create_venue_nodes(nodes_batch, database_url, database_name)
                nodes_batch = []

        if nodes_batch:
            create_venue_nodes(nodes_batch, database_url, database_name)

    print('\nVenues loaded to Graph Database.')