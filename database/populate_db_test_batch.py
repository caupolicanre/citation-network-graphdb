import os
from os.path import join, dirname

import time
from datetime import datetime
import dotenv
import ijson
from tqdm import tqdm

from neo4j import GraphDatabase
from neomodel import config, db

from database.utils.funcs import detect_encoding

from apps.author.models import Author, AuthorOrganizationRel
from apps.institution.models import Organization, Publisher, Venue, VenueType
from apps.paper.models import Paper, FieldOfStudy, PaperFieldOfStudyRel, DocumentType


dotenv_path = join(dirname(__file__), '.env')
dotenv.load_dotenv(dotenv_path)

URI = os.environ.get('DB_URI')
TEST_DB_NAME = os.environ.get('TEST_DB_NAME')
TEST_DB_USER = os.environ.get('TEST_DB_USER')
TEST_DB_PASS = os.environ.get('TEST_DB_PASS')
AUTH = (TEST_DB_USER, TEST_DB_PASS)

config.DATABASE_URL = f'bolt://{AUTH[0]}:{AUTH[1]}@{URI}'
config.DATABASE_NAME = TEST_DB_NAME


dataset_path = './dataset/dblp.v12.json'
encoding = detect_encoding(dataset_path)

BATCH_SIZE = 5000


def create_nodes_batch(nodes):
    with db.transaction:
        for obj in nodes:
            paper_id = int(obj['id']) # Int
            title = obj['title'] # String
            doi = obj.get('doi', None) # String
            year = obj.get('year', None) # Int Date Format Y
            page_start = obj.get('page_start', None) # Int
            page_end = obj.get('page_end', None) # Int
            volume = obj.get('volume', None) # Int
            issue = obj.get('issue', None) # Int
            n_citation = obj.get('n_citation', 0) # Int

            doc_type = obj.get('doc_type', None)
            publisher = obj.get('publisher', None)
            venue = obj.get('venue', None)
            authors = obj.get('authors', []) # List of Dict
            fields_of_study = obj.get('fos', []) # List of Dict
            references = obj.get('references', []) # List of Int

            if page_start:
                page_start = int(''.join(filter(str.isdigit, page_start)))
            
            if page_end:
                page_end = int(''.join(filter(str.isdigit, page_end)))
            
            if volume:
                volume = int(''.join(filter(str.isdigit, volume)))
            
            if issue:
                issue = int(''.join(filter(str.isdigit, issue)))


            paper_node = Paper.nodes.get_or_none(title=title)

            if not paper_node:

                paper = Paper(
                    paper_id=paper_id,
                    title=title,
                    doi=doi if (doi is not None and doi != '') else None,
                    year=datetime.strptime(str(year), '%Y') if year else None,
                    page_start=int(page_start) if page_start else None,
                    page_end=int(page_end) if page_end else None,
                    volume=int(volume) if volume else None,
                    issue=int(issue) if issue else None,
                    n_citation=n_citation
                ).save()


                if doc_type:
                    doc_type_node = DocumentType.nodes.get_or_none(type=doc_type)
                    if not doc_type_node:
                        doc_type_node = DocumentType(type=doc_type).save()

                    if not paper.type.is_connected(doc_type_node):
                        paper.type.connect(doc_type_node)


                if publisher:
                    publisher_node = Publisher.nodes.get_or_none(name=publisher)
                    if not publisher_node:
                        publisher_node = Publisher(name=publisher).save()
                    
                    if not paper.publisher.is_connected(publisher_node):
                        paper.publisher.connect(publisher_node)


                for author_data in authors:
                    author_id = int(author_data['id'])
                    author_node = Author.nodes.get_or_none(author_id=author_id)

                    # If not found by author_id, try to find by name
                    if not author_node:
                        author_node = Author.nodes.get_or_none(name=author_data['name'])

                    # If not found at all, create a new node
                    if not author_node:
                        author_node = Author(author_id=author_id, name=author_data['name']).save()
                    
                    if not paper.author.is_connected(author_node):
                        paper.author.connect(author_node)

                    if author_data.get('org'):
                        organization_node = Organization.nodes.get_or_none(name=author_data['org'])
                        if not organization_node:
                            organization_node = Organization(name=author_data['org']).save()
                        
                        if not author_node.organization.is_connected(organization_node):
                            author_node.organization.connect(organization_node)


                for fos_data in fields_of_study:
                    fos_node = FieldOfStudy.nodes.get_or_none(name=fos_data['name'])
                    if not fos_node:
                        fos_node = FieldOfStudy(name=fos_data['name']).save()
                    
                    if not paper.field_of_study.is_connected(fos_node):
                        paper.field_of_study.connect(fos_node, {'weight': fos_data["w"]})


                for ref_id in references:
                    ref_paper = Paper.nodes.get_or_none(paper_id=ref_id)
                    if ref_paper:
                        paper.reference.connect(ref_paper)


                if venue:
                    venue_node = Venue.nodes.get_or_none(name=venue['raw'])

                    if not venue_node:
                        venue_node = Venue(name=venue['raw']).save()

                        if venue.get('type'):
                            venue_type_node = VenueType.nodes.get_or_none(type=venue['type'])
                            if not venue_type_node:
                                venue_type_node = VenueType(type=venue['type']).save()
                        
                            if not venue_node.type.is_connected(venue_type_node):
                                venue_node.type.connect(venue_type_node)

                    paper.venue.connect(venue_node)

    time.sleep(0.1) # Sleep to free up resources



print('\nStarting to populate the Graph Database')


nodes_batch = []

with open(dataset_path, 'r', encoding=encoding) as f:
    objects = ijson.items(f, 'item')

    for obj in tqdm(objects, desc='Creating nodes', unit=' papers'):
        nodes_batch.append(obj)
        if len(nodes_batch) >= BATCH_SIZE:
            create_nodes_batch(nodes_batch)
            nodes_batch = []

    if nodes_batch:
        create_nodes_batch(nodes_batch)


print('\nGraph Database loaded successfully')