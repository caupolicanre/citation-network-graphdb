import gc
from datetime import datetime

from neomodel import config, db

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


def create_paper_nodes(nodes: list, database_url: str, database_name: str) -> None:
    '''
    Create nodes for each paper in the dataset with neomodel.
    Using the Paper model.
    The relationships are created with the following models, so they need to be created before this function is called:
        - DocumentType
        - Publisher
        - Venue
        - Author
        - Organization
        - FieldOfStudy
    
    Parameters
    ----------
    nodes : list
        A list of dictionaries containing the papers information.
        Required fields:
            - id: int (required)
                Paper ID.
            - title: str (required)
                Paper title.
            - authors: list (required at least one author)
                List of authors.
                - name: str (required)
                    Author name.
        Optional fields:
            - doi: str
                DOI of the paper.
            - year: int
                Year of the paper.
            - page_start: int
                Page start of the paper.
            - page_end: int
                Page end of the paper.
            - volume: int
                Volume of the paper.
            - issue: int
                Issue of the paper.
            - n_citation: int
                Number of citations of the paper.
            - doc_type: str
                Document type of the paper.
            - publisher: str
                Publisher of the paper.
            - venue: dict
                Venue of the paper.
                - raw: str (required)
                    Venue name.
            - fos: list
                List of field of study.
                - name: str (required)
                    Field of study name.
                - w: float (optional)
                    Weight of the field of study. Default is 0.0.
            - references: list
                List of paper IDs that the paper references.

    database_url : str (required)
        URL of the database.
    database_name : str (required)
        Name of the database.
    '''
    config.DATABASE_URL = database_url
    config.DATABASE_NAME = database_name

    with db.transaction:
        for obj in nodes:
            paper_id = int(obj['id'])
            title = obj['title']
            doi = obj.get('doi', None)
            year = obj.get('year', None)
            page_start = obj.get('page_start', None)
            page_end = obj.get('page_end', None)
            volume = obj.get('volume', None)
            issue = obj.get('issue', None)
            n_citation = obj.get('n_citation', 0)

            doc_type = obj.get('doc_type', None)
            publisher = obj.get('publisher', None)
            venue = obj.get('venue', None)
            authors = obj.get('authors', []) # List of Dict
            fields_of_study = obj.get('fos', []) # List of Dict
            references = obj.get('references', []) # List of Int


            # Clean up paper data
            if doi == '':
                doi = None
            if year:
                year = datetime.strptime(str(year), '%Y')
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
                    doi=doi,
                    year=year,
                    page_start=page_start,
                    page_end=page_end,
                    volume=volume,
                    issue=issue,
                    n_citation=n_citation
                ).save()


                if doc_type:
                    doc_type_node = DocumentType.nodes.get(type=doc_type)

                    if not paper.type.is_connected(doc_type_node):
                        paper.type.connect(doc_type_node)


                if publisher:
                    publisher_node = Publisher.nodes.get(name=publisher)
                    
                    if not paper.publisher.is_connected(publisher_node):
                        paper.publisher.connect(publisher_node)
                

                if venue:
                    venue_node = Venue.nodes.get(name=venue['raw'])

                    if not paper.venue.is_connected(venue_node):
                        paper.venue.connect(venue_node)


                for author in authors:
                    author_name = author.get('name', None)

                    author_node = Author.nodes.get(name=author_name)
                    
                    if not paper.author.is_connected(author_node):
                        paper.author.connect(author_node)


                for fos in fields_of_study:
                    fos_name = fos.get('name', None)
                    fos_weight = fos.get('w', 0.0)

                    fos_node = FieldOfStudy.nodes.get(name=fos_name)
                    
                    if not paper.field_of_study.is_connected(fos_node):
                        paper.field_of_study.connect(fos_node, {'weight': fos_weight})


                for ref_id in references:
                    ref_paper = Paper.nodes.get_or_none(paper_id=ref_id)
                    if ref_paper:
                        paper.reference.connect(ref_paper)

    gc.collect()