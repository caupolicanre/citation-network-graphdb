import os
from os.path import join, dirname

import dotenv

from neo4j import GraphDatabase
import neomodel
from neomodel import (
    config, StructuredNode, StructuredRel,
    UniqueIdProperty, StringProperty, IntegerProperty, FloatProperty, BooleanProperty,
    DateProperty, DateTimeProperty, DateTimeFormatProperty, AliasProperty, JSONProperty, ArrayProperty,
    Relationship, RelationshipTo, RelationshipFrom, One, ZeroOrOne, ZeroOrMore, OneOrMore
)


class DocumentType(StructuredNode):
    type_id = UniqueIdProperty()
    type = StringProperty(unique_index=True, required=True)

    def __str__(self):
        return self.type


class FieldOfStudy(StructuredNode):
    fos_id = UniqueIdProperty()
    name = StringProperty(unique_index=True, required=True)
    # codename = StringProperty(unique_index=True, required=True)

    def __str__(self):
        return self.name


class PaperFieldOfStudyRel(StructuredRel):
    paper_fos_id = UniqueIdProperty()
    weight = FloatProperty(default=0.0)


class Paper(StructuredNode):
    paper_id = IntegerProperty(unique_index=True, required=True)
    title = StringProperty(unique_index=True, required=True)
    doi = StringProperty(default=None)
    year = DateTimeFormatProperty(format='%Y')
    page_start = IntegerProperty(max_length=4, default=None)
    page_end = IntegerProperty(max_length=4, default=None)
    volume = IntegerProperty(max_length=4, default=None)
    issue = IntegerProperty(max_length=4, default=None)
    n_citation = IntegerProperty(default=0)

    type = RelationshipTo('DocumentType', 'OF_TYPE', cardinality=One)
    publisher = RelationshipTo('apps.institution.models.Publisher', 'PUBLISHED_BY', cardinality=One)
    venue = RelationshipTo('apps.institution.models.Venue', 'PRESENTED_AT', cardinality=ZeroOrOne)
    author = RelationshipTo('apps.author.models.Author', 'AUTHORED_BY', cardinality=OneOrMore)
    field_of_study = Relationship('FieldOfStudy', 'RELATED_TO', cardinality=ZeroOrMore, model=PaperFieldOfStudyRel)
    reference = Relationship('Paper', 'CITES', cardinality=ZeroOrMore)

    def __str__(self):
        return self.title