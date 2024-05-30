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

from apps.author.models import AuthorOrganizationRel


# Deprecated
class Institution(StructuredNode):
    inst_id = UniqueIdProperty()
    name = StringProperty(unique_index=True, required=True)

    def __str__(self):
        return self.name


class Organization(StructuredNode):
    org_id = UniqueIdProperty()
    name = StringProperty(unique_index=True, required=True)
    author = RelationshipFrom('apps.author.models.Author', 'AFFILIATED_WITH', cardinality=ZeroOrMore, model=AuthorOrganizationRel)

    def __str__(self):
        return self.name


class Publisher(StructuredNode):
    pub_id = UniqueIdProperty()
    name = StringProperty(unique_index=True, required=True)
    paper = RelationshipFrom('apps.paper.models.Paper', 'PUBLISHED_BY', cardinality=ZeroOrMore)

    def __str__(self):
        return self.name


class VenueType(StructuredNode):
    venue_type_id = UniqueIdProperty()
    type = StringProperty(unique_index=True, required=True)

    def __str__(self):
        return self.type


class Venue(StructuredNode):
    venue_id = UniqueIdProperty()
    name = StringProperty(unique_index=True, required=True)
    type = RelationshipTo('VenueType', 'OF_TYPE', cardinality=One)
    paper = RelationshipFrom('apps.paper.models.Paper', 'PRESENTED_AT', cardinality=ZeroOrMore)

    def __str__(self):
        return f'{self.name} ({self.type.type})'