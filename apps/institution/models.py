import os
from os.path import join, dirname

import dotenv

from neo4j import GraphDatabase
import neomodel
from neomodel import (
    config, StructuredNode, StructuredRel,
    UniqueIdProperty, StringProperty, IntegerProperty, FloatProperty, BooleanProperty,
    DateProperty, DateTimeProperty, AliasProperty, JSONProperty, ArrayProperty,
    Relationship, RelationshipTo, RelationshipFrom, One, ZeroOrOne, ZeroOrMore, OneOrMore
)

from apps.author.models import AuthorOrganizationRel


class Institution(StructuredNode):
    inst_id = UniqueIdProperty()
    name = StringProperty(unique_index=True, required=True)

    def __str__(self):
        return self.name


class Organization(Institution):
    org_id = UniqueIdProperty()
    author = RelationshipFrom('apps.author.models.Author', 'AFFILIATED_WITH', cardinality=ZeroOrMore, model=AuthorOrganizationRel)


class Publisher(Institution):
    pub_id = UniqueIdProperty()


class VenueType(StructuredNode):
    venue_type_id = UniqueIdProperty()
    type = StringProperty(unique_index=True, required=True)


class Venue(Institution):
    venue_id = IntegerProperty(unique_index=True, required=True)
    type = Relationship('VenueType', 'OF_TYPE', cardinality=One)

    def __str__(self):
        return f'{self.name} ({self.type.type})'