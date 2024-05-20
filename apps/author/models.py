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


class AuthorOrganizationRel(StructuredRel):
    author_org_id = UniqueIdProperty()

    @property
    def papers(self):
        pass


class Author(StructuredNode):
    author_id = IntegerProperty(unique_index=True, required=True)
    name = StringProperty(unique_index=True, required=True)
    organization = RelationshipTo('apps.institution.models.Organization', 'AFFILIATED_WITH', cardinality=ZeroOrMore, model=AuthorOrganizationRel)
    paper = Relationship('apps.paper.model.Paper', 'AUTHORED_BY', cardinality=ZeroOrMore)

    def __str__(self):
        return self.name