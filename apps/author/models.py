from neomodel import (
    StructuredNode, StructuredRel,
    UniqueIdProperty, StringProperty, IntegerProperty,
    RelationshipTo, RelationshipFrom, ZeroOrMore
)


class AuthorOrganizationRel(StructuredRel):
    author_org_id = UniqueIdProperty()

    @property
    def papers(self):
        pass


class Author(StructuredNode):
    author_id = IntegerProperty(unique_index=True, required=True)
    name = StringProperty(required=True)
    organization = RelationshipTo('apps.institution.models.Organization', 'AFFILIATED_WITH', cardinality=ZeroOrMore, model=AuthorOrganizationRel)
    paper = RelationshipFrom('apps.paper.models.Paper', 'AUTHORED_BY', cardinality=ZeroOrMore)

    def __str__(self) -> str:
        return self.name
