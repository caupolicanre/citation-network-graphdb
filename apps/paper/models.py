from neomodel import (
    StructuredNode, StructuredRel,
    UniqueIdProperty, StringProperty, IntegerProperty, FloatProperty,
    DateTimeFormatProperty,
    RelationshipTo, One, ZeroOrOne, ZeroOrMore, OneOrMore
)



class DocumentType(StructuredNode):
    type_id = UniqueIdProperty()
    type = StringProperty(unique_index=True, required=True)

    def __str__(self) -> str:
        return self.type


class FieldOfStudy(StructuredNode):
    fos_id = UniqueIdProperty()
    name = StringProperty(unique_index=True, required=True)

    def __str__(self) -> str:
        return self.name


class PaperFieldOfStudyRel(StructuredRel):
    paper_fos_id = UniqueIdProperty()
    weight = FloatProperty(default=0.0)


class Paper(StructuredNode):
    paper_id = IntegerProperty(unique_index=True, required=True)
    title = StringProperty(required=True)
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
    field_of_study = RelationshipTo('FieldOfStudy', 'RELATED_TO', cardinality=ZeroOrMore, model=PaperFieldOfStudyRel)
    reference = RelationshipTo('Paper', 'CITES', cardinality=ZeroOrMore)

    def __str__(self) -> str:
        return self.title
