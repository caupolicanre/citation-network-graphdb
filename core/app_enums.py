from enum import Enum



class AuthorApp(Enum):
    AUTHOR = 'Author'
    AUTHOR_ORG_REL = 'AuthorOrganizationRel'


class InstitutionApp(Enum):
    INSTITUTION = 'Institution'
    ORGANIZATION = 'Organization'
    PUBLISHER = 'Publisher'
    VENUE_TYPE = 'VenueType'
    VENUE = 'Venue'


class PaperApp(Enum):
    PAPER = 'Paper'
    DOCUMENT_TYPE = 'DocumentType'
    FIELD_OF_STUDY = 'FieldOfStudy'
    PAPER_FOS_REL = 'PaperFieldOfStudyRel'