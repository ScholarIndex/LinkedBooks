#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This module contains the schemata for collections in the MongoDB
described by using :py:mod:`mongoengine`.
"""

__author__ = """Giovanni Colavizza, Matteo Romanello"""

import mongoengine
from mongoengine import *
from datetime import datetime

PROVENANCE_FIELD = StringField(
    required=True,
    choices=("groundtruth", "processing", "lbcatalogue")
)


############
# METADATA #
############
class ImgPageStart(EmbeddedDocument):
    """
    The schema of embedded documents in :py:class:`MetadataIssue`.
    """
    type = StringField(required=True, choices=("arab","roman",""))
    img_number = IntField(required=False)
    page_number = IntField(required=False)

class MetadataIssue(EmbeddedDocument):
    """
    The schema of embedded documents in `metadata.issues`.
    """
    foldername = StringField(required=True)
    year = IntField(required=False)
    imgpagestart = EmbeddedDocumentField(ImgPageStart, required=True)
    imgindex = StringField(required=False)
    digitisation_note = StringField(required=False)
    provenance = StringField(required=True)
    issue = StringField(required=True)
    operator = StringField(required=True)
    marked_as_removed = BooleanField(required=True, default=False)

class MetadataTitle(EmbeddedDocument):
    """
    The schema of embedded documents in :py:class:`MetadataRelation` and :py:class:`Metadata`.
    """
    surface = StringField(required=True)
    responsible = StringField(required=True)
    publisher = StringField(required=True)
    materiality = StringField(required=True)
    specifications = StringField(required=True)

class MetadataRelation(EmbeddedDocument):
    """
    The schema of embedded documents in :py:class:`Metadata`.
    """
    type = StringField(required=True)
    title = EmbeddedDocumentField(MetadataTitle,required=True)

class Metadata(DynamicDocument):
    """
    The schema of documents in the `metadata` collection in MongoDB.

    """
    meta = {
        "collection" : "metadata"
    }
    bid = StringField(required=True)
    sbn_id = StringField(required=True)
    creator = StringField(required=True)
    language = StringField(required=True)
    title = EmbeddedDocumentField(MetadataTitle,required=True)
    date = StringField(required=True)
    relations = ListField(EmbeddedDocumentField(MetadataRelation),required=False)
    provenance = StringField(required=True) # partner libraries, empty if more than 1 (for journals)
    operator = StringField(required=True)
    digitisation_note = StringField(required=False)
    img_bib = ListField(required=False)
    type_catalogue = StringField(required=False)
    subjects = ListField(required=False)
    issues = ListField(EmbeddedDocumentField(MetadataIssue), required=False)
    foldername = StringField(required=True)
    type_document = StringField(required=True, choices=("monograph","journal"))
    marked_as_removed = BooleanField(required=True, default=False)
    updated_at = DateTimeField(required=True)
    created_at = DateTimeField(required=True)
#########
# PAGES #
#########
class Token(DynamicEmbeddedDocument):
    """
    The schema of embedded documents in :py:class:`Line`.
    """
    offset_start = IntField(required=True)
    offset_end = IntField(required=True)
    token_number = IntField(required=True)
    surface = StringField(required=True, default="")
    features = ListField(required=False)
    def __repr__(self):
        return "<Token: n=%i, surface=\"%s\">"%(self.token_number
                                                , self.surface)
class Line(DynamicEmbeddedDocument):
    """
    The schema of embedded documents in :py:class:`Page`.
    """
    tokens = ListField(EmbeddedDocumentField(Token), required=False, default=[])
    line_number = IntField(required=False)
    #split_after_line = BooleanField(default=False, required=False)

###############
# ANNOTATIONS #
###############

class PagePosition(EmbeddedDocument):
    """
    The schema of embedded documents in :py:class:`Annotation`.
    """
    page_id = ReferenceField("Page")
    start = IntField(required=True)
    end = IntField(required=True)
    line_n = IntField(required=True)

class Annotation(DynamicDocument):
    """
    The schema of documents in the `annotations` collection in MongoDB.
    """
    meta = {
        "collection": "annotations"
    }
    pageid = StringField(required=False)
    entity_type = StringField(required=True)
    ingestion_timestamp = DateTimeField(required=True)
    positions = ListField(EmbeddedDocumentField("PagePosition"),required=False, default=[])
    bid = StringField(required=True)
    ann_id = StringField(required=True)
    surface = StringField(required=True)
    annotation_ingester_version = StringField(required=False)
    continuation = BooleanField(required=True, default=False)
    container = BooleanField(required=True, default=False)
    filename = StringField(required=True)
    # is there a way to have a conditional required?
    contains = ListField(ReferenceField("Annotation", required = False))
    top_entities = ListField(ReferenceField("Annotation", required = False))

class Page(DynamicDocument):
    """
    The schema of documents in the `page` collection in MongoDB.
    """
    meta = {
        "collection" : "pages"
    }
    dbl_side_scan_number = IntField(required=False)
    single_page_file_number = IntField(required=True)
    fulltext = StringField(required=True, default="")
    filename = StringField(required=True)
    in_index = BooleanField(required=True, default=False)
    in_golden = BooleanField(required=True, default=False)
    is_annotated = BooleanField(required=True, default=False)
    annotations_ids = ListField(ReferenceField(Annotation, required=False, default=[], reverse_delete_rule=mongoengine.PULL))
    lines = ListField(EmbeddedDocumentField(Line),required=False, default=[])
    printed_page_number = ListField(required=False, default=[])
    updated_at = DateTimeField(required=True, default=datetime.utcnow())
    #document_id = ReferenceField('LBDocument')
    def __repr__(self):
        return "<Page: %s, number offset_end lines = %i, in_golden = %s, fulltext = %s (...truncated...)>"%(
                                                        self.printed_page_number
                                                        , len(self.lines)
                                                        , self.in_golden
                                                        , " ".join(self.fulltext.replace("\n","").split()[:20])
                                                        )
#############
# DOCUMENTS #
#############
class PageRef(EmbeddedDocument):
    """
    """
    page_id = ReferenceField(Page, db_field = "_id")

class Index(DynamicEmbeddedDocument):
    """
    The schema of embedded documents in :py:class:`LBDocument`.
    """
    page_ids = ListField(EmbeddedDocumentField(PageRef), required = False, default=[])

class LBDocument(DynamicDocument):
    """
    The schema of documents in the `documents` collection in MongoDB.
    """
    meta = {
        "collection" : "documents"
    }
    metadata_id = ReferenceField(Metadata)
    index = EmbeddedDocumentField(Index)
    ingestion_timestamp = DateTimeField(required=True)
    updated_at = DateTimeField(required=True)
    issue_number = StringField(required=False, db_field = "number", default="")
    internal_id = StringField(required=True)
    bid = StringField(required=True, unique_with="issue_number")
    content_ingester_version = StringField(required=True)
    pages = ListField(ReferenceField(Page), reverse_delete_rule=mongoengine.CASCADE)
    path = StringField(required=True)
    type = StringField(required=True, choices=("monograph","journal_issue"))
    def __repr__(self):
        #return "<Document: mongoid = %s, bid=%s, internal_id=%s, type=%s, number of pages=%i>"%(
        return "<Document: mongoid = %s, bid=%s, internal_id=%s, type=%s>"%(
                                                                                self.id
                                                                                , self.bid
                                                                                , self.internal_id
                                                                                , self.type
                                                                                #, len(self.pages)
                                                                                )
##############
# PROCESSING #
##############
class Processing(DynamicDocument):
    """
    The schema of documents in the `processing` collection in MongoDB.
    """
    meta = {
        "collection": "processing"
    }
    bid = StringField(required=True)
    number = StringField(required=True)
    foldername = StringField(required=True)
    type_document = StringField(required=True, choices=("monograph","issue"))
    dont_process = BooleanField(required=True, default=False)
    is_digitized = BooleanField(required=True, default=False) # if it has been digitised and stored in the NAS
    is_img = BooleanField(required=True, default=False) # if it has been split and stored as jpg on the Image Server folders
    is_ocr = BooleanField(required=True, default=False) # if it has been OCRed using ABBYY
    is_ingested_metadata = BooleanField(required=True, default=False) # if its metadata has been ingested. This step generates a new entry here, so it is always True in theory
    is_ingested_ocr = BooleanField(required=True, default=False) # if the OCR has been ingested, thus the collections documents and pages updated
    is_bibliodbed = BooleanField(required=True, default=False) # if the entries for books, articles, contributions and authors have been added to their respective bibliodb collections
    is_parsed = BooleanField(required=True, default=False) # if references have been parsed, results added to the collection references
    is_disambiguated_s = BooleanField(required=True, default=False) # if full references have been disambiguated, SS
    is_disambiguated_p = BooleanField(required=True, default=False)  # if full references have been disambiguated, PS
    is_disambiguated_partial = BooleanField(required=True, default=False)  # if partial references have been disambiguated, both SS and PS
    updated_at = DateTimeField(required=True)
    created_at = DateTimeField(required=True)


###############
# REFERENCES #
###############
class Reference(DynamicDocument):
    """
    The schema of documents in the `references` collection in MongoDB.
    """
    meta = {
        "collection": "references"
    }
    ref_type = StringField(required=True, choices=("primary","secondary","meta-annotation"))
    document_id = ReferenceField(LBDocument)
    reference_string = StringField(required=True)
    in_golden = BooleanField(required=True,default=False)
    order_in_page = IntField(required=True)
    continuation_candidate_in = BooleanField(required=False,default=False)
    continuation_candidate_out = BooleanField(required=False, default=False)
    continuation = BooleanField(required=False, default=False)
    bid = StringField(required=True)
    issue = StringField(required=True) # this is the number in Documents...
    contents = DictField(required=True)
    start_img_number = IntField(required=True)
    end_img_number = IntField(required=True)
    updated_at = DateTimeField(required=True)
    checked = BooleanField(required=True, default=False)
    correct = BooleanField(required=True, default=True)

    def get_snippet(self, character_size=100):
        """
        Returns text snippet around the reference.
        """
        context_before = ""
        context_after = ""

        fields = [str(field) for field in sorted([int(key) for key in self.contents.keys()])]
        first_field = self.contents[fields[0]]
        last_field = self.contents[fields[-1]]
        try:
            start_page_id = dict(first_field)['page_mongo_id']
            end_page_id = dict(last_field)['page_mongo_id']
            start_page = Page.objects(id=start_page_id).first()
            end_page = Page.objects(id=end_page_id).first()
            assert start_page is not None
            assert end_page is not None
            context_before = start_page.fulltext[first_field["start"]-character_size:first_field["start"]]
            context_after = end_page.fulltext[last_field["end"]:last_field["end"]+character_size]
            return (context_before, self.reference_string, context_after)
        except AssertionError as e:
            return ("", self.reference_string, "")

    def get_containing_publication(self):
        if self.document_id.type=="journal_issue":
            article = Article.objects(document_id=self.document_id
                                    , start_img_number__lte=self.start_img_number
                                    , end_img_number__gte=self.end_img_number)
            return article.first()
        elif self.document_id.type=="monograph":
            try:
                return Book.objects(document_id=self.document_id).get()
            except Exception as e:
                print("Reference %s returned error: %s" % (self.id, e))
                return None


    def __repr__(self):
        return "<Reference: %s>" % self.reference_string

########################
# BIBLIODB COLLECTIONS #
########################
class Author(DynamicDocument):
    """
    The schema of documents in the `bibliodb_authors` collection in MongoDB.
    """
    meta = {
        "collection": "bibliodb_authors",
        "index_background": True,
        "indexes":[
            ("$author_final_form","$surface_forms")
        ]
    }
    # `internal_id` is redundant, that's why I've "removed" it (but still in the DB)
    author_final_form = StringField(required=False, unique_with="viaf_id")
    notes = StringField(required=True, default="")
    checked = BooleanField(required=True, default=False)
    viaf_id = StringField(required=False, default="")
    surface_forms = ListField(required=False, default=[])
    provenance = PROVENANCE_FIELD

    def get_viaf_link(self, viaf_base_uri="http://viaf.org/viaf/"):
        """
        Returns a link to VIAF or None.
        """
        if(self.viaf_id!=""):
            return "%s%s"%(viaf_base_uri, self.viaf_id)
        else:
            return None

    def get_surface_forms(self): #TODO: implement
        """
        To be implemented: find checked disambiguations that refer to this author.
        """
        pass
    def __repr__(self):
        return "<Author: %s (%s)>"%(self.author_final_form, self.get_viaf_link())

class Article(DynamicDocument): #TODO finish
    """
    The schema of documents in the `bibliodb_articles` collection in MongoDB.
    """
    meta = {
        "collection": "bibliodb_articles",
        "index_background": True,
        "indexes":[
            {"fields":("document_id", "start_img_number", "end_img_number")}
        ]

    }
    journal_bid = StringField(required=True)
    journal_short_title = StringField(required=False)
    document_id = ReferenceField(LBDocument)
    title = StringField(required=True)
    internal_id = StringField(required=True)
    year = IntField(required=True)
    volume = StringField(required=True)
    issue_number = StringField(required=False)
    start_img_number = IntField(required=False)
    end_img_number = IntField(required=False)
    start_page_number = IntField(required=False)
    end_page_number = IntField(required=False)
    provenance = PROVENANCE_FIELD
    digitization_provenance = StringField(required=True)

    def get_author(self):
        """
        Get the author(s) of the article by going via the `disambiguation` collection.
        """
        try:
            return [d.author for d in Disambiguation.objects(type="author_of_disambiguation", article=self)]
        except DoesNotExist as e:
            print("Author of article %s does not exist: check this!" % self.id)
            print(e)
            return None

    def __repr__(self):
        journal_title = Journal.objects(bid=self.journal_bid).first().short_title
        return "<Article: \"%s\" in \"%s\", %s(%s) %s, internal_id=%s>"%(self.title
                                                        , journal_title
                                                        , self.volume
                                                        , self.issue_number
                                                        , self.year
                                                        , self.internal_id)

class SBN_Identifier(EmbeddedDocument):
    """
    TODO
    """
    identifier_type = StringField(required=True)
    value = StringField(required=True)

class Journal(DynamicDocument):
    """
    The schema of documents in the `bibliodb_journals` collection in MongoDB.
    """
    meta = {
        "collection": "bibliodb_journals",
    }
    bid = StringField(required=True)
    short_title = StringField(required=True)
    full_title = StringField(required=True)
    identifiers = ListField(EmbeddedDocumentField(SBN_Identifier), required=False)
    sbn_link = URLField(required=False)
    provenance = PROVENANCE_FIELD
    document_id = ReferenceField(LBDocument)
    previous_series = ReferenceField("Journal")
    following_series = ReferenceField("Journal")

    def __repr__(self):
        return "<Journal: %s, bid=%s, link=%s>" % (self.short_title, self.bid, self.sbn_link)

class Disambiguation(DynamicDocument):
    """
    The schema of documents in the `disambiguations` collection in MongoDB.
    """
    meta = {
        "collection": "disambiguations",
        "index_background": True,
        "indexes":[
            {"fields":("type", "author", "article")}
            , {"fields":("type", "author", "book")}
            , {"fields":("type", "reference", "article")}
            , {"fields":("type", "reference", "book")}
            , {"fields":("type", "reference", "archival_document")}
        ]
    }
    surface = StringField(required=False)
    reference = ReferenceField('Reference')
    author = ReferenceField(Author)
    book = ReferenceField('Book')
    journal = ReferenceField('Journal')
    bookpart = ReferenceField('BookPart')
    article = ReferenceField('Article')
    document_id = ReferenceField(LBDocument)
    archival_document = ReferenceField('ArchivalRecordASVE')
    checked = BooleanField(required=True, default=False)
    correct = BooleanField(required=True, default=True)
    updated_at = DateTimeField(required=True, default=datetime.now())
    provenance = PROVENANCE_FIELD
    type = StringField(required=True
                            , choices=(
                                    "author_of_disambiguation"
                                    , "editor_of_disambiguation"
                                    , "reference_disambiguation"
                                    , "in_journal_disambiguation"
                                    , "bookpart_disambiguation"
                                    )
                            )

class Book(DynamicDocument):
    """
    The schema of documents in the `bibliodb_books` collection in MongoDB.
    """
    meta = {
        "collection": "bibliodb_books",
    }
    document_id = ReferenceField(LBDocument)
    provenance = PROVENANCE_FIELD
    digitization_provenance = StringField(required=True)
    bid = StringField(required=True)
    title = StringField(required=True)
    publication_year = StringField(required=True)
    publication_place = StringField(required=True)
    publication_country = StringField(required=True)
    publication_language = StringField(required=True)
    publisher = StringField(required=True)
    identifiers = ListField(EmbeddedDocumentField(SBN_Identifier), required=False)

    def get_author(self):
        """Get book's author(s) via the `disambiguation` collection."""
        try:
            authors = set(
                [
                    d.author
                    for d in Disambiguation.objects(
                        type="author_of_disambiguation",
                        book=self
                    )
                ]
            )
        except DoesNotExist as e:
            print("Author of book %s does not exist: check this!" % self.id)
            print(e)
            return None
        return list(authors)

    def in_library(self):
        """Whether the book is available in the scholar library."""
        try:
            doc = LBDocument.objects(bid=self.bid).get()
            assert doc is not None
            return True
        except Exception as e:
            return False

    def is_open_access(self):
        """Whether the book's digitized images are openly accessible."""
        return False

class ArchivalRecordASVE(DynamicDocument):
    """
    The schema of documents in the `bibliodb_asve` collection in MongoDB.

    TODO use inheritance (in the future)
    """
    meta = {
        "collection": "bibliodb_asve",
    }
    title = StringField(required=True)
    label = StringField(required=False, default=None) # NEW
    archive = StringField(required=False, default=None) # NEW
    url = StringField(required=True)
    notes = StringField(required=False, default="")
    html = StringField(required=False)
    document_type = StringField(required=True, default="")
    size = StringField(required=False, default=None)
    internal_id = StringField(required=True)

    def get_hierarchy(self):

        hierarchical_bit = self.internal_id.split(":")[-1]
        base_id = self.internal_id.replace(":"+hierarchical_bit, '')
        segments = hierarchical_bit.split('.')
        internal_ids = ["%s:%s" % (base_id, ".".join(segments[:i+1])) for i in range(0, len(segments))]
        hierarchy = []
        for i, internal_id in enumerate(internal_ids):
            record = ArchivalRecordASVE.objects(internal_id=internal_id).first()

            if record is not None:
                hierarchy.append({"id":str(record.id)
                                , "internal_id":record.internal_id
                                , "document_type": record.document_type
                                , "level":i
                                , "title":record.title
                                , "current": True if internal_id == self.internal_id else False
                                })
            else:
                hierarchy.append({"id":None
                                , "internal_id": internal_id
                                , "level":i
                                , "title":""
                                , "current": True if internal_id == internal_id else False
                                })

        return hierarchy

    def get_label(self, hierarchical_separator=" >> ", skip_errors=False):

        hierarchical_bit = self.internal_id.split(":")[-1]
        base_id = self.internal_id.replace(":"+hierarchical_bit, '')

        if "." in hierarchical_bit:
            segments = hierarchical_bit.split('.')
            internal_ids = ["%s:%s" % (base_id, ".".join(segments[:i+1])) for i in range(0, len(segments))]
            hierarchical_labels = []
            for internal_id in internal_ids:
                record = ArchivalRecordASVE.objects(internal_id=internal_id).first()
                if skip_errors:

                    if record is not None:
                        hierarchical_labels.append(record.title)
                    else:
                        hierarchical_labels.append("?")
                else:
                    hierarchical_labels.append(record.title)

            return hierarchical_separator.join(hierarchical_labels)
        else:
            return self.title

    def __repr__(self):
        return "<ArchivalRecordASVE: %s (%s)>"%(self.title, self.internal_id)

class Cluster(DynamicDocument):
    pass

class BookPart(DynamicDocument):
    pass
