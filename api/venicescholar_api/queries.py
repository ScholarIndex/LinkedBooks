import sys
sys.path += ["../../", "../"]
from flask_restplus import Namespace, Resource, fields
from flask import current_app
from commons.dbmodels import Author as AuthorModel
from commons.dbmodels import Book as BookModel
from commons.dbmodels import Article as ArticleModel
from commons.dbmodels import ArchivalRecordASVE as ArchivalRecordASVEModel
from commons.dbmodels import Disambiguation as DisambiguationModel
from commons.dbmodels import Reference as ReferenceModel
from commons.dbmodels import Page as PageModel
from commons.dbmodels import Annotation as AnnotationModel
from commons.dbmodels import Processing as ProcessingModel


def get_publications(author_id):
    """
    Get a list of publications by the author.
    """
    # TODO: remove the workaround of using a set()
    # instead, add  a unique constraint to the datamodel
    # what should not happen is that the same disambiguation is added
    # twice (or more) to the database

    articles = set()
    books = set()
    contributions = set()

    print("Fetching publications for author %s" % author_id)

    for disambiguation in DisambiguationModel.objects(type="author_of_disambiguation", author=author_id):

        if disambiguation.article is not None:
            disambiguation.article.author = disambiguation.article.get_author()
            articles.add(disambiguation.article)

        elif disambiguation.book is not None:
            disambiguation.book.author = disambiguation.book.get_author()
            book = disambiguation.book
            # is_digitized and is_oa are dynamic fields and needs to be
            # instantiated before passing on to marshalling
            book.is_digitized = book.in_library()
            book.is_oa = book.is_open_access()
            books.add(book)

        elif disambiguation.bookpart is not None:
            pass # TODO implement at some point

    print("Fetching publications for author %s: done" % author_id)

    return {
        'articles': list(articles)
        , 'books': list(books)
        , 'contributions': list(contributions)
    }


def get_occurrences_count(author_id):
    """
    TODO
    """
    return DisambiguationModel.objects(author=author_id).count()


def _get_cited_publications(publication):
    """
    Logic
    - given a publication get all its references
    - for each reference get the corresp disambiguation
    - for each disambiguation get the pointed publication and add it to result list
    """
    cited_publications = {}
    try:
        doc_id = publication.document_id.id
    except AttributeError as e:
        return []

    if type(publication)==ArticleModel:
        # in the case of a journal article, it's not enough to filter by `document_id` (i.e. the journal issue)
        # but additionally, also by page range
        references_ids = [ref.id
                         for ref in ReferenceModel.objects(document_id=doc_id)
                         if ref.contents[list(ref.contents.keys())[0]]["single_page_file_number"]>=publication.start_img_number
                            and ref.contents[list(ref.contents.keys())[-1]]["single_page_file_number"]<=publication.end_img_number]
    else:
        references_ids = [ref.id for ref in ReferenceModel.objects(document_id=doc_id)]

    disambiguations = DisambiguationModel.objects(reference__in=references_ids)

    for disambiguation in disambiguations:

        reference = disambiguation.reference
        containing_publication = reference.get_containing_publication()
        reference.containing_document_type = "article" if type(containing_publication)==ArticleModel else "book"
        reference.containing_document_id = containing_publication.id

        if disambiguation.book is not None:

            book = disambiguation.book
            book.author = book.get_author()
            book.is_digitized = book.in_library()
            book.is_oa = book.is_open_access()

            for author in book.author:
                if author is not None:
                    try:
                        author.viaf_link = author.get_viaf_link()
                    except Exception as e:
                        print(book.id)
                        raise e


            if disambiguation.book.id not in cited_publications:
                book.incoming_references = [reference]
                cited_publications[book.id] = book
            else:
                cited_publications[book.id].incoming_references.append(reference)

        # TODO: add citations pointing to journal articles (we still don't have any in the DB
        # thus, it doesn't make much sense to implement it

        elif disambiguation.archival_document is not None:
            if disambiguation.archival_document.id not in cited_publications:
                archival_document = disambiguation.archival_document
                archival_document.hierarchy = archival_document.get_hierarchy()
                archival_document.incoming_references = [reference]
                cited_publications[archival_document.id] = archival_document
            else:
                cited_publications[archival_document.id].incoming_references.append(reference)

    return list(cited_publications.values())


def get_cited_publications(seed_publications):
    """
    Get the publications cited by an input set of publications.

    TODO: list of distinct publications is ok, but each publ needs to have the references pointing to it attached.

    """
    if type(seed_publications)==dict:
        seed_publications = seed_publications["articles"] + seed_publications["books"] + seed_publications["contributions"]
    else:
        seed_publications = [seed_publications]

    print("Fetching cited publications (%i seeds)" % len(seed_publications))

    cited_publications = []

    for pub in seed_publications:
        cited_publications += _get_cited_publications(pub)

    print("Fetching cited publications, done")

    if len(cited_publications) == 0:
        return {"articles":[], "books":[],"primary_sources":[]}
    else:
        return {
            'articles': [publ for publ in cited_publications if type(publ)==ArticleModel]
            , 'books': [publ for publ in cited_publications if type(publ)==BookModel]
            , 'primary_sources': [publ for publ in cited_publications if type(publ)==ArchivalRecordASVEModel]
            , 'contributions':[]
        }


def _get_citing_publications(publication):
    """
    ZLogic:
    - given a publication get all the disambiguations having in `article`, `book` (or `bookpart`) and archival record
         the `publication.id`
    - for each disambiguation get the corresp reference
    - for each reference get the containing documennt => corresponding record in the right bibliodb
    - do a distinct on the results before returning them
    """
    citing_publications = {}
    disambiguations = []

    if type(publication)==ArticleModel:
        disambiguations = DisambiguationModel.objects(type="reference_disambiguation", article=publication)
    elif type(publication)==BookModel:
        disambiguations = DisambiguationModel.objects(type="reference_disambiguation", book=publication)
    elif type(publication)==ArchivalRecordASVEModel:
        disambiguations = DisambiguationModel.objects(type="reference_disambiguation", archival_document=publication)

    for disambiguation in disambiguations:

        reference = disambiguation.reference
        citing_publication = reference.get_containing_publication()

        if citing_publication is not None:

            if type(publication) != ArchivalRecordASVEModel:
                citing_publication.author = citing_publication.get_author()

            if isinstance(citing_publication, BookModel):
                citing_publication.is_digitized = citing_publication.in_library()
                citing_publication.is_oa = citing_publication.is_open_access()

            if citing_publication.id not in citing_publications:
                citing_publications[citing_publication.id] = citing_publication
                citing_publications[citing_publication.id].incoming_references = [reference]
            else:
                citing_publications[citing_publication.id].incoming_references.append(reference)

    return list(citing_publications.values())


def get_citing_publications(seed_publications):
    """
    Get the publications that cite an input set of publications.
    :param seed_publications: a dictionary with keys: "articles", "books", "contributions"
    :return:
    """
    # for each publication in seed get citing publication
    # and put all results together
    # then filter the list of dicts and dispatch into the correct type sub-dict

    if type(seed_publications)==dict:
        seed_publications = seed_publications["articles"] + seed_publications["books"] + seed_publications["contributions"]
    else:
        seed_publications = [seed_publications]

    print("Fetching citing publications (%i seeds)" % len(seed_publications))

    citing_publications = []

    for pub in seed_publications:
        citing_publications += _get_citing_publications(pub)

    print("Fetching citing publications, done")

    if len(citing_publications) == 0:
        return {"articles":[], "books":[]}
    else:
        return {
            'articles': [publ for publ in citing_publications if type(publ)==ArticleModel]
            , 'books': [publ for publ in citing_publications if type(publ)==BookModel]
            , 'contributions':[]
        }


def get_statistics():
    """Return basic statistics about the database contents."""

    current_app.logger.info("preparing statistics")
    cited_books = DisambiguationModel.objects(
        type="reference_disambiguation",
        book__ne=None
    ).distinct('book')
    current_app.logger.info("N cited books: {}".format(len(cited_books)))

    cited_articles = DisambiguationModel.objects(
        type="reference_disambiguation",
        article__ne=None
    ).distinct('article')
    current_app.logger.info(
        "N cited articles: {}".format(len(cited_articles))
    )

    cited_book_authors = DisambiguationModel.objects(
        type="author_of_disambiguation",
        book__in=cited_books
    ).distinct('author')
    cited_article_authors = DisambiguationModel.objects(
        type="author_of_disambiguation",
        article__in=cited_articles
    ).distinct('author')
    cited_authors = len(list(set(cited_book_authors + cited_article_authors)))

    # NB: filtering only `is_ingested_ocr=True` returns (for now) less results
    book_bids = ProcessingModel.objects(
        type_document="monograph"
    ).distinct('bid')
    journal_bids = ProcessingModel.objects(
        type_document="issue"
    ).distinct('bid')

    return [
        {
            'id': 'total_scans',
            'label': 'scans',
            'value': PageModel.objects.count()
        },
        {
            'id': 'digitized_books',
            'label': 'books',
            'value': ProcessingModel.objects(
                type_document="monograph",
                is_digitized=True,
                is_ingested_ocr=True
            ).count()
        },
        {
            'id': 'digitized_journals',
            'label': 'journals',
            'value': len(ProcessingModel.objects(
                type_document="issue",
                is_digitized=True,
                is_ingested_ocr=True
            ).distinct('bid'))
        },
        {
            'id': 'digitized_issues',
            'label': 'journal issues',
            'value': ProcessingModel.objects(
                type_document="issue",
                is_digitized=True,
                is_ingested_ocr=True
            ).count()
        },
        {
            'id': 'digitized_articles',
            'label': 'journal articles',
            'value': ArticleModel.objects(provenance="lbcatalogue").count()
        },
        {
            'id': 'author_count',
            'label': 'authors',
            'value': DisambiguationModel.objects(
                provenance="lbcatalogue",
                type="author_of_disambiguation"
            ).count()
        },
        {
            'id': 'cited_authors',
            'label': 'cited authors',
            'value': cited_authors
        },
        {
            'id': 'cited_books',
            'label': 'cited books',
            'value': len(cited_books)
        },
        {
            'id': 'cited_articles',
            'label': 'cited articles',
            'value': len(cited_articles)
        },
        {
            'id': 'cited_ps',
            'label': 'cited primary sources',
            'value': len(DisambiguationModel.objects(
                type="reference_disambiguation",
                archival_document__ne=None
            ).distinct('archival_document'))
        },
        {
            'id': 'total_annotations',
            'label': 'annotations',
            'value': AnnotationModel.objects(
                bid__in=book_bids + journal_bids
            ).count()
        },
        {
            'id': 'monographs_annotations',
            'label': 'annotations in monographs',
            'value': AnnotationModel.objects(bid__in=book_bids).count()
        },
        {
            'id': 'journals_annotations',
            'label': 'annotations in journals',
            'value': AnnotationModel.objects(bid__in=journal_bids).count()
        },
        {
            'id': 'total_references',
            'label': 'references',
            'value': ReferenceModel.objects.count()
        },
        {
            'id': 'ps_references',
            'label': 'references to primary sources',
            'value': ReferenceModel.objects(ref_type="primary").count()
        },
        {
            'id': 'ss_references',
            'label': 'references to secondary sources',
            'value': ReferenceModel.objects(ref_type__ne="primary").count()
        }

    ]
