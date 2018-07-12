# -*- coding: utf-8 -*-
"""
Imports the metadata ingested by `metadata_importer.py` into LinkedBooks' bibliographic database (BibilioDB).

Example usage:
    python metadata_importer/bibliodb_importer.py --config-file=annotation_connector/config_files/LB_machine_sandbox.conf \
            --articles-file=tests/test_data/articles.json --authors-file=tests/test_data/authors.json --clear-db

"""
__author__ = """Matteo Romanello"""

import json
import pdb
import math
import sys
from tqdm import tqdm
import pandas as pd
import argparse
sys.path += ["../", "./"]
import logging
from datetime import datetime
from configparser import ConfigParser
from commons.dbmodels import *
from content_ingester.content_ingestion import convert_issue_number
from mongoengine import connect as engineconnect
from resolution.fuzzy_matching import *
from resolution.supporting_functions import *
from resolution.lookup_authors import *
from resolution import resolvers

logger = logging.getLogger(__name__)

def add_journal_continuation_information(journal_object):
    """
    - this needs to be done once everything is ingested into bibliodb
    - check in `metadata.relations`, where `relation.type=="CONTINUAZIONE DI"`
        - and then take `relation.title` and use it to get the corresponding BID
        -
    """
    metadata_record = Metadata.objects(bid=journal_object.bid).first()

    for relation in metadata_record.relations:
        if relation.type == "CONTINUAZIONE DI":
            continued_journal_title =  cleanup_string(relation.title.surface).replace("  "," ")
            continued_journal_objects  = [journal
                                        for journal in Journal.objects
                                        if cleanup_string(journal.full_title).replace("  "," ") == continued_journal_title]
            if len(continued_journal_objects) > 0:
                continued_journal_object = continued_journal_objects[0]
                print("%s is continuation of %s" % (repr(journal_object), repr(continued_journal_object)))
                journal_object.previous_series = continued_journal_object
                continued_journal_object.following_series = journal_object
                journal_object.save()
                continued_journal_object.save()
    return journal_object

def sbn2journal(sbn_record, permalink_template="http://id.sbn.it/bid/%s"):
    """
    Creates a `dbmodels.Journal` instance out of a dictionary with metadata.

    :param record: the dictionary returned by `resolution.supporting_functions.enrich_metadata()`
    :return: an instance of `dbmodels.Journal`
    """
    bid = normalize_bid(sbn_record["codiceIdentificativo"])
    metadata = {
        'short_title' : sbn_record["titolo"].split(":")[0].split("/")[0].strip()
        , 'full_title' : sbn_record["titolo"]
        , 'bid' : bid
        , 'sbn_link' : permalink_template % bid
        , 'identifiers' : []
        , "provenance" : "lbcatalogue"
    }
    if "numeri" in sbn_record:
        identifiers = sbn_record["numeri"]
        for identifier in identifiers:
            tmp = [{
                    "identifier_type" : key
                    ,"value": identifier[key]
                } for key in identifier.keys()][0]
            metadata["identifiers"].append(SBN_Identifier(**tmp))
    return Journal(**metadata)

def sbn2book(record, permalink_template="http://id.sbn.it/bid/%s", record_provenance="lbcatalogue"):
    """
    Creates a `dbmodels.Book` instance out of a dictionary with metadata.

    :param record: the dictionary returned by `resolution.supporting_functions.enrich_metadata()`
    :return: an instance of `dbmodels.Book`
    """
    #record = prepare_sbn_record(sbn_record)
    logger.debug(record)
    metadata = {
        'title' : record["title"]
        , 'title_orig' : record["title_orig"]
        , 'author' : record["author"]
        , 'bid' : record["bid"]
        , 'names' : record["names"] if "names" in record else None
        , 'publisher' : record["publisher"] if "publisher" in record else ""
        , 'publication_year' : str(record["year"]) if "year" in record else ""
        , 'publication_place' : record["place"] if "place" in record else ""
        , 'publication_country' : record["publication_country"] if "publication_country" in record else ""
        , 'publication_language' : record["publication_language"] if "publication_language" in record else ""
        , 'digitization_provenance' : record["digitization_provenance"] if "digitization_provenance" in record["digitization_provenance"] else ""
        , 'document_id' : record["document_id"]
        , 'sbn_link' : permalink_template % record["bid"]
        , 'identifiers' : []
        , "provenance" : record_provenance
    }
    if "identifiers" in record:
        for identifier in record["identifiers"]:
            tmp = [{
                    "identifier_type" : key
                    ,"value": identifier[key]
                } for key in identifier.keys()][0]
            metadata["identifiers"].append(SBN_Identifier(**tmp))
    return Book(**metadata)

def import_book(bid):
    """
    Imports a book into the BibilioDB.

    :param bid: the BID of the book to ingest
    :return: a Book object (`commons.dbmodels.Book`)
    """
    # TODO: disambiguate the authors/editors
    search_results = find_BID_in_SBN(bid)
    disambiguations = []

    if is_BID_in_SBN(bid):

        # first ingest the book
        sbn_record = enrich_metadata(bid)
        book_object = sbn2book(sbn_record)

        # TODO: add information from `metadata` (date and digitization_provenance)
        book_object.save()
        logger.info("Ingested book %s into bibliodb (id=%s)" % (bid, book_object.id))

        # then create the author/editor disambiguation
        if "names" in book_object:
            for name in book_object.names:
                author_object = resolvers.resolve_author(name, "lbcatalogue")
                disambiguation_type = ""

                if "cura di" in book_object.author or "edited by" in book_object.author:
                    disambiguation_type = "editor_of_disambiguation"
                else:
                    disambiguation_type = "author_of_disambiguation"

                disambiguation = Disambiguation(
                                                type=disambiguation_type
                                                , provenance="lbcatalogue"
                                                , book=book_object
                                                , author=author_object
                                                )
                disambiguation.save()
                logger.info("(import of %s) %s was disambiguated with %s [%s]" %(bid, name, repr(author_object), disambiguation_type))
                logger.info("Saved disambiguation %s (%s)" % (disambiguation.id, disambiguation.type))
        else:
            pass
        return book_object
    else:
        logger.warning("Book %s was not ingested: no record found in the SBN dump" % bid)
        return None

def import_journal(bid):
    """
    Imports a journal into the BibilioDB.

    :param bid: the BID of the journal to ingest
    :return: a Journal object (`commons.dbmodels.Journal`)

    """
    sbn_record = find_BID_in_SBN(bid)[0]
    journal_object = sbn2journal(sbn_record)
    journal_object.document_id = LBDocument.objects(bid=bid).first()
    journal_object.save()
    logger.info("Ingested journal %s into bibliodb (id=%s)" % (bid, journal_object.id))
    return journal_object

def import_articles(journal, articles_metadata, year, volume, issue):
    """

    :param journal:
    :param articles_metadata:
    :param bid:
    :param volume:
    :return: a list of articles (type = `commons.dbmodels.Article`)

    LOGIC
    it produces:
    - author_disamguation (article -> disambig. -> author [prov = catalog])
    - in_journal_disambiguation (article -> disambig. -> journal [prov = catalog])
    - returns list of commons.dbmodels.Article

    """
    if issue is None:
        issue_number = "%s_%s" % (year, volume)
    else:
        issue_number = "%s_%s_%s" % (year, volume, issue)
    articles = []
    for i, article in enumerate(articles_metadata):
        internal_id = "%s:%s:%i"%(journal.bid, issue_number, i+1)
        try:
            assert article["end_page"]!="" and article["start_page"]!=""
        except Exception as e:
            logger.error("Check the start/end page information for article \"%s\" (%s)" % (article["title"], internal_id))
            continue

        # TODO: find an issue object (see code in content ingestion) and copy `provenance`
        issue_object = find_issue_in_metadata(journal.bid, issue_number)
        article_object = Article(title=article["title"]
                                , journal_short_title=journal.short_title
                                , journal_bid=journal.bid
                                , document_id=find_issue_in_documents(journal.bid, issue_number)
                                , internal_id=internal_id
                                , authors=clean_authors(article["author"]).split(" - ")
                                , start_img_number=int(article["start_page"])
                                , end_img_number=int(article["end_page"])
                                , provenance="lbcatalogue"
                                , digitization_provenance=issue_object.provenance if issue_object is not None else None
                                , year=year
                                , volume=volume)
        if issue is not None:
            article_object.issue_number = issue
        article_object.save()
        articles.append(article_object)
        #
        # try to match each author name first against Mongo and then against VIAF
        # if still not found, add it to the DB
        #
        for author in article_object.authors:
            if author!="":
                author_lookup_result = resolvers.resolve_author(author, "lbcatalogue")
                disambiguation = Disambiguation(
                                                type="author_of_disambiguation"
                                                , provenance="lbcatalogue"
                                                , article=article_object
                                                , author=author_lookup_result
                                                , surface=author
                                                )
                disambiguation.save()
            else:
                logger.debug("Skipping author name as it's empty")
    return articles

def find_issue_in_metadata(bid, issue_number):
    """
    TODO
    """
    metadata_record = Metadata.objects(bid=bid).get()
    issue_record = None
    #
    # fetch the metadata record for the issue to be ingested
    # fail and exit if it cannot be found
    #
    try:
        issue_record = next(issue_obj
                                for issue_obj in metadata_record['issues']
                                    if issue_obj['foldername']==issue_number and issue_obj["marked_as_removed"] is False)
        assert issue_record is not None
        return issue_record
    except Exception as e:
        logger.warning(e)
        logger.warning("The record %s doesn't contain the issue \"%s\""%(bid, issue_number))
        try:
            new_issue_number = convert_issue_number(issue_number)
            logger.info("Trying now with issue number = %s"%new_issue_number)
            issue_record = next(issue_obj
                                for issue_obj in metadata_record['issues']
                                    if issue_obj['foldername']==new_issue_number and issue_obj["marked_as_removed"] is False)
            assert issue_record is not None
            logger.info("Record found!")
            return issue_record
        except Exception as e:
            try:
                new_issue_number = issue_number.replace('-','_')
                logger.info("Trying now with issue number = %s"%new_issue_number)
                issue_record = next(issue_obj
                                    for issue_obj in metadata_record['issues']
                                        if issue_obj['foldername']==new_issue_number and issue_obj["marked_as_removed"] is False)
                assert issue_record is not None
                logger.info("Record found!")
                return issue_record
            except Exception as e:
                logger.error("Couldn't find a metadata record for issue \"%s-%s\""%(bid, issue_number))
                return None

def find_issue_in_documents(bid, issue_number): #TODO: finish to document
    """
    TODO

    strategies:
        - issue_number as given
        - new_issue_number = convert_issue_number(issue_number)
        - new_issue_number = issue_number.replace('-','_')
    """

    try:
        document = LBDocument.objects(bid=bid, issue_number=issue_number).first()
        assert document is not None
        return document
    except AssertionError as e:
        try:
            new_issue_number = convert_issue_number(issue_number)
            document = LBDocument.objects(bid=bid, issue_number=new_issue_number).first()
            assert document is not None
            return document
        except AssertionError as e:
            try:
                new_issue_number = issue_number.replace('-','_')
                document = LBDocument.objects(bid=bid, issue_number=new_issue_number).first()
                assert document is not None
                return document
            except AssertionError as e:
                return None

def _convert_issue_number(number):
    if "." in number:
        year, rest = number.split("_")
        volume, issue = rest.split(".")
        return "%s_%s_%s" % (year, volume, issue)
    else:
        return number

def _split_issue_number(number):
    logger.info("Splitting issue number: %s" % number)
    if "." in number:
        year, rest = number.split("_")
        volume, issue = rest.split(".")
        return year, volume, issue
    else:
        try:
            year, volume = number.split("_")
            return year, volume, None
        except ValueError as e:
            year = number.split("_")[0]
            volume = "%s-%s" % (number.split("_")[1], number.split("_")[2])
            return year, volume, None

def _fetch_article_metadata(df, bid, issue_number):
        """
        TODO

        Example of output:

        [{'author': '',
          'end_page': '5',
          'start_page': '4',
          'title': 'Sulle vicende d’Italia'},
        ...]
        """
        try:
            logger.debug("%s-%s" % (bid, issue_number))
            articles = dict(df[(df["issue"]==issue_number) & (df["bid"]==bid.strip())]['articles'])
            articles = [article for article in articles.values()][0]
            return articles
        except Exception as e:
            issue_number = _convert_issue_number(issue_number)
            logger.debug("%s-%s" % (bid, issue_number))
            articles = dict(df[(df["issue"]==issue_number) & (df["bid"]==bid.strip())]['articles'])
            articles = [article for article in articles.values()][0]
            return articles

def import_authors(authors_file_name):
    """
    Imports a list of authors into the BiblioDB.
    """
    legacy_authors = None
    duplicates_found = 0

    with open(authors_file_name, "r") as input_file:
        legacy_authors = json.load(input_file)
    for legacy_author in legacy_authors:
        new_author = Author(**legacy_author)
        new_author.provenance = "lbcatalogue"
        try:
            new_author.save()
            logger.info("Inserted %s"%repr(new_author))
        except NotUniqueError as e:
            duplicates_found += 1
            logger.warning("%s not saved as it's a duplicate. Error message: %s" % (repr(new_author), e))

def import_metadata(articles_file_name, limit=None):
    """
    Import metadata of ingested documents into BiblioDB.

    :param articles_file_name: the path of the JSON file containing article-level metadata (by journal issue).

    """

    documents_not_imported = Processing.objects(is_ingested_metadata=True
                                                , is_ocr=True
                                                , is_ingested_ocr=True
                                                , is_bibliodbed=False)
    logger.info("There are %i documents to import into BiblioDB" % documents_not_imported.count())
    articles_df = pd.read_json(articles_file_name, encoding='utf-8') if articles_file_name is not None else None

    #pdb.set_trace()

    for document in list(documents_not_imported)[:limit]:

        # the document is a journal issue
        if document.type_document == "issue" and articles_df is not None:
            # first import the journal
            journal_bid = document.bid
            journal_object = Journal.objects(bid=journal_bid).first()
            if journal_object is None:
                journal_object = import_journal(journal_bid)
            else:
                logger.info("Bibliodb already contains a record for journal %s" % journal_bid)
            try:
                articles_metadata = _fetch_article_metadata(articles_df, document.bid, document.number)
                logger.info("Found %i articles for %s %s" % (len(articles_metadata), document.bid, document.number))
                year, volume, issue = _split_issue_number(document.number)

                # import articles and author disambiguation into the DB

                article_objects = import_articles(journal_object, articles_metadata, year, volume, issue)
                for article_object in article_objects:
                    in_journal_disambiguation = Disambiguation(type="in_journal_disambiguation"
                                                                , article=article_object
                                                                , journal=journal_object
                                                                , checked=True
                                                                , correct=True
                                                                , provenance="lbcatalogue"
                                                                )
                    in_journal_disambiguation.save()

            except IndexError as e:
                logger.warning("Found no articles for %s %s" % (document.bid, document.number))
            except Exception as e:
                raise e
            finally:
                # save the corresponding record in processing
                Processing.objects(id=document.id).update_one(set__is_bibliodbed=True)

        # the document is a book
        elif document.type_document == "monograph":

            #pdb.set_trace()

            book_bid = document.bid
            book_object = Book.objects(bid=book_bid).first()

            if book_object is None:
                logger.info("No record for %s in BiblioDB; let's ingest it." % book_bid)
                imported_record = import_book(book_bid)

                if imported_record is not None:
                    Processing.objects(id=document.id).update_one(set__is_bibliodbed=True)
            else:
                logger.info("Bibliodb already contains a record for book %s coming from %s" % (book_bid, book_object.provenance))
                Processing.objects(id=document.id).update_one(set__is_bibliodbed=True)
                logger.info("Updated `processing.is_bibliodbed` for book %s" % book_bid)

    # add previous/next series information for all journals contained in biblio_db
    if articles_df is not None:
        [add_journal_continuation_information(j) for j in Journal.objects]

def import_asve(asve_file_name):
    """
    """
    def _normalize_id(id):
        return ".".join([component for component in id.split(".") if component!=""])

    df = pd.read_csv(asve_file_name, encoding="utf-8")[['n', 'document_type', 'id', 'title', 'url', 'notes']]
    df.columns = ['n', 'document_type', 'internal_id', 'title', 'url', 'notes']
    df["internal_id"] = df["internal_id"].apply(_normalize_id)

    for i, row in tqdm(df.iterrows(), desc="Ingesting records:"):
        row = dict(row)
        del row["n"]
        if math.isnan(row["notes"]):
            row["notes"] = ""
        try:
            row["html"] = requests.get(row["url"]).text
        except Exception as e:
            logger.error("Could't fetch the web page for %s" % row)
            row["html"] = ""
        record = ArchivalRecordASVE(**row)
        record.validate()
        record.save()

if __name__ == "__main__":
    desc="""
    TODO
    """

    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("--limit", help="The limit of document folders to process",type=int)
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument("--config-file", help="The configuration file (with parameters for connecting to MongoDB)",type=str,required=True)
    requiredNamed.add_argument("--articles-file", help="TODO", type=str, required=False, default=None)
    requiredNamed.add_argument("--authors-file", help="TODO", type=str, required=False, default=None)
    requiredNamed.add_argument("--asve-file", help="TODO", type=str, required=False, default=None)
    requiredNamed.add_argument("--log-file", help="Destination file for the logging",type=str,required=False)
    requiredNamed.add_argument("--log-level", help="Log level",type=str,required=False,default="INFO")
    requiredNamed.add_argument("--clear-db"
                                , help="Remove all existing_annotations before ingesting new ones"
                                , action="store_true"
                                , required=False
                                , default=False)
    args = parser.parse_args()
    #
    # Initialise the logger
    #
    logger = logging.getLogger()
    numeric_level = getattr(logging, args.log_level.upper(), None)
    logger.setLevel(numeric_level)
    if(args.log_file is not None):
        handler = logging.FileHandler(filename=args.log_file, mode='w')
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info("Logger successfully initialised")

    config = ConfigParser(allow_no_value=False)
    config.read(args.config_file)
    logger.info('Read configuration file %s' % args.config_file)
    mongo_db = config.get('mongo', 'db-name')
    mongo_user = config.get('mongo', 'username')
    mongo_pwd = config.get('mongo', 'password')
    mongo_auth = config.get('mongo', 'auth-db')
    mongo_host = config.get('mongo', 'db-host')
    mongo_port = config.get('mongo', 'db-port')
    logger.debug(engineconnect(mongo_db
                         , username=mongo_user
                         , password=mongo_pwd
                         , authentication_source=mongo_auth
                         , host=mongo_host
                         , port=int(mongo_port)))
    #
    # delete existing documents from collections
    #
    if args.clear_db:
        logger.info("%i records ingested into BiblioDB" % Processing.objects(is_bibliodbed=True).count())
        for processing_record in Processing.objects(is_bibliodbed=True):
            Processing.objects(id=processing_record.id).update_one(set__is_bibliodbed=False)
        logger.info("%i records ingested into BiblioDB" % Processing.objects(is_bibliodbed=True).count())

        logger.info("Clearing the BiblioDBs...")
        logger.info("Deleted %i authors" % len([a.delete() for a in Author.objects(provenance="lbcatalogue")]))
        logger.info("Deleted %i journals" % len([j.delete() for j in Journal.objects(provenance="lbcatalogue")]))
        logger.info("Deleted %i articles" % len([a.delete() for a in Article.objects(provenance="lbcatalogue")]))
        logger.info("Deleted %i books" % len([b.delete() for b in Book.objects(provenance="lbcatalogue")]))
        logger.info("Deleted %i disambiguations" % len([d.delete() for d in Disambiguation.objects(provenance="lbcatalogue")]))
        #logger.info("Deleted %i AsVe records" % len([r.delete() for r in ArchivalRecordASVE.objects]))
        logger.info("...done!")

    if args.authors_file is not None:
        import_authors(args.authors_file)

    if args.asve_file is not None:
        import_asve(args.asve_file)

    import_metadata(args.articles_file, args.limit)
