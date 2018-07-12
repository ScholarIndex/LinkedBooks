#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
This module contains the main functions needed for the resolution of several kinds of references.

Usage:
    resolution/resolvers.py --config=file <bid> [--log-level=<ll> --log-file=<lf> --clear-db]
    resolution/resolvers.py --config=file <bid> <issue_number> [--log-level=<ll> --log-file=<lf> --clear-db]
    resolution/resolvers.py --config=file [--limit=<n> --log-level=<ll> --log-file=<lf> --clear-db]

Options:
    --help              Shows help message.
    --log-level=<ll>    The level of verbosity of the logging [default: INFO].
    --limit=<n>         Process only the first n documents.
    --clear-db          whether the previously stored disambiguations should be deleted.

"""
__author__ = """Matteo Romanello, matteo.romanello@epfl.ch"""

import logging
global logger
logger = logging.getLogger(__name__)
import pdb
import sys
sys.path += ["../", "./"]
from docopt import docopt
import copy
from datetime import datetime
from collections import Counter
import multiprocessing as mp
from configparser import ConfigParser
from commons.dbmodels import *
from mongoengine import connection
from resolution.lookup_authors import *
from resolution.lookup_monographs import lookup_SBN
from resolution.supporting_functions import *
from resolution.fuzzy_matching import editDistance
#from metadata_importer import bibliodb_importer
from functools import reduce

PORT = 27017

# function for the reference resolutions
def retain_reference(journal_reference, min_size=3, required_fields=["author","title"]):
    """
    Determine whether the input reference should be retained, and thus resolved, or skipped.
    This allows to skip erroneously extracted references, partial ones, etc.

    :param journal_reference: the input reference (extracted from journals in LinkedBooks)
    :type journal_reference: dict
    :param min_size: the minimum number of fields `journal_reference` must have
    :type min_size: int
    :param required_fields: the fields that must be contained in `journal_reference`
    :type required_fields: list of str
    :return: bool -- True if the reference should be retained, False otherwise

    """
    fields = [field["tag"] for field in journal_reference["contents"].values()]
    if(len(fields)>=min_size):
        if(len(set(fields).intersection(set(required_fields))) >= len(required_fields)):
            return True
        else:
            return False
    else:
        return False

def classify_and_prepare(reference):
    """
    Classify the input reference into one of the following cases:
    1. ref to an article
    2. ref to a book (monograph)
    3. ref to a book part, which can be:
        a) a ref to a chapter in a book
        b) a ref to an essay within an edited volume
    by overwriting the `ref_type` field
    and change the structure of the reference accordingly

    :param reference: instance of `commons.dbmodels.Reference`
    :return: a dictionary with fields: TODO

    """

    def fields2tags(fields):
        """
        fields is a list of dictionaries, each dict having at least the following keys:
            - tag
            - surface
        returns a dictionary

        TODO: handle the problem of having authors twice (verify)

        """
        tags = {}
        for field in fields:
            if(field["tag"]=="title"):
                if("title" in tags):
                    tags["container_title"] = field["surface"]
                else:
                    tags[field["tag"]] = field["surface"]
            elif(field["tag"]=="author"):
                author_number = len([tag for tag in tags if "author" in tag])+1
                tags["%s_%s"%(field["tag"],author_number)] = field["surface"]
            else:
                if(field["tag"]=="conjunction"):
                    pass
                else:
                    tags[field["tag"]] = field["surface"]
        return tags

    reference_object = copy.deepcopy(dict(reference._data))
    sorted_keys = sorted(reference_object["contents"].keys(), key=lambda x: int(x))
    tags = fields2tags([reference_object["contents"][key] for key in sorted_keys])
    del reference_object["contents"]
    reference_object["reference"] = tags
    if(reference_object["ref_type"]=="meta-annotation"):
        if("publicationnumber-year" in reference_object["reference"]):
            if("container_title" in reference_object["reference"]):
                reference_object["reference"]["journal_title"] = reference_object["reference"].pop("container_title")
            else:
                reference_object["reference"]["journal_title"] = ""
            reference_object["ref_type"] = "journal_article"
        else:
            if("container_title" in reference_object["reference"]):
                reference_object["reference"]["book_title"] = reference_object["reference"].pop("container_title")
            else:
                reference_object["reference"]["book_title"] = None
            reference_object["ref_type"] = "book_part"
    else:
        reference_object["ref_type"] = "book"
    return reference_object

def resolve_article_reference(reference, record_provenance="processing"): #TODO: implement
    """
    `reference` is a dictionary with the following structure: TODO

    Deal with the following cases:
    - the article (and journal) are already in `bibliodb_articles`
        - in this case return the internal_id of the article
    - the article is **not** in `bibliodb_articles` but the journal is in `bibliodb_articles`
        - create a new record for the article and link it to the journal
        - try to link article and author
        - return the internal_id of the newly created article record
    - neither the article nor the journal are in MongoDB
        - create the necessary records in the DB and return the internal_id

    """
    pass

def resolve_book_reference(reference, record_provenance="processing"):
    """
    TODO

    .. todo::
    - make this work also with references extracted from monographs
    - filter the candidates returned by `lookup_SBN`: check field `livello` (exclude `livello==spoglio`)
    - returns a list of disambiguations

    """

    from metadata_importer import bibliodb_importer

    logger.info("Processing the following reference: \"%s\""%(reference["reference_string"]))
    # find out how many `author` fields there are in the reference
    author_number = len([field for field in reference["reference"].keys() if "author" in field])

    if(author_number > 0):
        reference["reference"]["author"] = reference["reference"].pop("author_1")

    candidates = lookup_SBN(reference["reference"], n_words=3, similarity_threshold=0.4, ref_type="journals")
    logger.info("%i candidates were found for \"%s\" (%s) " % (len(candidates)
                                                            , reference["reference_string"]
                                                            , reference["id"]))
    disambiguations = []

    if(len(candidates)==0):
        # there are no candidates to return
        return disambiguations
    else:
        max_score = max([similarity_score for similarity_score, candidate, score_explanation in candidates])
        single_best_bid = [candidate["bid"] for similarity_score, candidate, score_explanation in candidates
                                                                                if similarity_score == max_score][0]

        bid_in_mongo = True if Book.objects(bid=single_best_bid).first() is not None else False
        logger.info("Best match is %s (score = %s); in Mongo=%s"%(single_best_bid, max_score, bid_in_mongo))

        # first the book
        if bid_in_mongo is False:
            sbn_record = enrich_metadata(single_best_bid)
            book_object = bibliodb_importer.sbn2book(sbn_record, record_provenance=record_provenance)
            book_object.save()

        else:
            book_object = Book.objects(bid=single_best_bid).first()

        #pdb.set_trace()
        disambiguation = Disambiguation(type="reference_disambiguation"
                                        , provenance=record_provenance
                                        , book=book_object.id
                                        , reference=reference["id"]
                                        , document_id=Reference.objects(id=reference["id"]).get().document_id
                                        )
        disambiguation.save()
        disambiguations.append(disambiguation)


        # then create the author/editor disambiguation
        if "names" in book_object:
            for name in book_object.names:
                try:
                    author_object = resolve_author(name, record_provenance)

                    disambiguation_type = ""

                    if "cura di" in book_object.author or "edited by" in book_object.author:
                        disambiguation_type = "editor_of_disambiguation"
                    else:
                        disambiguation_type = "author_of_disambiguation"

                    disambiguation = Disambiguation(
                                                    type=disambiguation_type
                                                    , provenance=record_provenance
                                                    , book=book_object
                                                    , author=author_object
                                                    )
                    disambiguation.save()
                    disambiguations.append(disambiguation)
                    logger.info("(Disambiguation of %s) %s was disambiguated with %s [%s]" %(reference["id"], name, repr(author_object), disambiguation_type))
                    logger.info("Saved disambiguation %s (%s)" % (disambiguation.id, disambiguation.type))

                except Exception as e:
                    logger.error("The author lookup in reference %s failed with error \'%s\'" % (reference["id"], e))

        return disambiguations

def resolve_bookpart_reference(reference): #TODO: implement
    """
    - the reference has 1 title and 1 book_title (but this can also be None)
    - make a copy of the reference and pass to lookup_SBN only the fields that are (believed
    to be) part of the container
    """
    pass

def resolve_author(author_string, record_provenance="processing"):
    """
    TODO

    .. todo::

    Test on one author that is not in VIAF (e.g. Murru, Piefranco).

    """

    author_lookup_result = lookup_author_mongo(author_string)
    logger.info("Mongo lookup of %s: %s" % (author_string, repr(author_lookup_result)))

    if author_lookup_result is None:
        viaf_lookup_result = viaf_lookup(author_string)

        if viaf_lookup_result!=("",""):
            author_final_form, viaf_id = viaf_lookup_result
            logger.info("%s: Found a match in VIAF, id=%s" % (author_string, viaf_id))
            author_lookup_result = Author(author_final_form=author_final_form
                                          , provenance=record_provenance
                                          , surface_forms=[author_string]
                                          , viaf_id=viaf_id)
            try:
                author_lookup_result.save()
                logger.info("Added new author to MongoDB: %s" % repr(author_lookup_result))
            except NotUniqueError as e:
                logger.warning("%s not saved as it's a duplicate. Error message: %s" % (repr(author_lookup_result), e))
                author_lookup_result = Author.objects(author_final_form=author_final_form, viaf_id=viaf_id).first()
        else:
            author_lookup_result = Author(author_final_form=author_string
                                          , provenance=record_provenance
                                          , surface_forms=[author_string])
            logger.info("No viaf match; created new author: %s" % repr(author_lookup_result))
            try:
                author_lookup_result.save()
                logger.info("Added new author to MongoDB: %s" % repr(author_lookup_result))
            except NotUniqueError as e:
                logger.warning("%s not saved as it's a duplicate. Error message: %s" % (repr(author_lookup_result), e))
                author_lookup_result = Author.objects(author_final_form=author_string).first()
    else:
        logger.info("A match for \"%s\" was found in MongoDB (id=%s)." % (author_string, author_lookup_result.id))
    return author_lookup_result

def resolve_references_by_document(bid, issue_number=None):
    """
    :param bid:
    :param issue_number:
    :return: ?

    """

    results = []

    if issue_number is not None:
        document = LBDocument.objects(bid=bid, issue_number=issue_number).get()
    else:
        document = LBDocument.objects(bid=bid).get()

    assert document is not None
    logger.debug(repr(document))

    extracted_references = Reference.objects(document_id=document.id)
    logger.info("Document %s contains %i references" % (document.id, extracted_references.count()))

    if extracted_references.count() > 0:
        # get references
        extracted_secondary_references = [reference
                                          for reference in extracted_references
                                          if reference["ref_type"]!="primary"]
        # filter references
        filtered_extracted_secondary_references = [reference
                                                   for reference in extracted_secondary_references
                                                   if retain_reference(reference, 4, required_fields=["title"]) == True]

        # classify references
        logger.info("%i secondary references (after filtering) will be resolved"%len(filtered_extracted_secondary_references))
        references = [classify_and_prepare(reference) for reference in filtered_extracted_secondary_references]
        types = dict(Counter([reference["ref_type"] for reference in references]))
        logger.info("Reference count by type: %s"%types)

        book_references = [reference for reference in references if reference["ref_type"]=="book"]
        book_part_references =  [reference for reference in references if reference["ref_type"]=="book_part"]
        article_references = [reference for reference in references if reference["ref_type"]=="journal_article"]

        pool = mp.Pool()
        ((lambda x,y: x+y), pool.map(resolve_book_reference, book_references))
        pool.close()
        pool.join()

        # TODO: finish implelementing the resolution for the other entity types

        return results
    else:
        return [] #TODO => by document

def main(arguments):
    """

    If called as a CLI, processes the input parameters and acts accordingly.

    """
    # read parameters from the configuation file
    config_file = arguments["--config"]
    assert config_file is not None
    config_parser = ConfigParser(allow_no_value=True)
    config_parser.read(config_file)
    #global mongo_db, mongo_user, mongo_pwd, mongo_auth, mongo_host, mongo_port
    mongo_host = config_parser.get('mongo','db-host')
    mongo_db = config_parser.get('mongo','db-name')
    mongo_port = config_parser.getint('mongo','db-port')
    mongo_user = config_parser.get('mongo','username')
    mongo_pwd = config_parser.get('mongo','password')
    mongo_auth = config_parser.get('mongo','auth-db')
    logger.info('Read configuration file %s'%config_file)

    # TODO: to avoid issues with multiprocessing and forking
    #        try to connect to MongoDB from within the
    #        `resolve_references_by_document` function

    db = connect(mongo_db
            , username=mongo_user
            , password=mongo_pwd
            , authentication_source=mongo_auth
            , host=mongo_host
            , port=mongo_port
            , connect=False
            )

    log_level = arguments["--log-level"]
    log_file = arguments["--log-file"]
    logger.setLevel(log_level)
    handler = logging.FileHandler(filename=log_file, mode='w') if log_file is not None else logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.debug("Logger initialised")

    #logger.debug("Established mongoengine connection with %s" % db)

    if arguments["--clear-db"]:
        logger.info("%i documents where references were disambiguated" % Processing.objects(is_disambiguated_s=True).count())
        for processing_record in Processing.objects(is_disambiguated_s=True):
            Processing.objects(id=processing_record.id).update_one(set__is_disambiguated_s=False)
        logger.info("%i documents where references were disambiguated" % Processing.objects(is_disambiguated_s=True).count())

        logger.info("Clearing the BiblioDBs from record with `provenance=processing`")
        logger.info("Deleted %i authors" % len([a.delete() for a in Author.objects(provenance="processing")]))
        logger.info("Deleted %i journals" % len([j.delete() for j in Journal.objects(provenance="processing")]))
        logger.info("Deleted %i articles" % len([a.delete() for a in Article.objects(provenance="processing")]))
        logger.info("Deleted %i books" % len([b.delete() for b in Book.objects(provenance="processing")]))
        logger.info("Deleted %i disambiguations" % len([d.delete() for d in Disambiguation.objects(provenance="processing", archival_document=None)]))
        logger.info("...done!")

    bid = arguments["<bid>"]
    issue_number = arguments["<issue_number>"]

    bids_not_disambiguated = [(document.bid, document.number) for document in Processing.objects(is_parsed=True, is_disambiguated_s=False)]
    logger.info("References in %i documents have not yet been disambiguated" % len(bids_not_disambiguated))

    if bid is not None or (bid is not None and issue_number is not None):
        if(bid, issue_number) in bids_not_disambiguated:
            bids_not_disambiguated = [(_bid, _issue_number)
                                        for _bid, _issue_number in bids_not_disambiguated
                                        if _bid == bid and _issue_number == issue_number]

            logger.info("You selected bid=%s and issue_number=%s; %i documents will be disambiguated" % (bid
                                                                                                        , issue_number
                                                                                                        , len(bids_not_disambiguated)))
    else:
        limit = arguments["--limit"]
        if limit is not None:
            limit = int(limit)
            bids_not_disambiguated = bids_not_disambiguated[:limit]
            logger.info("Limiting the disambiguation to %i documents" % limit)

    for bid, issue_number in bids_not_disambiguated:
        # process references in document
        if issue_number == "":
            resolve_references_by_document(bid)
        else:
            resolve_references_by_document(bid, issue_number)

        # update document record in processing
        if issue_number == "":
            record = Processing.objects(bid=bid).get()
        else:
            record = Processing.objects(bid=bid, number=issue_number).get()

        record.updated_at = datetime.utcnow()
        record.is_disambiguated_s = True
        record.save()

if __name__ == '__main__':
    arguments = docopt(__doc__)
    main(arguments)
