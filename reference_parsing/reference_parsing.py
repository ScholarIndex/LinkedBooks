# -*- coding: utf-8 -*-
"""
This code defines the loader, parser and ingester for references.
"""
__author__ = """Giovanni Colavizza"""

import sys
sys.path += ["../","./","../../"]
import logging
from collections import OrderedDict
from configparser import ConfigParser
from datetime import datetime
from pymongo import MongoClient
from mongoengine import connect as engineconnect
from commons.dbmodels import *

how_many = 10 # bids at a time

def loader(db, bids=None, use_journals=True, use_monographs=True):
    # TODO: add filter by issue
    # TODO: decide on force parsing on already parsed stuff: problem of disambiguations
    """
    Loads data for parsing. The current version does NOT consider anything that is marked as is_parsed in Processing.
    Here only non annotated pages are considered.

    :param db: Connection to database from which to load data
    :param bids: List of bids to process, should you want to filter by bid
    :param use_journals: If to use journals
    :param use_monographs: If to use monographs
    :return: A list of documents exported, with all their pages and contents, ready for parsing
    """

    logging.basicConfig(filename="reference_parsing/logs/loader.log", level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Initialize counters and data structures
    data = list() # a list of documents with all data for every page (both annotated and not)

    # check boundaries of loading
    if not bids or len(bids) == 0:
        if use_journals and use_monographs:
            bids = list(set([x["bid"] for x in db.documents.find()]))
        elif use_journals:
            bids = list(set([x["bid"] for x in db.documents.find() if x["type"] == "journal_issue"]))
        elif use_monographs:
            bids = list(set([x["bid"] for x in db.documents.find() if x["type"] == "monograph"]))
    logger.info('Bids list established.')
    logger.info('Number of bids: %d'%len(bids))

    for bid in bids[:how_many]:
        # Build query for identifiers
        query = {"bid": bid}

        for doc in db.documents.find(query,no_cursor_timeout=True):
            #TODO: note that this check should be done in metadata.
            if "marked_as_removed" in doc and doc["marked_as_removed"]:
                logger.info(str(doc["_id"]) + " Marked as removed, SKIPPING")
                continue
            doc_type = doc["type"]
            doc_number = ""
            if doc_type == "journal_issue" and "number" in doc.keys():
                doc_number = doc["number"]
            doc_id = doc["_id"]
            proc_doc = db.processing.find_one({"bid":bid,"number":doc_number})
            if not proc_doc or proc_doc["is_parsed"] or not proc_doc["is_ingested_ocr"]:
                logger.warning(str(doc["_id"]) + " Missing, no text yet or already parsed, SKIPPING")
                continue
            logger.info(str(doc["_id"])+" OK")
            pages = OrderedDict()

            for page in db.pages.find({"_id": {"$in": doc["pages"]}}):
                page_number = int(page["single_page_file_number"])
                pages[page_number] = {"offsets":list(),"page_id":"","page_mongo_id":"","single_page_file_number":page_number}
                page_id = bid+"-"+doc_number+"-page-"+page["filename"].split("-")[-1].split(".")[0]
                page_mongo_id = page["_id"]
                container = list()
                if page["is_annotated"]:
                    logger.warning(page_id + " Page annotated!! CHECK, SKIPPING")
                    continue
                # load text
                # create a reverse index
                offsets = dict()
                for line in page["lines"]:
                    line_number = line["line_number"]
                    if line_number is None:
                        continue
                    for token in line["tokens"]:
                        italics = False
                        bold = False
                        size = ""
                        if "features" in token.keys():
                            for feature in token["features"]:
                                feat = feature["feature"]
                                value = feature["value"]
                                if "font-weight" == feat:
                                    if value == "bold":
                                        bold = True
                                if "font-style" == feat:
                                    if value == "italics":
                                        italics = True
                                if "font-size" == feat:
                                    if "small" in value:
                                        size = "small"
                                    elif "medium" in value:
                                        size = "medium"
                                    elif "large" in value:
                                        size = "large"
                        offsets.update({token["offset_start"]: {"surface": token["surface"],
                                                                "position": token["token_number"],
                                                                "end": token["offset_end"], "line": line_number,
                                                                "italics": italics, "bold": bold, "size": size,
                                                                "bid": bid,
                                                                "general_category": "", "specific_category": "",
                                                                "beginend": "o", "taggedbe": "o"}})
                    offsets = OrderedDict(sorted(offsets.items(), key=lambda key_value: key_value[0]))

                # store meta
                for start,token in offsets.items():
                    new_token = ((token["surface"],start,token["end"],token["position"],token["line"],token["bid"]),(token["italics"],token["bold"],token["size"]))
                    container.append(new_token)
                # store annotated page in pertinent article
                pages[page_number]["offsets"] = container
                pages[page_number]["page_id"] = page_id
                pages[page_number]["page_mongo_id"] = page_mongo_id
            doc_data = {"doc_mongo_id":doc_id,"doc_type":doc_type,"pages":pages,"bid":bid,"doc_number":doc_number}
            data.append(doc_data)

    return data

###
# PARSER
###
from sklearn.externals import joblib
#from pathos.multiprocessing import ProcessPool as Pool
from multiprocessing import Pool
from .feature_extraction_words import word2features

# define supporting functions for features, m1 and m2
def text2featuresM1(text,window):
    return [word2features(text, i, window=window) for i in range(len(text))]
def text2featuresM2(text, window, extra_labels):
    return [word2features(text, i, extra_labels=extra_labels, window=window) for i in range(len(text))]

def process_document(doc, model_1="reference_parsing/model_dev/models/modelM1_ALL_L.pkl", model_2="reference_parsing/model_dev/models/modelM2_ALL_L.pkl", window = 2):

    # load models
    crf1 = joblib.load(model_1)
    crf2 = joblib.load(model_2)

    for page in doc["pages"].values():
        data_to_tag = [text2featuresM1(page["offsets"], window)]
        page_lab_m1 = crf1.predict(data_to_tag)
        assert len(page_lab_m1[0]) == len(page["offsets"])
        data_to_tag = [text2featuresM2(page["offsets"], window, page_lab_m1[0])]
        page_lab_m2 = crf2.predict(data_to_tag)
        assert len(page_lab_m2[0]) == len(page["offsets"])
        page.update({"specific_tags": page_lab_m1[0]})
        page.update({"BET_tags": page_lab_m2[0]})
    return doc

def parser(data, threads=7):
    """
    Takes loaded data and parses it for specific and generic tags. Returns the same data, with the tags.
    Implementation is multithreaded for speed.
    :param db: Connection to database from which to load data
    :param data: Dataset, out of loader
    :param model_1: path to model 1, for specific tags
    :param model_2: path to model 2, for generic tags
    :param threads: number of threads to use
    :return: A list of documents exported, with all their pages and contents, plus parsed. Ready for ingestion
    """

    # parse all
    processes = Pool(threads)
    data_parsed = [d for d in processes.imap_unordered(process_document, data)]

    return data_parsed

###
# INGESTER
###
from .support_functions import json_outputter

def ingester(db, data, threads=7):
    """
    Takes parsed data from parser and ingests it into the database
    :param db: Connection to database from which to load data
    :param data: Dataset, out of parser
    :param threads: number of threads to use
    :return: Nothing
    """

    logging.basicConfig(filename="reference_parsing/logs/ingester.log", level=logging.INFO)
    logger = logging.getLogger(__name__)

    # first, we go through the parsers which consolidate references
    _, refs, _ = json_outputter(data,threads)

    issues_dict = list()
    # update processing collection
    # get all bids and issues just dumped
    for r in refs:
        issues_dict.append((r["bid"], r["issue"]))

    db.references.insert_many(refs) # insert references. NOTE that in loader we already skip already parsed documents, no need to check again now.
    for bid,issue in list(set(issues_dict)):
        try:
            if not issue or len(issue) == 0:
                processing_info = Processing.objects(type_document="monograph", bid=bid).get()
            else:
                processing_info = Processing.objects(type_document="issue", number=issue, bid=bid).get()
            if not processing_info.is_parsed:
                processing_info.is_parsed = True
                processing_info.updated_at = datetime.now()
                processing_info.save()
                logger.info("Updated item in Processing: %s, %s" % (bid, issue))
        except Exception as e:
            logger.warning(e)
            logger.warning("Missing item in Processing: %s, %s"%(bid,issue))
            continue

if __name__=="__main__":

    # NB consider the how_many constant above: how many bids to process in one run

    # choose the database to parse. Only documents that have not been parsed, and that have their full text available will be considered.
    db = "mongo_dev"  # "mongo_prod" "mongo_dev" "mongo_sand"
    config = ConfigParser(allow_no_value=False)
    config.read("reference_parsing/config.conf")

    mongo_db = config.get(db, 'db-name')
    mongo_user = config.get(db, 'username')
    mongo_pwd = config.get(db, 'password')
    mongo_auth = config.get(db, 'auth-db')
    mongo_host = config.get(db, 'db-host')
    mongo_port = config.get(db, 'db-port')
    client = MongoClient(mongo_host)
    db = client[mongo_db]
    db.authenticate(mongo_user, mongo_pwd, source=mongo_auth)

    engineconnect(mongo_db, username=mongo_user
                               , password=mongo_pwd
                               , authentication_source=mongo_auth
                               , host=mongo_host
                               , port=int(mongo_port))

    data = loader(db)
    print("Data loaded: %d documents"%len(data))
    data = parser(data,threads=7)
    print("Data parsed.")
    if len(data) > 0:
        ingester(db,data,threads=7)
        print("Data ingested.")
