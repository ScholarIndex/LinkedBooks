# -*- coding: utf-8 -*-
"""
USED una tantum to refactor the journal_references collection.
Note that the old collection references (monograph reference lists) is discarded: monographs are going to ba parsed again.

this script:
1- copies the journal_references collection to another collection: sand, test and production databases
2- uniforms the data model in so doing
3- updated Processing
4- validates everything using the mongoengine

"""
__author__ = """Giovanni Colavizza"""

from collections import OrderedDict
import logging
logging.basicConfig(filename="logs/xml_parser.log", level=logging.INFO)
logger = logging.getLogger(__name__)
from configparser import ConfigParser
from datetime import datetime
# Mongo
from pymongo import MongoClient, TEXT, ASCENDING
from mongoengine import connect as engineconnect
# Test models
from commons.dbmodels import *

# Establish Mongo connections
config = ConfigParser(allow_no_value=False)
config.read("config.conf")
logger.info('Read configuration file.')

# SANDBOX the playground
db = "mongo_sand"
mongo_user = config.get(db, 'username')
mongo_pwd = config.get(db, 'password')
mongo_auth = config.get(db, 'auth-db')
mongo_host = config.get(db, 'db-host')
mongo_port = config.get(db, 'db-port')
con = MongoClient(mongo_host,port=int(mongo_port), **{"socketKeepAlive":True})
con.linkedbooks_sandbox.authenticate(mongo_user, mongo_pwd, source=mongo_auth)
db_sand = con.linkedbooks_sandbox
# SOURCE the collection where journal_references is
db = "mongo_source"
mongo_user = config.get(db, 'username')
mongo_pwd = config.get(db, 'password')
mongo_auth = config.get(db, 'auth-db')
mongo_host = config.get(db, 'db-host')
mongo_port = config.get(db, 'db-port')
con = MongoClient(mongo_host,port=int(mongo_port), **{"socketKeepAlive":True})
con.linkedbooks_dev.authenticate(mongo_user, mongo_pwd, source=mongo_auth)
db_source = con.linkedbooks_dev
# DEV the development DB
db = "mongo_dev"
mongo_user = config.get(db, 'username')
mongo_pwd = config.get(db, 'password')
mongo_auth = config.get(db, 'auth-db')
mongo_host = config.get(db, 'db-host')
mongo_port = config.get(db, 'db-port')
con = MongoClient(mongo_host,port=int(mongo_port), **{"socketKeepAlive":True})
con.linkedbooks_refactored.authenticate(mongo_user, mongo_pwd, source=mongo_auth)
db_dev = con.linkedbooks_refactored
# PROD the production DB, only connect if explicitly called
db = "mongo_prod"
mongo_user = config.get(db, 'username')
mongo_pwd = config.get(db, 'password')
mongo_auth = config.get(db, 'auth-db')
mongo_host = config.get(db, 'db-host')
mongo_port = config.get(db, 'db-port')
con = MongoClient(mongo_host,port=int(mongo_port), connect=False, **{"socketKeepAlive":True})
con.linkedbooks_refactored.authenticate(mongo_user, mongo_pwd, source=mongo_auth)
db_prod = con.linkedbooks_refactored
logger.info('Loaded Mongo dbs configs.')

def transfer_collection(destination_db,db):
    """
    Transfer the journal_references collection to other databases, after refactoring
    :param destination_db: Mongo connector to the right destination database
    :param db: config.conf name of the destination database
    :return: Nothing.
    """

    # IMPORT journal_references collection from SOURCE to new database
    references = list()
    pages_dict = dict()
    # index of items from metadata which are valid
    valid_documents = list()
    for m in destination_db.metadata.find():
        if m["marked_as_removed"]:
            continue
        if m["type_document"] == "monograph":
            continue  # we only have journals here
        else:
            for d in m["issues"]:
                if d["marked_as_removed"]:
                    continue
                else:
                    valid_documents.append((m["bid"], d["foldername"]))
    for reference in db_source.journal_references.find(no_cursor_timeout=True):
        contents = OrderedDict(sorted(reference["contents"].items(),key=lambda x:int(x[0])))
        pages = set([x["page_id"] for x in contents.values()])
        for p in pages:
            if p not in pages_dict.keys():
                try:
                    items = p.split("-")
                    bid = items[0]
                    image = items[-1]
                    issue = "-".join(items[1:-2])
                    image = int(image)
                except:
                    print(p)
                    continue
                if (bid,issue) in valid_documents:
                    document = destination_db.documents.find_one({"bid":bid,"number":issue})
                else:
                    split_issue = issue.split("_")
                    issue = "_".join(split_issue[:-1])
                    issue = issue + "." + split_issue[-1]
                    if (bid, issue) in valid_documents:
                        document = destination_db.documents.find_one({"bid": bid, "number": issue})
                    else:
                        logger.info("MISSING DOCUMENT: %s, %s, %s" % (bid, issue, p))
                        continue
                    logger.info("Found a mark as removed: %s, %s" % (bid, issue))
                    #logger.warning("MISSING DOCUMENT: %s, %s, %s"%(bid,issue,p))
                    #continue
                try:
                    page = destination_db.pages.find_one({"single_page_file_number":image,"_id":{"$in":document["pages"]}})
                except:
                    logger.warning("MISSING PAGE: %s, %s, %s" % (bid, issue, p))
                    continue
                pages_dict[p] = {"id":page["_id"],"issue":issue}
        issue = reference["issue"]
        for c in contents.values():
            try:
                c["page_mongo_id"] = pages_dict[c["page_id"]]["id"]
                issue = pages_dict[c["page_id"]]["issue"]
            except:
                logger.warning("MISSING PAGE IN DICT: %s" % c["page_id"])
                c["page_mongo_id"] = ""
        r = {"ref_type":reference["ref_type"],
             "reference_string":" ".join([x["surface"] for x in contents.values()]),
             "in_golden":reference["in_golden"],
             "order_in_page":reference["order_in_page"],
             "continuation_candidate_in":reference["continuation_candidate_in"],
             "continuation_candidate_out":reference["continuation_candidate_out"],
             "continuation":reference["continuation"],
             "bid":reference["bid"],
             "issue":issue,
             "contents":contents,
             "updated_at":datetime.now()
             }

        references.append(r)
    destination_db.drop_collection("references")
    destination_db.references.insert_many(references)
    destination_db.references.create_index([('reference_string', TEXT),('bid', TEXT),('issue', TEXT)], default_language='none')
    destination_db.references.create_index([('contents.1.single_page_file_number',ASCENDING)],unique=False)
    logger.info('Created journal_references collection into database %s'%db)

def updates_checks(destination_db,db):
    """
    Checkes the new references collection is properly done, updates the Processing collection.
    Note that this assumes the references collection contains objects that have been fully parsed (reason why we do not consider monograph reference lists for now: they have not!)

    :param destination_db: Mongo connector to the right destination database
    :param db: config.conf name of the destination database
    :return: Nothing.
    """

    issues_dict = list()
    # update processing collection
    # get all bids and issues just dumped
    for r in destination_db.references.find():
        issues_dict.append((r["bid"],r["issue"]))

    mongo_db = config.get(db, 'db-name')
    mongo_user = config.get(db, 'username')
    mongo_pwd = config.get(db, 'password')
    mongo_auth = config.get(db, 'auth-db')
    mongo_host = config.get(db, 'db-host')
    mongo_port = config.get(db, 'db-port')
    logger.debug(engineconnect(mongo_db
                               , username=mongo_user
                               , password=mongo_pwd
                               , authentication_source=mongo_auth
                               , host=mongo_host
                               , port=int(mongo_port)))

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
        except:
            logger.warning("Missing item in Processing: %s, %s"%(bid,issue))
            continue

    logger.info('Updated Processing collection into database %s'%db)

    # AT THE END, TEST COLLECTION
    objects = Reference.objects
    logger.info("The database contains %d Reference objects"%len(objects))

transfer_collection(db_sand,"mongo_sand")
updates_checks(db_sand,"mongo_sand")
#transfer_collection(db_dev,"mongo_dev")
#updates_checks(db_dev,"mongo_dev")
#transfer_collection(db_prod,"mongo_prod")
#updates_checks(db_prod,"mongo_prod")