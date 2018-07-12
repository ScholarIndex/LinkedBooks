"""Content ingestion script adapted for Classics articles in Brat."""
# !/usr/bin/python
# -*- coding: UTF-8 -*-
# author: Matteo Romanello, matteo.romanello@epfl.ch

import argparse
import os
import glob
import sys
import codecs
import ipdb as pdb
sys.path.append("../")
from commons.dbmodels import LBDocument, Page, Line, Token
import content_ingester
from content_ingester.version import __version__
from mongoengine import connect
from datetime import datetime
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
from commons.functions import split_and_tokenise_standoff


def detect_documents(base_dir):
    docs = [
        {
            "bid": file.replace("-doc-1.ann", "").replace(".txt", "")
            .replace("ocr_", "").replace("_", "/"),
            "path": os.path.join(base_dir, file.replace("-doc-1.ann", ""))
        }
        for file in os.listdir(base_dir)
        if ".ann" in file
    ]
    return docs


def process_document(bid, doc_path, brat_suffix="-doc-1.txt"):
    document = LBDocument(
        path=doc_path,
        type="journal_issue",
        bid=bid,
        internal_id=doc_path,
        content_ingester_version=__version__ + "brat",
        ingestion_timestamp=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )

    page_n = 1
    txt_fname = "{}{}".format(doc_path, brat_suffix)

    with codecs.open(txt_fname, "r", "utf-8") as f:
        text = f.read()

    page = Page(
        filename=txt_fname,
        single_page_file_number=page_n,
        fulltext=text
    )
    document.pages = [page]

    current_line_n = 1
    n = 1
    tokens = split_and_tokenise_standoff(page.fulltext)
    current_line = Line()
    # pdb.set_trace()

    for surface, offset_start, offset_end, pos_in_line, line_number in tokens:

        if(line_number == current_line_n):
            current_line.line_number = line_number
            token = Token(
                offset_start=offset_start,
                offset_end=offset_end,
                surface=surface,
                features=[],
                token_number=n
            )
            current_line.tokens.append(token)
            n += 1
        else:
            if(pos_in_line == 1):
                page.lines.append(current_line)
                current_line_n = line_number
            current_line = Line()
            token = Token(
                offset_start=offset_start,
                offset_end=offset_end,
                surface=surface,
                features=[],
                token_number=n
            )
            current_line.tokens.append(token)
            n += 1
        if(n == len(tokens) + 1):
            page.lines.append(current_line)

    return document


def process_folder(directory, bid, bid_metadata_id):
    """
    Returns a tuple: (bid,json_document)
    """
    logger.info(directory)
    document = {}
    bid = directory.split('/')[len(directory.split('/'))-3]
    issue_number = directory.split('/')[len(directory.split('/'))-2]
    doc_id = "%s-%s"%(bid,issue_number)
    print(doc_id)
    logger.info('Processing folder %s'%directory)
    document["path"] = directory
    document["bid"]=bid
    document["id"]=doc_id
    document["number"]=issue_number
    document["content_ingester_version"]=__version__
    document["ingestion_timestamp"]=datetime.utcnow()
    document["type"]="journal_issue"
    document["metadata_id"]=bid_metadata_id
    page_numbers = {int(os.path.basename(fname).replace("page-","").split('.')[0]):os.path.basename(fname).split('.')[0]
                                                    for fname in glob.glob("%s/*.ann"%directory)}
    document["pages"] = []
    for page_n in sorted(page_numbers):
        page = {}
        page["filename"] = "%s/%s%s"%(directory,page_numbers[page_n],".txt")
        page["fulltext"] = codecs.open(page["filename"],"r","utf-8").read()
        page["single_page_file_number"] = page_n
        page["printed_page_number"] = [page_n]
        page["lines"] = []
        page["is_annotated"]=False
        page["in_golden"]=False
        page["has_footnotes"]=False
        document["pages"].append(page)
        current_line_n = 1
        current_line = {"tokens":[],"line_number":None,"in_footnote":False}
        n = 1
        tokens = split_and_tokenise_standoff(page["fulltext"])
        for surface,offset_start,offset_end,pos_in_line,line_number in tokens:
            if(line_number == current_line_n):
                current_line["line_number"] = line_number
                token = {
                        "offset_start":offset_start
                        ,"offset_end":offset_end
                        ,"surface":surface
                        ,"features":[]
                        ,"token_number":n
                }
                current_line["tokens"].append(token)
                n+=1
            else:
                if(pos_in_line==1):
                    page["lines"].append(current_line)
                    current_line_n = line_number
                current_line = {"tokens":[],"line_number":line_number,"in_footnote":False}
                token = {
                        "offset_start":offset_start
                        ,"offset_end":offset_end
                        ,"surface":surface
                        ,"features":[]
                        ,"token_number":n
                }
                current_line["tokens"].append(token)
                n+=1
            if(n==len(tokens)+1):
                page["lines"].append(current_line)
        #pprint.pprint(page)
    return document


def save_document_to_db(document):
    """
    Stores the ingested content into a collection of the MongoDB
    """
    [p.save() for p in document.pages]
    document.save()
    return


def main():
    parser = argparse.ArgumentParser(description="")
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument("--log-file", help="The destination file for the logging",type=str)
    requiredNamed.add_argument("--log-level", help="The log level",type=str,required=True)
    requiredNamed.add_argument("--db-host", help="The address of the MongoDB",type=str,required=True)
    requiredNamed.add_argument("--db-name", help="The name of the database to use",type=str,required=True)
    requiredNamed.add_argument("--data-dir", help="The directory with the data to ingest",type=str,required=True)
    requiredNamed.add_argument("--username", help="TODO",type=str,required=True)
    requiredNamed.add_argument("--password", help="TODO",type=str,required=True)
    requiredNamed.add_argument("--auth-db", help="TODO",type=str,required=True)
    requiredNamed.add_argument("--mongo-port", help="Port for the MongoDB connection",required=False, default=27017, type=int)
    requiredNamed.add_argument("--documents-type", help="TODO", choices=["monographs","journals"],type=str,required=True)
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
    logger.debug("Parameters from the CLI: %s" % args)
    #
    # Initialise the DB connection for the mongoengine models
    #
    db = connect(
        args.db_name,
        username=args.username,
        password=args.password,
        authentication_source=args.auth_db,
        host=args.db_host,
        port=int(args.mongo_port)
    )

    logger.info(db)
    documents = detect_documents(args.data_dir)
    mongo_documents = [
        process_document(d["bid"], d["path"]) for d in documents
    ]
    result = [save_document_to_db(md) for md in mongo_documents]
    logger.info(result)


if __name__ == "__main__":
    main()
