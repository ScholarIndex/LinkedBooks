# -*- coding: utf-8 -*-
"""
This code defines an annotation and text loader, which can be used for parsing references (train, test, parse).
"""
__author__ = """Giovanni Colavizza"""

import sys
sys.path += ["../../", "../", "./"]
import codecs, logging
from collections import OrderedDict, defaultdict
from configparser import ConfigParser
from datetime import datetime
# Mongo
from pymongo import MongoClient
from dbmodels import *

# Internal reference: some annotations were old stuff, to be removed. Here they are defined accordingly. DO NOT CHANGE.
general_entities = ["secondary-partial", "secondary-full", "primary-partial", "primary-full", "meta-annotation"]
discard_entities = ["full","partial","implicit"]

# support functions
class ReportError(Exception):
    """
    Personalized class to report errors with a message
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def using_split2(text, _len=len):
    """
    LEGACY
    Takes a text in input and returns a list of tuples with surface, start and end offsets for each token.
    From: http://stackoverflow.com/questions/9518806/how-to-split-a-string-on-whitespace-and-retain-offsets-and-lengths-of-words

    :param text: a string of text
    :param _len: the way offsets are calculated, default using len of token
    :return: list of tuples with surface, start and end offsets for each token plus their position in the line and the line number
    """
    lines = text.split("\n")
    index = text.index
    offsets = []
    append = offsets.append
    running_offset = 0
    for l, line in enumerate(lines):
        words = line.split()
        for n,word in enumerate(words):
            word_offset = index(word, running_offset)
            word_len = _len(word)
            running_offset = word_offset + word_len
            append((word, word_offset, running_offset - 1, n+1, l+1))
    return offsets

def loader(bids, use_journals=True, use_monographs=True, which_mongo="mongo_sand", load_ann=True, prune_lines=True, verbose=False):
    """
    Loads texts and eventually annotations, to be used for training and parsing.

    :param bids: bids of journals or monographs to parse. If empty, use all available. If bids is specified, use_whatever is not considered
    :param: use_journals: if to use journals
    :param: use_monographs: if to use monographs
    :param which_mongo: the name of the databsdr to use, cf. config.conf
    :param load_ann: if to load annotations or just the text
    :param prune_lines: if to discard lines without annotations (only for golden pages)
    :param verbose: if to provide detailed logging or not
    :return: A list of documents to parse, a dictionary report on the loaded annotations
    """
    #logging.basicConfig(filename="loader_%s.log"%which_mongo, level=logging.WARNING)
    #if verbose:
    logging.basicConfig(filename="loader_%s.log"%which_mongo, level=logging.INFO)
    logger = logging.getLogger()

    # Load collections
    config = ConfigParser(allow_no_value=False)
    config.read("config.conf")
    print('Read configuration file.')
    mongo_db = config.get(which_mongo, 'db-name')
    mongo_user = config.get(which_mongo, 'username')
    mongo_pwd = config.get(which_mongo, 'password')
    mongo_auth = config.get(which_mongo, 'auth-db')
    mongo_host = config.get(which_mongo, 'db-host')
    mongo_port = config.get(which_mongo, 'db-port')
    con = MongoClient(mongo_host, port=int(mongo_port), **{"socketKeepAlive": True})
    db = con[mongo_db]
    db.authenticate(mongo_user, mongo_pwd, source=mongo_auth)

    print('Loaded Mongo.')

    # Initialize counters and data structures
    data = list() # a list of documents with all data for every page (both annotated and not)
    report = dict()
    total_general_annotations = 0
    total_general_annotations_j = 0
    total_general_annotations_m = 0
    tot_general_per_class = {x: 0 for x in general_entities}
    tot_general_per_class_j = {x: 0 for x in general_entities}
    tot_general_per_class_m = {x: 0 for x in general_entities}
    tot_general_per_class_nofilter = {x: 0 for x in general_entities}
    tot_specific_per_class = defaultdict(int)
    total_specific_annotations = 0
    total_ann_pages = 1
    total_annotated_docs = 1
    total_pages = 1
    no_ann_no_golden = list()
    no_ann_in_golden = list()

    # check boundaries of loading
    if not bids or len(bids) == 0:
        if use_journals and use_monographs:
            bids = list(set([x["bid"] for x in db.documents.find()]))
        elif use_journals:
            bids = list(set([x["bid"] for x in db.documents.find() if x["type"] == "journal_issue"]))
        elif use_monographs:
            bids = list(set([x["bid"] for x in db.documents.find() if x["type"] == "monograph"]))
    print('Bids list established.')

    for bid in bids:
        # define detailed reporting files
        job = bid+"-"+str(load_ann)+"-"+str(prune_lines)
        errors_to_correct = codecs.open("logs/errors_to_correct-%s-%s.csv"%(job,which_mongo),"w+",encoding="utf-8")

        # Define data to be loaded
        # Build query for identifiers
        query = {"bid": bid}
        n_docs = db.documents.count(query)
        print("Number of loaded docs: " + str(n_docs))
        try:
            report.update({"positive_results": n_docs, "overall_documents": db.documents.count({})})
        except:
            raise ReportError("Query error")

        for doc in db.documents.find(query,no_cursor_timeout=True):
            #print(str(doc["_id"]))
            is_doc_annotated = False
            doc_type = doc["type"]
            doc_number = ""
            if doc_type == "journal_issue" and "number" in doc.keys():
                doc_number = doc["number"]
            doc_id = doc["_id"]
            pages = OrderedDict()

            for page in db.pages.find({"_id": {"$in": doc["pages"]}}):
                page_number = int(page["single_page_file_number"])
                pages[page_number] = {"offsets":list(),"page_id":"","page_mongo_id":"","is_annotated":False,"single_page_file_number":page_number}
                page_id = bid+"-"+doc_number+"-page-"+page["filename"].split("-")[-1].split(".")[0]
                page_mongo_id = page["_id"]
                total_pages += 1
                container = list()
                annotated = False
                # load text
                # create a reverse index
                offsets = dict()
                annotations_in_pageline = list()
                if prune_lines and page["is_annotated"]:
                    for a in db.annotations.find({"_id": {"$in": page["annotations_ids"]}}):
                        for pos in a["positions"]:
                            annotations_in_pageline.append(pos["line_n"])
                    annotations_in_pageline = list(set(annotations_in_pageline))
                for line in page["lines"]:
                    line_number = line["line_number"]
                    if line_number is None:
                        continue
                    if prune_lines and page["is_annotated"] and line_number not in annotations_in_pageline:
                        continue # keep only lines with annotations
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

                # load annotations
                if load_ann and page["is_annotated"]:
                    print("ann:" + str(len(page["annotations_ids"]))+"\n")
                    if len(page["annotations_ids"]) == 0:
                        print(page_mongo_id)
                        if page["is_annotated"] and page["in_golden"]:
                            no_ann_in_golden.append((bid,doc_number,page_mongo_id,page_number))
                        else:
                            no_ann_no_golden.append((bid,doc_number,page_mongo_id,page_number))
                        continue
                    annotated = True
                    is_doc_annotated = True

                    # general and beginend
                    for annotation in db.annotations.find({"_id": {"$in": page["annotations_ids"]}}):
                        if annotation["entity_type"] in discard_entities:
                            continue
                            # generic entity
                        elif annotation["entity_type"] in general_entities:
                            tot_general_per_class_nofilter[annotation["entity_type"]] += 1
                            positions = sorted([(x["start"], x["end"]) for x in annotation["positions"]],
                                               key=lambda x: x[0])
                            ann_span = (positions[0][0], positions[-1][1])
                            keys = sorted([x for x in offsets.keys() if x >= ann_span[0] and x <= ann_span[1]])
                            if len(keys) < 1:
                                keys = sorted([x for x in offsets.keys() if offsets[x]["end"] > ann_span[0] and x <= ann_span[1]])  # to fix a possible miss alignment with offsets and annotation spans
                            if len(keys) < 1:
                                print("Problem with general keys " + str(keys) + "\n")
                                print(str(ann_span[0]) + " - " + str(ann_span[1]) + "\n")
                                print(str(positions) + "\n")
                                print(str(sorted([x for x in offsets.keys() if x >= ann_span[0] - 10 and x <= ann_span[1] + 10])) + "\n")
                                continue
                            for token_key in keys:
                                if "meta-annotation" in offsets[token_key]["general_category"] and annotation["entity_type"] != "meta-annotation":
                                    continue  # to avoid overwriting meta-annotations!
                                offsets[token_key]["general_category"] = annotation["entity_type"]
                                offsets[token_key]["beginend"] = "i"
                                offsets[token_key]["taggedbe"] = "i-" + annotation["entity_type"]
                            if ("b-meta-annotation" in offsets[keys[0]]["taggedbe"] or "e-meta-annotation" in offsets[keys[-1]]["taggedbe"]) and annotation["entity_type"] != "meta-annotation":
                                continue  # to avoid overwriting meta-annotations!
                            if not annotation["continuation"]:  # take out begin if there is a continuation
                                offsets[keys[0]]["beginend"] = "b"
                                offsets[keys[0]]["taggedbe"] = "b-" + annotation["entity_type"]
                            offsets[keys[-1]]["beginend"] = "e"
                            offsets[keys[-1]]["taggedbe"] = "e-" + annotation["entity_type"]
                            total_general_annotations += 1
                            if doc_type == "journal_issue":
                                total_general_annotations_j += 1
                                tot_general_per_class_j[annotation["entity_type"]] += 1
                            else:
                                total_general_annotations_m += 1
                                tot_general_per_class_m[annotation["entity_type"]] += 1
                            tot_general_per_class[annotation["entity_type"]] += 1
                        # specific
                        else:
                            positions = sorted([(x["start"], x["end"]) for x in annotation["positions"]],
                                               key=lambda x: x[0])
                            ann_span = (positions[0][0], positions[-1][1])
                            keys = sorted([x for x in offsets.keys() if x >= ann_span[0] and x <= ann_span[1]])
                            if len(keys) < 1:
                                keys = sorted([x for x in offsets.keys() if offsets[x]["end"] > ann_span[0] and x <= ann_span[1]])  # to fix a possible miss alignment with offsets and annotation spans
                            if len(keys) < 1:
                                print("Problem with specific keys " + str(keys) + "\n")
                                print(str(ann_span[0]) + " - " + str(ann_span[1]))
                                print(str(positions))
                                print(str(sorted([x for x in offsets.keys() if x >= ann_span[0] - 10 and x <= ann_span[1] + 10])) + "\n")
                                continue
                            for token_key in keys:
                                offsets[token_key]["specific_category"] = annotation["entity_type"]
                            total_specific_annotations += 1
                            tot_specific_per_class[annotation["entity_type"]] += 1

                # store meta
                full_cit_counter = 0
                for start,token in offsets.items():

                    if annotated and load_ann:
                        new_token = ((token["surface"],start,token["end"],token["position"],token["line"],token["bid"]),(token["italics"],token["bold"],token["size"]),(token["general_category"],token["specific_category"],token["beginend"],token["taggedbe"]))
                        if token["beginend"] == "e":
                            full_cit_counter += 1
                    else:
                        new_token = ((token["surface"],start,token["end"],token["position"],token["line"],token["bid"]),(token["italics"],token["bold"],token["size"]))
                    container.append(new_token)

                # store page in data
                if annotated:
                    # note we don't check for split_train_pages because in this way the loader exports the last citations in container as well.
                    total_ann_pages += 1
                    pages[page_number]["is_annotated"] = True
                # store annotated page in pertinent article
                pages[page_number]["offsets"] = container
                pages[page_number]["page_id"] = page_id
                pages[page_number]["page_mongo_id"] = page_mongo_id

            if is_doc_annotated:
                total_annotated_docs += 1
            doc_data = {"doc_mongo_id":doc_id,"doc_type":doc_type,"pages":pages,"bid":bid,"doc_number":doc_number}
            data.append(doc_data)

        errors_to_correct.close()

    print(tot_general_per_class)
    print(tot_general_per_class_nofilter)

    print(len(set(no_ann_no_golden)))
    print(len(set(no_ann_in_golden)))

    print("No ann and in golden:")
    print(set(no_ann_in_golden))
    print("\nNo ann and no golden:")
    print(set(no_ann_no_golden))

    report.update({"total_annotations": total_specific_annotations+total_general_annotations,
                   "total_specific": total_specific_annotations,
                   "total_general": total_general_annotations,
                   "total_general_j": total_general_annotations_j,
                   "total_general_m": total_general_annotations_m,
                   "total_general_per_class": tot_general_per_class,
                   "total_general_per_class_j": tot_general_per_class_j,
                   "total_general_per_class_m": tot_general_per_class_m,
                   "total_general_per_class_nofilter": tot_general_per_class_nofilter,
                   "total_specific_per_class": tot_specific_per_class,
                   "avg annotated pages per annotated doc": total_ann_pages/total_annotated_docs,
                   "avg annotated pages over total pages": total_ann_pages/total_pages,
                   "avg annotated docs over total docs": total_annotated_docs/len(data)})
    return data, report



