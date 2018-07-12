#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""

This script takes care of ingesting the annotations produced within Brat for the LinkedBooks project into a MongoDB.
(run `python annotation_connector/ingester.py --help` for the script usage).

Example of usage:

    python annotation_connector/ingester.py --filter-file=tests/test_data/loggerPERIODICI.ods_ArchivioVeneto.tsv \
    --data-dir=tests/test_data/annotations/journals/ --annotation-type=manual \
    --config-file=annotation_connector/config_files/LB_machine_sandbox.conf 

"""
__author__ = """Giovanni Colavizza, Matteo Romanello"""
import pdb
import codecs
import pdb
import copy
import pprint
import traceback
import logging
import os
import sys
import glob
from configparser import ConfigParser
import argparse
import pandas as pd
from bson.objectid import ObjectId
import pymongo
from pymongo import MongoClient
sys.path += ["../", "./", "../../"]
from commons.dbmodels import *
from mongoengine.errors import NotUniqueError
from mongoengine import connect
from content_ingester import page_number2image_name, convert_issue_number
from annotation_connector.version import __version__
from datetime import datetime

class AnnotationIngestionError(Exception):
    """
    Raised when trying to ingest annotations for a document whose content 
    was not yet ingested.  
    """
    def __init__(self, docid):
        self.docid = docid

logger = logging.getLogger(__name__) 

container_annotations = ['primary-partial'
                        ,'primary-full'
                        ,'secondary-partial'
                        ,'secondary-full']

def find_ngrams(input_list, n):
    """
    taken from http://locallyoptimal.com/blog/2013/01/20/elegant-n-gram-generation-in-python/
    """
    return zip(*[input_list[i:] for i in range(n)])

def find_newlines(text,newline='\n'):
    positions = []
    last_position = 0
    if(text.find(newline) == -1):
        return positions
    else:
        while(text.find(newline,last_position+1)>-1):
            last_position = text.find(newline,last_position+1)
            positions.append((last_position,last_position+len(newline)))
        return positions

def find_linenumber_for_string(offset_start, offset_end, newline_offsets):
    """
    TODO
    """
    for n,nl_offset in enumerate(newline_offsets):
        #print offset_start,offset_end,nl_offset
        if(offset_start <= nl_offset[0] and offset_end <= nl_offset[0]):
            return n+1
    if n==len(newline_offsets)-1:
        return n+2

def sort_annotations_by_offset(annotations):
    """
    This function expects as input a list of dictionaries with this structure:

    {'ann_id': u'T34',
     'continuation': False,
     'entity_type': u'Registry',
     'positions': [{'end': 2465, 'start': 2448}],
     'surface': u'reg 38 Padavinus,'},

    And sorts them by offset. When the annotations spans more than one token 
    (i.e. there is more than one position in `positions`) the offset_start 
    of the first token is considered.
    """
    return sorted(annotations, key=lambda k: k['positions'][0]['start'])

def read_ann_file(fileid, ann_dir):
    """
    # get entities and relations from an .ann file
    >> entities, relations = read_ann_file("page-0034","./LO10015953/1998_151/")
    
    # print the first entity
    >> entities[entities.keys()[0]]
    {'ann_id': u'T38',
     'continuation': False,
     'entity_type': u'PublicationYear',
     'positions': [{'end': 2550, 'start': 2545}]}
    
    # print the first relation
    >> relations[relations.keys()[0]]
    {'ann_id': u'R4',
     'arguments': (u'T30', u'T31'),
     'relation_type': u'ContainedIN'}
    """
    ann_file = "%s/%s.ann"%(ann_dir,fileid)
    with codecs.open(ann_file, 'r', 'utf-8') as f:
        data = f.read()
    rows = data.split('\n')
    entities = {}
    ent_count = 0
    relations = {}
    #annotations = []
    for row in rows:
        cols = row.split("\t")
        ann_id = cols[0]
        if(u"#" in cols[0]):
            tmp = cols[1].split()[1:]," ",cols[2]
            annotations.append(tmp)
        elif(len(cols)==3 and u"T" in cols[0]):
            # is an entity
            ent_count += 1
            ent_type = cols[1].split()[0]
            ranges = cols[1].replace("%s"%ent_type,"")
            if ";" in ranges:
                ranges = [{"start":int(r.split()[0]),"end":int(r.split()[1])} for r in ranges.split(';')]
            else:
                ranges = [{"start":int(ranges.split()[0]),"end":int(ranges.split()[1])}]
            entities[cols[0]] = {"ann_id":ann_id
                                ,"entity_type": ent_type
                                ,"positions": ranges
                                ,"surface":cols[2]
                                ,"continuation":False}
        elif(len(cols)>=2 and u"R" in cols[0]):
            rel_type, arg1, arg2 = cols[1].split()
            relations[cols[0]] = {"ann_id":ann_id
                                ,"arguments":(arg1.split(":")[1], arg2.split(":")[1])
                                ,"relation_type":rel_type}
        else:
            if(len(cols)>1):
                if(cols[1].split()[0]=="Continuation"):
                    continued_entity_id = cols[1].split()[1]
                    #print cols[1].split()[0],continued_entity_id
                    entities[continued_entity_id]["continuation"] = True
    return entities, relations

def ingest_annotations(directory, annotation_type):
    """
    This function ingests the annotations from an input of .ann files (brat
        stand-off annotation format).

    :param directory: the directory containing the annotations to be ingested. 
    :type directory: str.
    :param annotation_type: the type of annotations being ingested.
    :type annotation_type: str. accepted values = ["manual" | "automatic"].
    :returns: list -- the ids (type = `bson.objectid.ObjectId`) of the pages from which
    annotations where ingested.
    """
    in_golden = annotation_type == "manual"
    doc_type = None
    issue_number = None
    bid = None
    annotated_pages = []
    file_prefix = "page"
    try:
        bid = directory.split('/')[len(directory.split('/'))-1:][0]
        record = Metadata.objects(bid=bid).first()
        assert record is not None
    except Exception as e:
        bid,issue_number =  directory.split('/')[len(directory.split('/'))-2:]
        record = Metadata.objects(bid=bid).first()
    try:
        doc_type = record["type_document"]
    except Exception as e:
        doc_type = None
        logger.warning('The record for %s is not in MongoDB'%bid)
    try:
        page_numbers = {int(os.path.basename(fname).replace("page-","").split('.')[0]):os.path.basename(fname).split('.')[0] 
                                                        for fname in glob.glob("%s/*.ann"%directory)}
    except Exception as e:
        page_numbers = {int(os.path.basename(fname).replace("image-","").split('.')[0]):os.path.basename(fname).split('.')[0] 
                                                    for fname in glob.glob("%s/*.ann"%directory)}
        file_prefix = "image"
    # TODO: handle the exception of document not in the DB
    logger.info("Ingesting the annotations from directory \"%s\""%directory)
    if(issue_number != None):
        logger.info("Found document %s-%s [type=%s]"%(bid,issue_number,doc_type))
    else:
        logger.info("Found document %s [type=%s]"%(bid,doc_type))
    try:
        if(doc_type=="journal"):
            doc = LBDocument.objects(internal_id="%s-%s"%(bid,issue_number)).first()
            if doc is None:
                doc = LBDocument.objects(internal_id="%s-%s"%(bid, convert_issue_number(issue_number))).first()
        elif(doc_type=="monograph"):
            doc = LBDocument.objects(bid=bid).first()
        logger.info("%s has %i pages"%(doc.internal_id, len(doc.pages)))
        for page_n in sorted(page_numbers):
            logger.debug("Reading in annotations for page %i from file %s/ %s"%(page_n,directory,page_numbers[page_n]))
            entities_with_continuations = {}
            entities,relations = read_ann_file(page_numbers[page_n],directory)
            fulltext = codecs.open("%s/%s.txt"%(directory,page_number2image_name(page_n, string=file_prefix)),'r', 'utf-8').read()
            line_breaks = find_newlines(fulltext)
            #logger.info("Found %i entities, %i relation in %s"%(len(entities), len(relations), directory))
            doc_id = "%s-%s-%s"%(bid, issue_number, page_numbers[page_n])
            try:
                page = next((page for page in doc.pages if page.single_page_file_number==page_n))
                if(page["in_golden"]==True):
                    annotated_pages.append(page.id)
                    logger.info("found %i entities in %s (p. %i)"%(len(entities),doc_id,page_n))
                    logger.info("found %i relations in %s (p. %i)"%(len(relations.keys()),doc_id,page_n))
                    """
                    Parse the `ContainedIN` relations and identify annotations that should be merged together.
                    IDs of candidates for merging are stored in a dict(), e.g. {"T1":["T2","T4"]}
                    """
                    entities_with_continuations = {}
                    if len(relations.keys())>0:
                        for relation_key in relations:
                            args = relations[relation_key]["arguments"]
                            if args[0] in entities_with_continuations:
                                entities_with_continuations[args[0]].append(args[1])
                            else:
                                entities_with_continuations[args[0]] = [args[1]]
                        logger.debug("(%s-%s) entities to be merged: %s"%(doc_id,page_n,entities_with_continuations))
                    """
                    Create the annotations (`entities` dict). 
                    Later they will be stored into the MongoDB
                    """
                    for entity in entities:
                        entities[entity]["ingestion_timestamp"] = datetime.utcnow()
                        entities[entity]["annotation_ingester_version"] = __version__
                        entities[entity]["entity_type"] = entities[entity]["entity_type"].lower( )
                        entities[entity]["filename"] = "%s/%s%s"%(directory,page_numbers[page_n],".ann")
                        if(doc_type=="journal"):
                            entities[entity]["bid"] = bid
                            entities[entity]["pageid"] = doc_id
                        elif(doc_type=="monograph"):
                            entities[entity]["bid"] = bid
                            entities[entity]["pageid"] = "%s-%s"%(bid,page_numbers[page_n])
                        entities[entity]["container"] = entities[entity]["entity_type"] in container_annotations
                        # ref to page_id (from content_loader) ✓
                        for position in entities[entity]["positions"]:
                            line_number = find_linenumber_for_string(position["start"],position["end"], line_breaks)
                            logger.debug("%s is found at line %s"%(entity,line_number))
                            position["line_n"] = line_number
                            position["page_id"] = page.id
                        positions_by_offset =  sorted(entities[entity]["positions"]
                                                    ,key=lambda position: position['start'])
                        entities[entity]["positions"] = sorted(positions_by_offset
                                                        , key=lambda position: Page.objects(id=position['page_id']).first().single_page_file_number)
                        logger.debug("Annotations %s %s"%(entity,entities[entity]))
                    """
                    Now take the candidates for merging identified above and populate the annotations.
                    Still nothing is saved into MongoDB at this stage.
                    """
                    for ann_id in entities_with_continuations:
                        try:
                            logger.debug("Starting to merge SP and SF entities into meta-annotations (%s-%s)"%(doc_id, page_n))
                            logger.debug("%s will be merged with %s"%(ann_id,"+".join(entities_with_continuations[ann_id])))
                            top_entity_types = "_".join([entities[ann_id]["entity_type"]]+[entities[annid]["entity_type"] 
                                                                                    for annid in entities_with_continuations[ann_id]])
                            logger.debug("%s"%top_entity_types)
                            new_entity = copy.deepcopy(entities)[ann_id] 
                            #container = True           
                            new_entity["ann_id"] = "%s+%s"%(ann_id,"+".join(entities_with_continuations[ann_id]))
                            new_entity["entity_type"] = "meta-annotation"
                            new_entity["top_entity_types"] = top_entity_types
                            new_entity["top_entities_ids"] = [ann_id]
                            new_entity["top_entities_ids"] += [id for id in entities_with_continuations[ann_id]]
                            fname = new_entity["filename"]
                            new_entity["filename"] = [fname]
                            for to_merge_id in entities_with_continuations[ann_id]:
                                to_merge = dict(entities)[to_merge_id]
                                new_entity["filename"]+= [to_merge["filename"]]
                                new_entity["positions"] = new_entity["positions"] + to_merge["positions"]
                            positions_by_offset =  sorted(new_entity["positions"]
                                                        ,key=lambda position: position['start'])
                            new_entity["positions"] = sorted(positions_by_offset
                                                            ,key=lambda position: Page.objects(id=position['page_id']).first().single_page_file_number)
                            new_entity["filename"] = ", ".join(list(set(new_entity["filename"])))
                            surface_start = new_entity["positions"][0]["start"]
                            surface_end = new_entity["positions"][-1]["end"]
                            new_entity["surface"] = fulltext[surface_start:surface_end]
                            entities[new_entity["ann_id"]] = new_entity
                            logger.debug(new_entity)
                        except Exception as e:
                            logger.error("The merging of %s in (%s-%s) failed with error\"%s\""%(new_entity["ann_id"],bid,page_n,e))
                    """
                    Now all annotations will be stored into the MongoDB. 
                    And some specific fields (e.g. `top_entities`) are sorted, and annotations updated 
                    accordingly in the DB. 
                    """
                    try:
                        annotations = []
                        for entity in entities.values():
                            annotation = Annotation(**entity)
                            annotation.positions = [PagePosition(**position) for position in entity["positions"]]
                            annotation.save()
                            annotations.append(annotation)
                        page.annotations_ids = [] #TODO
                        page.annotations_ids = annotations
                        page.is_annotated = True
                        page.save()
                        logger.debug("Following annotations were inserted into MongoDB: %s"%([annotation.id for annotation in annotations]))
                        logger.info("%i annotations were inserted into MongoDB"%len(annotations))
                    except Exception as e:
                        raise e
                    containers = [annotation for annotation in annotations if annotation["container"]] 
                    contained = [annotation for annotation in annotations if not annotation["container"]]
                    meta_annotations = [annotation for annotation in annotations if annotation["entity_type"]=="meta-annotation"]
                    logger.debug("meta annotations: %s"%meta_annotations)
                    """
                    Resolve the top entities in the meta-annotations: replace entity IDs with 
                    a reference to the annotation in the MongoDB.
                    """
                    for annotation in meta_annotations:
                        top_entities_ids = annotation["top_entities_ids"]
                        logger.debug('resolving top_entities')
                        top_entities = [Annotation.objects(ann_id=ann_id, pageid=annotation.pageid).first() for ann_id in top_entities_ids]
                        #top_entities = list([db_conn.annotations.find_one({"ann_id":ann_id,"pageid":annotation["pageid"]}) for ann_id in top_entities_ids])
                        logger.debug("Top entities before sorting %s"%[ann.id for ann in top_entities])
                        annotation["top_entities"] = sort_annotations_by_offset(top_entities)
                        logger.debug("Top entities after sorting %s"%[ann.id for ann in top_entities])
                        annotation["top_entities"] = top_entities
                        annotation.save()
                        logger.debug("Updating meta-annotation: %s"%annotation.id)
                    """
                    Transform contains relations between entities into references between annotations 
                    in the MongoDB.
                    """
                    for annotation in sort_annotations_by_offset(containers):
                        if(len(annotation["positions"]) > 1):
                            start = annotation["positions"][0]["start"]
                            end = annotation["positions"][len(annotation["positions"])-1]["end"]
                        else:
                            start = annotation["positions"][0]["start"]
                            end = annotation["positions"][0]["end"]
                        annotation["contains"] = []
                        for contained_annotation in sort_annotations_by_offset(contained):
                            if(len(contained_annotation["positions"])>1):
                                if(contained_annotation["positions"][0]["start"] >= start
                                    and contained_annotation["positions"][len(contained_annotation["positions"])-1]["end"] <= end):
                                    annotation["contains"].append(contained_annotation)
                                    logger.debug("[%s] Annotation %s (%s) contains %s (%s)"%(
                                                                    doc_id
                                                                    ,annotation["ann_id"]
                                                                    ,annotation["id"]
                                                                    ,contained_annotation["ann_id"]
                                                                    ,contained_annotation["id"]))
                                    annotation.save()
                            else:
                                if(contained_annotation["positions"][0]["start"] >= start
                                    and contained_annotation["positions"][0]["end"] <= end):
                                    annotation["contains"].append(contained_annotation)
                                    logger.debug("[%s] Annotation %s (%s) contains %s (%s)"%(
                                                                    doc_id
                                                                    ,annotation["ann_id"]
                                                                    ,annotation["id"]
                                                                    ,contained_annotation["ann_id"]
                                                                    ,contained_annotation["id"]))
                                    annotation.save()
                else:
                    page.is_annotated = False
                    logger.info("%s was ignored because it's not in the golden set"%doc_id)
            except StopIteration as e:
                logger.error("The annotations for %s-%s p. %i  can't be ingested"%(bid, issue_number, page_n))
    except Exception as e:
        logger.error("The annotations for %s-%s  can't be ingested. Got error %s"%(bid, issue_number, e))
    return annotated_pages

def is_annotated(page_id, line_n, token, annotations):
        """
        Check whether the input token has attached an annotation.

        Args:
        - token: dictionary
        - annotations: list of dictionaries

        Returns True or False
        """
        positions = [p for a in annotations for p in a["positions"]]
        for position in positions:
            if(position["page_id"]==page_id):
                if(position["line_n"]==line_n):
                    if(token["offset_start"] >= position["start"]):
                        if(token["offset_end"] <= position["end"]):
                            return True
        return False

def tag_conjunction_entities(annotated_pages):
    """
    find entities to be tagged as conjunction (u'entity_type' == u'meta-annotation')
    create annotations and add them to MongoDB
    add the annotation_id to the list of top_entities
    """
    for page_id in annotated_pages:
        page = Page.objects(id=page_id).first()
        #page = db_conn.pages.find_one({"_id":page_id}) # TODO: refactor
        annotation_ids = [p.id for p in page["annotations_ids"]]
        all_annotations = list(Annotation.objects(id__in=annotation_ids))
        # retrieve meta-annotations from that page
        meta_annotations = list(Annotation.objects(id__in=annotation_ids, entity_type="meta-annotation"))
        #all_annotations = list(db_conn.annotations.find({"_id":{"$in":annotation_ids}})) # TODO: refactor
        #meta_annotations = list(db_conn.annotations.find({"_id":{"$in":annotation_ids} # TODO: refactor
        #                            ,"entity_type":"meta-annotation"}))
        if(len(meta_annotations)>0):
            logger.debug("Meta-annotations: %s"%meta_annotations)
            for meta_annotation in meta_annotations:
                logger.info("Processing meta-annotation %s"%meta_annotation["id"])
                line_span = sorted(list(set([(position["page_id"], position["line_n"]) 
                                        for position in meta_annotation["positions"]])))
                top_entities_ids = [ann.id for ann in meta_annotation["top_entities"]]
                top_entities = list(Annotation.objects(id__in=top_entities_ids))
                #top_entities = [db_conn.annotations.find_one({"_id":top_annotation_id}) 
                #                        for top_annotation_id in meta_annotation["top_entities"]]
                tokens = []
                for page_obj, line_n in line_span:
                    page = Page.objects(id=page_obj.id).first()
                    #page = db_conn.pages.find_one({"_id":page_id})
                    for line in page["lines"]:
                        if line["line_number"]==line_n:
                            tokens.append((page_obj,line_n,line["tokens"]))
                try:
                    for entity in top_entities:
                            assert entity is not None
                    true_conjunctions = []
                    meta_annotation_start = (top_entities[0]["positions"][0]["page_id"]
                                            ,top_entities[0]["positions"][0]["line_n"]
                                            ,top_entities[0]["positions"][0]["start"])
                    meta_annotation_end = (top_entities[-1]["positions"][-1]["page_id"]
                                            ,top_entities[-1]["positions"][-1]["line_n"]
                                            ,top_entities[-1]["positions"][-1]["end"])
                    conjunctions = [(token,page,line) for page,line,toks in tokens for token in toks
                                if(token["offset_start"] >= meta_annotation_start[2] and token["offset_end"] <= meta_annotation_end[2])]
                    true_conjunctions += [(page,line,token) for token,page,line in conjunctions 
                                                if not is_annotated(page,line,token,all_annotations)]
                    if(len(true_conjunctions)>0):
                        logger.debug("Conjunctions found: %s"%true_conjunctions)
                        conjunction_annotations = []
                        all_ann_ids = [annotation["ann_id"] for annotation in all_annotations 
                                                                        if '+' not in annotation["ann_id"] ]
                        identifier_counter = int(sorted(all_ann_ids, key=lambda x: int(x.replace('T','')))[-1].replace("T",""))
                        logger.debug(sorted(all_ann_ids, key=lambda x: int(x.replace('T','')))[-1])
                        for page_obj, line_n, token in true_conjunctions:
                            identifier_counter += 1
                            conjunction_annotation = Annotation(entity_type="conjunction"
                                                    , ingestion_timestamp=datetime.utcnow()
                                                    , annotation_ingester_version=__version__
                                                    , pageid=meta_annotation.pageid
                                                    , filename=meta_annotation.filename
                                                    , bid=meta_annotation.bid)
                            conjunction_annotation.surface = token["surface"]
                            conjunction_annotation.ann_id = "T%i"%identifier_counter
                            conjunction_annotation.positions.append(PagePosition(page_id = page_obj
                                                                                , start = token["offset_start"]
                                                                                , end = token["offset_end"]
                                                                                , line_n = line_n))
                            conjunction_annotation.save()
                            conjunction_annotations.append(conjunction_annotation)
                        logger.info("(Page: %s) %i conjunction annotations were created and stored in MongoDB"%(page_obj.id
                                                                                                , len(conjunction_annotations)))
                        logger.debug("N %i of top entities before adding conjunction entities"%len(meta_annotation["top_entities"]))
                        meta_annotation["top_entities"] += conjunction_annotations
                        logger.debug("N %i of top entities after adding conjunction entities"%len(meta_annotation["top_entities"]))
                        Annotation.objects(id=meta_annotation.id).update_one(set__top_entities = meta_annotation["top_entities"])
                        for conj_annotation in conjunction_annotations:
                            for position in conj_annotation["positions"]:
                                page = Page.objects(id=position.page_id.id).first()
                                page["annotations_ids"].append(conj_annotation)
                                page.save()
                except AssertionError as e:
                    #raise e
                    logger.warning("The meta-annotation %s has no top-level entities and generated the following error: %s"%(meta_annotation["_id"],e))
                except Exception as e:
                    raise e

def main():
    desc="""

    A script to ingest annotations produced within Brat for the LinkedBooks project into a MongoDB (version %s).

    """%__version__
    parser = argparse.ArgumentParser(description=desc)
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument("--data-dir", help="The directory with the data to ingest",type=str,required=True)
    requiredNamed.add_argument("--config-file", help="The configuration file (with parameters for connecting to MongoDB)",type=str,required=True)
    requiredNamed.add_argument("--log-file", help="Destination file for the logging",type=str,required=False)
    requiredNamed.add_argument("--log-level", help="Log level",type=str,required=False,default="INFO")
    requiredNamed.add_argument("--clear-db", help="Remove all existing_annotations before ingesting new ones",action="store_true",required=False, default=False)
    requiredNamed.add_argument("--filter-file", help="The filter file",type=str,required=True)
    requiredNamed.add_argument("--annotation-type",
                                 help="Indicates whether the annotations were done manually or automatically"
                                 ,choices=["automatic","manual"],type=str,required=True)
    args = parser.parse_args()
    #
    # Parse the configuration file
    #
    config = ConfigParser(allow_no_value=True)
    config.read(args.config_file)
    mongo_host = config.get('mongo','db-host')
    mongo_db = config.get('mongo','db-name')
    mongo_port = config.getint('mongo','db-port')
    mongo_user = config.get('mongo','username')
    mongo_pwd = config.get('mongo','password')
    mongo_auth = config.get('mongo','auth-db')
    # 
    # Initialise the logger
    #
    logger = logging.getLogger()
    if(args.log_file is not None):
        handler = logging.FileHandler(filename=args.log_file, mode='w')
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    numeric_level = getattr(logging, args.log_level.upper(), None)
    logger.setLevel(numeric_level)
    #
    # Initialise the DB connection for the mongoengine models
    #
    db = connect(mongo_db
            , username=mongo_user
            , password=mongo_pwd
            , authentication_source=mongo_auth
            , host=mongo_host
            , port=int(mongo_port)
            )
    if args.clear_db:
        existing_annotations = Annotation.objects
        logger.info("The DB contains %i annotations: let's delete them..." % existing_annotations.count())
        [annotation.delete() for annotation in existing_annotations]
        logger.info("Now the DB contains %i annotations" % Annotation.objects.count())

    annotations = {}
    root = args.data_dir
    logger.info("Processing directory \"%s\""%root)
    logger.info("Looking for documents to ingest...")
    to_ingest = [dirpath for dirpath, dirnames, filenames in os.walk(root) if(len(dirnames)==0)]
    tsv_file = args.filter_file
    if(tsv_file != ""):
        df = pd.read_csv(tsv_file, sep='\t')
        df = df.query("tenere != 1.0")[["bid","issue"]]
        to_skip = ["%s-%s"%(row[1]['bid'],row[1]["issue"]) for row in df.iterrows()]
    else:
        to_skip = []
    logger.debug("The following documents wil be ignored: %s"%" ".join(to_skip))
    to_ingest = [item for item in to_ingest if "%s-%s"%(item.split('/')[-2],item.split('/')[-1]) not in to_skip]
    logger.info("%i documents will be ignored (info read from %s)"%(len(to_skip),tsv_file))
    logger.info("done: found %i documents to ingest (%s)"%(len(to_ingest), to_ingest))
    logger.info("Starting to ingest documents...")
    annotated_pages = {directory:ingest_annotations(directory,args.annotation_type) for directory in sorted(to_ingest)}
    logger.info("done: ingested %i documents"%len(annotated_pages))
    logger.info("Starting to tag conjunction entities...")
    [tag_conjunction_entities(annotated_pages[item]) for item in annotated_pages.keys()] 
    logger.info("done.")
    logger.info("Bye bye!")
if __name__ == "__main__":
    main()
