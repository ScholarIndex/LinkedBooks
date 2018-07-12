#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
The tests for the `commons.dbmodels`.
"""

__author__ = """Giovanni Colavizza, Matteo Romanello"""

import json
import logging
from datetime import datetime
from pytest import mark
import pkg_resources
from commons.dbmodels import *
from mongoengine import connect
from mongoengine.errors import NotUniqueError
from metadata_importer.metadata_importer import parser, import_metadata, create_meta_objects
from metadata_importer.bibliodb_importer import import_metadata as import_metadata_bibliodb

logger = logging.getLogger(__name__)

@mark.run(order=1)
def test_create_meta_objects():
    record = {'title': {'surface': 'Vita di Marco Polo veneziano', 'responsible': 'Alvise Zorzi.', 'specifications': '3. ed.', 'publisher': 'Milano : Rusconi, 1982.', 'materiality': '420 p. ; 21'}, 'creator': 'Zorzi, Alvise  <1922-    >', 'subjects': ['Polo, Marco - Biografia', 'Storia - Venezia'], 'issues': [], 'relations': [{'title': {'surface': 'La storia.', 'responsible': '', 'specifications': '', 'publisher': 'Milano : Rusconi.', 'materiality': ''}, 'type': 'FA PARTE DI'}], 'sbn_id': 'DUC-VEA-2283429', 'bid': 'ANA0002861', 'provenance': 'BAUM', 'date': '1982', 'type_catalogue': 'Testo a stampa (moderno)', 'type_document': 'monograph', 'foldername': 'ANA0002861', 'language': 'ITA', 'operator': 'Martina Babetto', 'digitisation_note': 'major problems', 'img_bib': [], 'marked_as_removed': False}
    create_meta_objects(record)
    logger.info("Created meta object")

@mark.run(order=2)
def test_metadata_importer(config_parser, clear_test_db):
    mongo_db = config_parser.get('mongo','db-name')
    mongo_user = config_parser.get('mongo','username')
    mongo_pwd = config_parser.get('mongo','password')
    mongo_auth = config_parser.get('mongo','auth-db')
    mongo_host = config_parser.get('mongo', 'db-host')
    mongo_port = config_parser.get('mongo', 'db-port')
    # let `mongoengine` establish a connection to the MongoDB via `mongoengine.connect()` 
    logger.debug(connect(mongo_db
        ,username=mongo_user
        ,password=mongo_pwd
        ,authentication_source=mongo_auth
        ,host=mongo_host
        ,port=int(mongo_port)))
    # reset collections
    Metadata.drop_collection()
    Processing.drop_collection()
    logger.info("Dropped collections")
    # load data from first test folder
    test_folder = pkg_resources.resource_filename(__name__,"/test_data/xml_output_first")
    logger.info("Processing folder: %s"%test_folder)
    records = parser(test_folder)
    logger.info("Loaded %d records"%len(records))
    # assuming that the folder should contain some xml files to ingest
    # therefore, it fails if none is found
    assert len(records)>0
    # Merge
    # create a dict of merged records to ingest/update
    records_dict = dict()
    for r in records:
        if r.id2 not in records_dict.keys():
            records_dict[r.id2] = r
        else:
            if r.type2 == "journal":
                r.provenance = ""
                for i in r.issues:
                    if i.foldername not in [x.foldername for x in records_dict[r.id2].issues]:
                        records_dict[r.id2].issues.append(i)
                    else:
                        logger.warning("Issue present twice %s, %s" % (r.id2, i.foldername))
            else:
                logger.warning("Book present twice %s" % (r.id2))
    logger.info("Created merged records")
    r_to_insert = [x.export_json() for x in records_dict.values()]
    logger.info("%d records to insert"%len(r_to_insert))
    time_now = datetime.now()
    for r in r_to_insert:
        r["updated_at"] = time_now
        r["created_at"] = time_now
        new_entry = create_meta_objects(r)
        new_entry.save()
    logger.info("Inserted records")
    # create preprocessing objects
    for k,v in records_dict.items():
        if v.type2 == "monograph":
            new_entry = Processing(bid=k,number="",type_document=v.type2,foldername="books/"+v.provenance+"_"+k,is_digitized=True,is_ingested_metadata=True, updated_at=time_now, created_at=time_now)
            new_entry.save()
        else:
            for i in v.issues:
                new_entry = Processing(bid=k, number=i.foldername, foldername="journals/" + v.provenance + "_" + k + "/" + i.foldername,
                           type_document='issue', is_digitized=True, is_ingested_metadata=True, updated_at=time_now, created_at=time_now)
                new_entry.save()
    logger.info("Inserted processing records")
    # create indexes
    Metadata.create_index("bid", background=True)
    Processing.create_index(["bid","number"], background=True)

    # load data from second test folder and add them up
    test_folder = pkg_resources.resource_filename(__name__,"/test_data/xml_output_second")
    logger.info("Processing folder: %s"%test_folder)
    # assuming that the folder should contain some xml files to ingest
    # therefore, it fails if none is found
    assert len(records)>0
    logger.info("Loaded records")
    logger.debug(pkg_resources.resource_filename(__name__,"tests.conf"))
    import_metadata(records, config_file_name=pkg_resources.resource_filename(__name__,"/tests.conf"),
                    db="mongo")

#@mark.skip(reason="Fails when ES not available")
@mark.run(order=9)
def test_metadata_bibliodb_ingestion(mongoengine_connection, article_file="tests/test_data/articles.json"):
    for processing_record in Processing.objects(is_bibliodbed=True):
        Processing.objects(id=processing_record.id).update_one(set__is_bibliodbed=False)

    for processing_record in Processing.objects(is_ingested_ocr=True):
        processing_record.is_ocr = True
        processing_record.is_img = True
        processing_record.save()
                
    import_metadata_bibliodb(article_file)

@mark.run(order=3)
def test_import_authors(mongoengine_connection, data_dir="tests/test_data/"):
    """
    Before executing the tests below, the legacy author records
    need to be imported into the DB. They come from the LBCatalogue 
    and were partly cleaned manually by Silvia and Giovanni.
    """
    legacy_authors = None
    duplicates_found = 0

    with open("%sauthors.json"%data_dir, "r") as input_file:
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
    
    assert (len(Author.objects) + duplicates_found) == len(legacy_authors)
