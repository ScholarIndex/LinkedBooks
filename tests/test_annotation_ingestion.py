import pytest
from pytest import mark
from random import shuffle
import logging
import os
import pdb
from annotation_connector.ingester import *
from commons.dbmodels import *

logger = logging.getLogger(__name__)

@mark.run(order=9)
def test_add_pages_to_golden_set(mongoengine_connection, n=600):
    # add some pages to the golden set for the sake of the test
    for page in Page.objects:
        page.in_golden = True
        page.save()
    logger.info("%i pages added to golden set"%n)

@mark.run(order=10)
def test_annotation_ingestion_journals(mongoengine_connection
                                        , data_dir="tests/test_data/annotations/journals/"
                                        , filter_file="tests/test_data/loggerPERIODICI.ods_ArchivioVeneto.tsv"):
    """
    Test ingestion of brat annotations from journals.
    """
    annotations = {}
    annotation_type = "manual"
    root = data_dir
    logger.info("Processing directory \"%s\""%root)
    logger.info("Looking for documents to ingest...")
    to_ingest = [dirpath for dirpath, dirnames, filenames in os.walk(root) if(len(dirnames)==0)]
    tsv_file = filter_file
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
    annotated_pages = {directory:ingest_annotations(directory, annotation_type) for directory in sorted(to_ingest)}
    logger.info("done: ingested %i documents"%len(annotated_pages))
    logger.info("Starting to tag conjunction entities...")
    [tag_conjunction_entities(annotated_pages[item]) for item in annotated_pages.keys()] 
    logger.info("done.")
    logger.info("Bye bye!")

def test_annotation_ingestion_monographs(mongoengine_connection
                                        , data_dir="tests/test_data/annotations/books/"
                                        , filter_file="tests/test_data/loggerPERIODICI.ods_ArchivioVeneto.tsv"):
    """
    Test ingestion of brat annotations from monographs.
    """
    annotations = {}
    annotation_type = "manual"
    root = data_dir
    logger.info("Processing directory \"%s\""%root)
    logger.info("Looking for documents to ingest...")
    to_ingest = [dirpath for dirpath, dirnames, filenames in os.walk(root) if(len(dirnames)==0)]
    tsv_file = filter_file
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
    annotated_pages = {directory:ingest_annotations(directory, annotation_type) for directory in sorted(to_ingest)}
    logger.info("done: ingested %i documents"%len(annotated_pages))
    logger.info("Starting to tag conjunction entities...")
    [tag_conjunction_entities(annotated_pages[item]) for item in annotated_pages.keys()] 
    logger.info("done.")
    logger.info("Bye bye!")