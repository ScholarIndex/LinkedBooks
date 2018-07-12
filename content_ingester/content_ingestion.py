#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""

The `ContentIngester` takes care of ingesting the digitised materials for the LinkedBooks project into a MongoDB
(run `python content_ingestion.py --help` for the script usage).

Command example (journals):

    python content_ingester/content_ingestion.py  --log-level=DEBUG --db-host localhost --db-name=linkedbooks_refactored \
    --username=scripty --password=L1nk3dB00ks  --data-dir ./tests/test_data/content/journals/ --documents-type journals --auth-db=admin

Command example (monographs):

    python content_ingester/content_ingestion.py  --log-level=DEBUG --db-host localhost --db-name=linkedbooks_refactored \
    --username=scripty --password=L1nk3dB00ks  --data-dir ./tests/test_data/content/books/ --documents-type monographs --auth-db=admin

NB: Before running this script, make sure you remove the duplicate files identified by `find_ocr_duplicate_files.py`

"""
__author__ = """Giovanni Colavizza, Matteo Romanello"""

import pdb
import os
import stat
import json
import sys
import glob
import shutil
from shutil import Error
import logging
sys.path += ["../", "./"]
from commons.dbmodels import *
from mongoengine.errors import NotUniqueError
from mongoengine import connect
#import multiprocessing as mp
from datetime import datetime
from content_ingester.version import __version__
from commons.functions import split_and_tokenise_standoff

logger = logging.getLogger(__name__)


def move_issue_folder(issue, destination_folder):
    """
    :param issue: a dictionary with keys: `bid`, `number` and `path`
    """
    new_bid_path = "%s%s" % (destination_folder, issue["bid"])
    os.umask(0000)
    if os.path.isdir(new_bid_path):
        pass
    else:
        os.mkdir(new_bid_path)
    try:
        shutil.move(issue["path"], new_bid_path)
    except Error as e:
        print(e)
    return


def convert_issue_number(number):
    parts = number.split("_")
    if(len(parts) == 3):
        return "%s_%s" % (parts[0], ".".join(parts[1:]))
    else:
        return number


def page_number2image_name(number, string="image", padding_size=4):
    """
    Utility function to format a number with a padding of size n.

    :param number: the number to format (int)
    :param string: the prefix to prepend (str)
    :param padding_size: the desired lenght of the resulting string (int)
    :return: a string, whose length == padding_size and in which the number
            of zeroes == padding_size - len(number)

    Usage example:

    >>> page_number2image_name(24)
    'image-0024'

    >>> page_number2image_name(24,padding_size=5)
    'image-00024'

    """
    return "{}-{}".format(string, str(number).zfill(padding_size))


def process_journalissue_folder(issue_info, extension=".htm"):
    """
    Process a folder containing a journal issue, HTML files one for each page
    image. The text is extracted from the HTML, tokenised etc.

    :param issue_info: the dictionary returned by calling `detect_issues()`
    :param extension: the extension of files to ingest
    :return: an `commons.dbmodels.LBDocument` instance or None

    """
    logger.info('Processing issue %s/%s in folder %s'%(issue_info['bid']
                                                    , issue_info["number"]
                                                    , issue_info['path']))
    issue_doc = LBDocument(internal_id = "%s-%s"%(issue_info['bid'], issue_info["number"])
                                        , issue_number = issue_info["number"]
                                        , bid = issue_info['bid']
                                        , content_ingester_version = __version__
                                        , ingestion_timestamp = datetime.utcnow()
                                        , type = "journal_issue"
                                        , path = issue_info["path"]
                                        , updated_at = datetime.now())
    filenames = [os.path.basename(file) for file in glob.glob("%s*%s"%(issue_info["path"],extension))]
    logger.debug("Scanning directory for pattern %s*%s"%(issue_info["path"], extension))
    filenumbers = [int(fname.replace("image-","").replace("%s"%extension,"")) for fname in filenames]
    logger.debug("Found %i files. Last file number is %i"%(len(filenumbers), max(filenumbers)))
    metadata_record = Metadata.objects(bid = issue_info["bid"]).get()
    issue_doc.metadata_id = metadata_record
    issue_record = None
    #
    # fetch the metadata record for the issue to be ingested
    # fail and exit if it cannot be found
    #
    try:
        issue_record = next(issue_obj
                                for issue_obj in metadata_record['issues']
                                    if issue_obj['foldername']==issue_info["number"] and issue_obj["marked_as_removed"] is False)
        assert issue_record is not None
        issue_doc.provenance = issue_record.provenance
    except Exception as e:
        logger.warning(e)
        logger.warning("The record %s doesn't contain the issue \"%s\""%(issue_info['bid'], issue_info["number"]))
        try:
            new_issue_number = convert_issue_number(issue_info["number"])
            logger.info("Trying now with issue number = %s"%new_issue_number)
            issue_record = next(issue_obj
                                for issue_obj in metadata_record['issues']
                                    if issue_obj['foldername']==new_issue_number and issue_obj["marked_as_removed"] is False)
            assert issue_record is not None
            logger.info("Record found!")
            issue_doc.provenance = issue_record.provenance
        except Exception as e:
            logger.error("Couldn't find a metadata record for issue \"%s-%s\""%(issue_info['bid'], issue_info["number"]))
            return None
    try:
        if(issue_record["imgpagestart"]["img_number"] is None and issue_record["imgpagestart"]["page_number"] is None):
            logger.warning("The `imgpagestart` field of metadata is empty: assuming default value (1)")
            issue_record["imgpagestart"]["page_number"] = 1
            issue_record["imgpagestart"]["img_number"] = 1
        assert issue_record["imgpagestart"]["img_number"]!= -1 and issue_record["imgpagestart"]["page_number"]!= -1
        #
        # reconstruct the pagination
        #
        pages = generate_page_objects(issue_record["imgpagestart"],max(filenumbers)+issue_record["imgpagestart"]["page_number"])
        # TODO: add assertion
        for page in pages:
            # derive the filenames from the image file numbers
            fname = "%s%s%s"%(issue_info["path"]
                            , page_number2image_name(pages[page]["single_page_file_number"])
                            , extension)
            try:
                pages[page]["filename"] = fname
                # tokenise the html, line by line, and add it to the page
                pages[page]["lines"], pages[page]["fulltext"] = tokenise_html(fname)
                # add the page to the LBDodument
                issue_doc.pages.append(pages[page])
            except FileNotFoundError as e:
                logger.debug("File \'%s\' does not exist"%fname)
        assert len(issue_doc.pages) == len(filenumbers)
        #
        # Add the index (ToC) of the journal issue
        # check existence and load the full text
        #
        #try:
        # check that the `imgindex` is not empty
        #assert issue_record["imgindex"]!=""
        index = {}
        if issue_record["imgindex"]!="":
            index_scans = [int(scan_n) for scan_n in issue_record["imgindex"].split("-")]
            index_pages = [pages[page] for page in pages
                                if(pages[page]["dbl_side_scan_number"] in index_scans)]
            index["filenumbers"] = [page["single_page_file_number"]
                                 for page in index_pages]
            logger.info("Successfully fetched index information. The index is at pages: %s"%
                    ", ".join([str(page["single_page_file_number"]) for page in index_pages]))
        else:
            logger.info("No index information was provided for this document.")
        issue_doc["index"] = index
        logger.info("The document %s has %i pages." % (issue_doc.internal_id, len(issue_doc.pages)))
        return issue_doc
    except AssertionError as e:
        logger.error(str(e))
        logger.error("There is a problem with the `imgpagestart` field of metadata. Ingestion aborted.")
        return None


def process_monograph_folder(monograph_info, extension=".htm"):
    """
    Description.

    :param monograph_info:
    :param annotation_metadata:
    :param extension:
    :return:
    """
    logger.info('Processing monograph %s in folder %s'%(monograph_info['bid']
                                                    , monograph_info['path']))
    monograph_doc = LBDocument(internal_id = "book_%s"%(monograph_info['bid'])
                                        , bid = monograph_info['bid']
                                        , content_ingester_version = __version__
                                        , ingestion_timestamp = datetime.utcnow()
                                        , type = "monograph"
                                        , path = monograph_info["path"]
                                        , updated_at = datetime.now())
    filenames = [os.path.basename(file) for file in glob.glob("%s*%s"%(monograph_info["path"], extension))]
    logger.debug("Scanning directory for pattern %s*%s"%(monograph_info["path"], extension))
    filenumbers = [int(fname.replace("image-","").replace("%s"%extension,"")) for fname in filenames]
    logger.debug("Found %i files. Last file number is %i"%(len(filenumbers), max(filenumbers)))
    metadata_record = Metadata.objects(bid = monograph_info["bid"]).get()
    monograph_doc.metadata_id = metadata_record
    monograph_doc.provenance = metadata_record.provenance
    logger.debug(repr(monograph_doc))
    imgpagestart = {"img_number":1, "page_number":1}
    pages = generate_page_objects(imgpagestart, max(filenumbers)+imgpagestart["page_number"])
    for page in pages:
            # derive the filenames from the image file numbers
            fname = "%s%s%s"%(monograph_info["path"]
                            , page_number2image_name(pages[page]["single_page_file_number"]).replace("image-", "")
                            , extension)
            try:
                pages[page]["filename"] = fname
                # tokenise the html, line by line, and add it to the page
                pages[page]["lines"], pages[page]["fulltext"] = tokenise_html(fname)
                # add the page to the LBDodument
                monograph_doc.pages.append(pages[page])
            except FileNotFoundError as e:
                logger.debug("File \'%s\' does not exist"%fname)
            except Exception as e:
                logger.error("Ingestion of {} failed with error {}".format(
                    monograph_doc.bid,
                    e
                ))
                return monograph_doc
    return monograph_doc


def save_journalissue_to_db(issue_object, force_update = False):
    """
    Persist a new document (i.e. journal issue) into the MongoDB, taking care that no duplicates are inserted
    and that document pages that are part of the golden set are always preserved.

    :param issue_object:
    :param force_update:
    :return: `True` upon success, `False` upon failure and `None` if document
            is skipped (None or bool).

    """
    # TODO check that this query works properly
    try:
        processing_info = Processing.objects(type_document = "issue"
                                            , bid=issue_object.bid
                                            , number = issue_object["issue_number"]).get()
        logger.info("Command running with --force-update=%s"%str(force_update))
    except Exception as e:
        new_issue_number = convert_issue_number(issue_object['issue_number'])
        try:
            processing_info = Processing.objects(type_document = "issue", bid=issue_object['bid'], number=new_issue_number).get()
        except Exception as e:
            logger.info("Ingestion failed. Could not find a processing record for %s"%(repr(issue_object)))
            return False
    if not processing_info.is_ingested_ocr:
        try:
            # persist the pages to MongoDB
            saved_pages = [page.save() for page in issue_object.pages]
            # create an index and populate it with PageRefs
            index = Index()
            if "filenumbers" in issue_object.index:
                for page_n in issue_object["index"]["filenumbers"]:
                    page = next((page for page in issue_object.pages if page.single_page_file_number == page_n), None)
                    index.page_ids.append(PageRef(page_id = page))
            issue_object["index"] = index
            issue_object.save()
            issue_object.reload()
            logger.info("%s was saved into the MongoDB"%repr(issue_object))
            processing_info.is_ingested_ocr = True
            processing_info.updated_at = datetime.utcnow()
            processing_info.save()
            return True
        except NotUniqueError as e:
            logger.warning("The document is already contained in the DB (%s)"%repr(issue_object))
            return False
    elif processing_info.is_ingested_ocr:
        if force_update:
            existing_object = LBDocument.objects(internal_id = issue_object["internal_id"]).get()
            logger.info("Updating %s (%s)" % (existing_object.internal_id, existing_object.id))
            number_of_pages = len(existing_object.pages)
            pages_in_golden = [page for page in existing_object.pages if "in_golden" in page and page.in_golden]
            [page.delete() for page in existing_object.pages if not page.in_golden]
            pages_in_golden_numbers = [page["single_page_file_number"] for page in existing_object.pages if page.in_golden]
            new_pages = []
            for page in issue_object.pages:
                if page["single_page_file_number"] not in pages_in_golden_numbers:
                    page.save()
                    new_pages.append(page)
                    logger.debug("Page %s saved to the MongoDB and added to document %s"%(page.id, existing_object.id))
                else:
                    logger.debug("Skipping page %s as it's in the golden set" % page.single_page_file_number)
            LBDocument.objects(id=existing_object.id).update_one(set__pages = new_pages+pages_in_golden)
            LBDocument.objects(id=existing_object.id).update_one(set__updated_at = datetime.utcnow())
            LBDocument.objects(id=existing_object.id).update_one(set__content_ingester_version = __version__)
            logger.info("New pages ingested: %i"%len(new_pages))
            logger.info("Pages from golden set not updated: %i"%len(pages_in_golden))
            existing_object.reload()
            logger.info("Document %s (%s) was updated in the DB"%(existing_object["internal_id"], existing_object.id))
            #assert number_of_pages == len(existing_object.pages)
            return True
        else:
            logger.warning("""[%s-%s]Document not ingested as it's already in the DB.
                        To overwrite run the command with flag --force-update"""%(issue_object.bid, issue_object.issue_number))
            return None

def save_monograph_to_db(monograph_object, force_update = False):
    """
    Persiste a new document (i.e. monograph) into the MongoDB, taking care that no duplicates are inserted
    and that document pages that are part of the golden set are always preserved.

    :param monograph_object:
    :param force_update:
    :return: `True` upon success, `False` upon failure and `None` if document
            is skipped (None or bool).
    """
    processing_info = Processing.objects(type_document = "monograph", bid = monograph_object["bid"]).get()
    logger.info("Command running with --force-update=%s"%str(force_update))
    if not processing_info.is_ingested_ocr:
        try:
            # persist the pages to MongoDB
            saved_pages = [page.save() for page in monograph_object.pages]
            monograph_object.save()
            monograph_object.reload()
            logger.info("%s was saved into the MongoDB"%repr(monograph_object))
            processing_info.is_ingested_ocr = True
            processing_info.updated_at = datetime.utcnow()
            processing_info.save()
            return True
        except NotUniqueError as e:
            logger.warning("The document is already contained in the DB (%s)"%repr(monograph_object))
            return False
    elif processing_info.is_ingested_ocr:
        if force_update:
            existing_object = LBDocument.objects(type="monograph", bid=monograph_object["bid"]).get()
            number_of_pages = len(existing_object.pages)
            pages_in_golden = [page for page in existing_object.pages if "in_golden" in page and page.in_golden]
            [page.delete() for page in existing_object.pages if not page.in_golden]
            pages_in_golden_numbers = [page["single_page_file_number"] for page in existing_object.pages if page.in_golden]
            new_pages = []
            for page in monograph_object.pages:
                if page["single_page_file_number"] not in pages_in_golden_numbers:
                    page.save()
                    new_pages.append(page)
                    logger.debug("Page %s saved to the MongoDB and added to document %s"%(page.id, existing_object.id))
            LBDocument.objects(id=existing_object.id).update_one(set__pages = new_pages+pages_in_golden)
            LBDocument.objects(id=existing_object.id).update_one(set__updated_at = datetime.utcnow())
            LBDocument.objects(id=existing_object.id).update_one(set__content_ingester_version = __version__)

            if not "internal_id" in existing_object:
                LBDocument.objects(id=existing_object.id).update_one(set__internal_id = "book_%s" % monograph_object["bid"])

            logger.info("New pages ingested: %i"%len(new_pages))
            logger.info("Pages from golden set not updated: %i"%len(pages_in_golden))
            existing_object.reload()
            logger.info("Document %s (%s) was updated in the DB"%(existing_object["internal_id"], existing_object.id))
            return True
        else:
            logger.warning("Document not ingested as it's already in the DB. To overwrite run the command with flag --force-update")
            return None

def tokenise_html(input_file):
    """
    Tokenise the HTML output of Abbyy Fine Reader while retaining divisions into lines
    and for each token of text some formatting features (e.g. font size, etc.).

    Returns:
        a tuple; tuple[0] is a list of <Line> objects; tuple[] the fulltext

    Example:


    """
    import bs4 # pip install beautifulsoup4
    text = ""
    html = bs4.BeautifulSoup(open(input_file,'r'), "lxml")
    lines = []
    line_count = 1
    #
    # Rationale: each line of the OCRed text is contained within a <p> or <a>
    # These top level elements correspond *almost* always to separate lines of text
    #
    top_elements = [element for element in html.body.children if element is not None]
    for m,element in enumerate(top_elements):# TODO: this is not true anymore, sometimes is an <a> ...
        line = Line(line_number = line_count)
        line_text = ""
        token_count = 1
        if(element.name is not None):
            if(len(element.find_all('font'))>0):
                for font in element.find_all('font'):
                    modified_html = str(font).replace("<sup>","").replace("</sup>","")
                    font = bs4.BeautifulSoup(modified_html,"html.parser").font
                    features = [{"feature":property.split(':')[0],"value":property.split(':')[1]}
                                                    for property in font["style"].split(';') if property!=""]
                    for n,content in enumerate(font.contents):
                        if(content.name==None):
                            line_text+=content.replace("¬","-").replace(' \xa0\xa0\xa0',"")
                            for i,token in enumerate(line_text.split(" ")):
                                if(token!=""):
                                    line["tokens"].append(Token(
                                        token_number = token_count
                                        , surface = token
                                        , offset_start = len(text)
                                        , offset_end = len(text)+len(token)
                                        , features = features
                                        ))
                                    if(i!=len(line_text.split(" "))-1): # not the last token, add space
                                        text+="%s "%token
                                    else:                               # last token, don't add a space after the token
                                        text+="%s"%token
                                    token_count+=1
                        elif(content.name=="a"):
                            if(content.string is not None):
                                token = content.string
                                line["tokens"].append(Token(
                                        token_number = token_count
                                        , surface = token
                                        , offset_start = len(text)
                                        , offset_end = len(text)+len(token)
                                        , features = features
                                        ))
                                text+="%s"%token
                        elif(content.name=="br"):
                            text+="\r\n" # replace a <br> with a line break
                            lines.append(line)
                            line_count+=1
                            line = Line(line_number = line_count)
                            token_count = 1
                            line_text = ""
                        line_text = ""
                if(m != len(top_elements)-1):
                    text+="\r\n" # add a line break only if it's not the last <p> (so as to avoid trailing '\n')
                    lines.append(line)
                    line_count+=1
                else:
                    lines.append(line)
            else:
                if(element.string is not None):
                    token = element.string
                    line["tokens"].append(Token(
                                        token_number = token_count
                                        , surface = token
                                        , offset_start = len(text)
                                        , offset_end = len(text)+len(token)
                                        , features = features
                                        ))
                    text+="%s"%token
    return lines,text
def generate_page_objects(imgpagestart,last_page_number=500):
    """

    TODO: change attributes of imgpagestart that are accessed

    desc: ?

    imgpagestart : value taken from the MongoDB

    returns: ?

    """
    scan_to_pages = {} # an index for mapping scan numbers onto page numbers
    pages_to_scan = {} # an index for mapping page numbers onto scan numbers
    scan_n = imgpagestart["img_number"] # the n. of the scan where the pagination beings (scans are double sided)
    start_page_n = imgpagestart["page_number"] # the first page number
    logger.info("The pagination starts at scan n. %i with page number %i"%(scan_n,start_page_n))
    #
    # Step 1:
    # calculate the n. of scans and single pages before the beginning of pagination
    #
    scans_before_numbering = list(range(0,scan_n)) # the n. of scans before the beginning of pagination
    scans_before_numbering.sort(reverse=True) # sort so that we can proceed backwards
    logger.debug("Calculating pages **before** the beginning of pagination")
    for scan in scans_before_numbering:
        scan+=1
        scan_to_pages[scan] = (start_page_n -1, start_page_n) # each scan contains two pages
        logger.debug("Scan %i contains pages %s"%
                     (scan," and ".join(str(n) for n in scan_to_pages[scan])))
        start_page_n -= 2 # 2 since each scan contains two pages, decrease the counter of two
    #
    # Step 2:
    # calculate the n. of physical pages after the beginning of pagination
    # same as above, but proceeding onwards
    #
    start_page_n = imgpagestart["page_number"]+2
    logger.debug("Calculating pages **after** the beginning of pagination")
    for scan in range(imgpagestart["img_number"]+1, last_page_number+1):
        if(start_page_n > last_page_number):
            scan_to_pages[scan] = (start_page_n -1,)
            logger.debug("Scan %i contains pages %s"%
                         (scan," and ".join(str(n) for n in scan_to_pages[scan])))
            break
        elif(start_page_n <= last_page_number+1):
            scan_to_pages[scan] = (start_page_n -1, start_page_n)
            logger.debug("Scan %i contains pages %s"%
                         (scan," and ".join(str(n) for n in scan_to_pages[scan])))
            start_page_n += 2
        else:
            break
    #
    # Step 3:
    # create the inverted index `pages_to_scan` in order
    # to get the correspondence between page numbers and scan numbers
    #
    for scan in scan_to_pages:
        for page in scan_to_pages[scan]:
            pages_to_scan[page] = scan
            logger.debug("Page %i is contained within scan %i"%(page,scan))
    #
    # Step 4:
    # create one record (dict) for each page where to store:
    # 1. the number of the corresp. scan
    # 2. the physical page number
    # 3. the printed page number
    #
    pages = list(pages_to_scan.keys()) # collect all numbers of physical pages
    pages.sort()                 # sort page numbers: negative numbers go first
    page_objects = {n+1:Page(
                        single_page_file_number = n+1
                        , printed_page_number = [page]
                        , dbl_side_scan_number = pages_to_scan[page]
                        )
                    for n,page in enumerate(pages)}
    return page_objects
def detect_issues(basedir):
    """
    Given a containing folder, looks for sub-folders that may
    contain the OCR of a journal issue.

    :param basedir: the directory to search for journal issue sub-directories
    :return: a list of dictionaries, with keys "number", "path" and "bid".
    """
    bids = [{"bid" : result
            , "path" : "%s/%s/"%(basedir, result)}
                            for result in os.listdir(basedir)
                                   if os.path.isdir("%s/%s"%(basedir,result))]
    issues = [{"number" : result
              , "path" : "%s%s/"%(bid["path"],result)
              , "bid" : bid["bid"]}
                             for bid in bids for result in os.listdir(bid["path"])
                                        if os.path.isdir("%s%s"%(bid["path"],result))]
    return issues
def detect_monographs(basedir):
    """
    Given a containing folder, looks for sub-folders that may
    contain the OCR of a monograph.

    :param basedir: the directory to search for monograph sub-directories
    :return: a list of dictionaries, with keys "path" and "bid".
    """
    to_ingest = [dirpath for dirpath, dirnames, filenames in os.walk(basedir) if(len(dirnames)==0)]
    return [{
            "bid" : directory.split('/')[len(directory.split('/'))-1].split('_')[0]
            , "path" : "%s%s/"%(basedir, directory.split('/')[len(directory.split('/'))-1])
            }
            for directory in to_ingest]
def main():
    import argparse
    desc="""

    A script to ingest the digitised materials for the LinkedBooks project into a MongoDB (version %s).

    """%__version__
    parser = argparse.ArgumentParser(description=desc)
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument("--log-file", help="Destination file for the logging",type=str,required=False)
    requiredNamed.add_argument("--log-level", help="Log level",type=str,required=False,default="INFO")
    requiredNamed.add_argument("--db-host", help="Address of the MongoDB",type=str,required=True)
    requiredNamed.add_argument("--limit", help="Limit the number of documents to ingest",type=int, required=False)
    requiredNamed.add_argument("--db-name", help="Name of the database to use",type=str,required=True)
    requiredNamed.add_argument("--data-dir", help="Directory with the data to ingest",type=str,required=True)
    requiredNamed.add_argument("--username", help="MongoDB user",type=str,required=True)
    requiredNamed.add_argument("--password", help="Password for the MongoDB user",type=str,required=True)
    requiredNamed.add_argument("--auth-db", help="MongoDB collection to use for authentication",type=str,required=True)
    requiredNamed.add_argument("--force-update", help="If passed, already ingested documents are overwritten"
                                                , required=False, action="store_true")
    requiredNamed.add_argument("--mongo-port", help="Port for the MongoDB connection",required=False, default=27017, type=int)
    requiredNamed.add_argument("--documents-type", help="Type of documents to ingest", choices=["monographs","journals"],type=str,required=True)
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
    db = connect(args.db_name
            , username=args.username
            , password=args.password
            , authentication_source=args.auth_db
            , host=args.db_host
            , port=int(args.mongo_port)
            )
    logger.info(db)
    #
    # Start with the ingestion
    #
    if(args.documents_type=="journals"):
        detected_issues = detect_issues(args.data_dir)
        duplicated_issues = []
        if(not args.force_update):
            for issue in detected_issues:
                try:
                    Processing.objects(bid=issue['bid'], number=issue['number'], is_ingested_ocr=True).get()
                    duplicated_issues.append(detected_issues.pop(detected_issues.index(issue)))
                except Exception as e:
                    new_issue_number = convert_issue_number(issue['number'])
                    try:
                        Processing.objects(bid=issue['bid'], number=new_issue_number, is_ingested_ocr=True).get()
                        duplicated_issues.append(detected_issues.pop(detected_issues.index(issue)))
                    except Exception as e:
                        pass
        #pdb
        logger.info("%i duplicates were found and will not be ingested"%len(duplicated_issues))
        [logger.info("Found issue to ingest: %s"%issue) for issue in detected_issues]
        issue_objects = [process_journalissue_folder(issue) for issue in detected_issues]
        logger.info("Documents to be saved into the MongoDB: %i" % len([issue for issue in issue_objects if issue is not None]))
        results = [(issue.issue_number, save_journalissue_to_db(issue, force_update = args.force_update))
                                                            for issue in issue_objects if issue is not None]
        docs_succeeded = len([outcome for issue_number, outcome in results if outcome])
        docs_failed = len([outcome for issue_number, outcome in results if outcome is False])
        docs_skipped = len([outcome for issue_number, outcome in results if outcome is None])
        logger.info("Finished ingestion. %i documents were ingested; %i failed; %i skipped."%(docs_succeeded
                                                                                , docs_failed
                                                                                , docs_skipped))
    else:
        detected_monographs = detect_monographs(args.data_dir)
        logger.debug(detected_monographs)

        if args.limit is not None:
            detected_monographs = detected_monographs[:args.limit]
            logger.info("The script was called with --limit=%i. Taking only first %i documents." % (args.limit, args.limit))

        monograph_objects = [
            process_monograph_folder(monograph)
            for monograph in detected_monographs
        ]

        results = []
        for monograph in monograph_objects:
            if monograph.pages == []:
                results.append((monograph.bid, False))
            else:
                results.append((
                    monograph.bid,
                    save_monograph_to_db(
                        monograph,
                        force_update=args.force_update
                    )
                ))

        docs_succeeded = [
            bid
            for bid, outcome in results if outcome
        ]

        docs_failed = [
            bid
            for bid, outcome in results
            if outcome is False
        ]

        docs_skipped = [
            bid
            for bid, outcome in results
            if outcome is None
        ]

        logger.info(
            "Finished: {} documents ingested; {} failed; {} skipped.".format(
                len(docs_succeeded),
                len(docs_failed),
                len(docs_skipped)
            )
        )
        logger.info("Documents succeeded {}".format(docs_succeeded))
        logger.info("Documents failed {}".format(docs_failed))
        logger.info("Documents skipped {}".format(docs_skipped))
        return


if __name__ == "__main__":
    main()
