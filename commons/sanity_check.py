#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: Matteo Romanello, matteo.romanello@epfl.ch

"""
CLI for sanity_check.py

Usage:
    commons/sanity_check.py <host> <dbname>
"""

import os
import pdb
import sys
sys.path += ["../", "./"]
import glob
import pandas as pd
import numpy as np
import logging
from docopt import docopt
from dbmodels import *
from tqdm import tqdm
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from content_ingester import move_issue_folder, convert_issue_number

#HOST = "128.178.245.10"
#HOST = "localhost"
#DBNAME = "linkedbooks_refactored"
#DBNAME = "linkedbooks_sandbox"
PORT = 27017
USER = "scripty"
PASSWORD = "L1nk3dB00ks"
AUTHDB = "admin"
LOG_FILE = None
#LOG_FILE = "refactor_documents.log"

global logger
logger = logging.getLogger()

def isolate_problematic_documents(dataframe, dest_folder):
	# spot discrepancies between: n of pages, n of images, n of text files
	discrepancies = dataframe[(dataframe["number_text_files"]>0) & (dataframe["number_text_files"]!=dataframe["number_document_pages"])]
	if len(discrepancies) > 0:
		print("\nRecords with a discrepancy between numer of document and number of html files:")
		for i, row in discrepancies.iterrows():
			document = LBDocument.objects(id=ObjectId(row["lbdocument"])).first()
			pages_in_golden = [page for page in document.pages if page.in_golden]
			print("%s; pages in golden: %i (ingested at: %s)" % (row["text_folder"], len(pages_in_golden), document.ingestion_timestamp))
			issue = {
				"bid" : row["bid"]
				, "number" : row["issue_number"].replace(".", "_")
				, "path" : row["text_folder"]
			}
			move_issue_folder(issue, dest_folder)

def fix_processing(dataframe):
	# find processing records to fix
	processing_record_to_fix = dataframe[(dataframe["has_image_folder"]==True) 
									& (dataframe["is_img"]==False) 
									& (dataframe["number_image_files"]>0)]
	if len(processing_record_to_fix) > 0:
		print("\nDetected records in `processing` for which images exists but `is_img` is False:")
		for i, row in processing_record_to_fix.iterrows():
			processing_record = Processing.objects(id=ObjectId(row["processing_mongo_id"])).first()
			print(processing_record.id)
			assert processing_record is not None
			processing_record.is_img = True
			processing_record.updated_at = datetime.utcnow()
			processing_record.save()

	# find processing records to fix
	processing_record_to_fix = dataframe[(dataframe["has_text_folder"]==True) 
									& (dataframe["is_ocr"]==False)]
	if len(processing_record_to_fix) > 0:
		print("\nDetected records in `processing` for which text (html) exists but `is_ocr` is False:")
		for i, row in processing_record_to_fix.iterrows():
			processing_record = Processing.objects(id=ObjectId(row["processing_mongo_id"])).first()
			print(processing_record.id)
			assert processing_record is not None
			processing_record.is_ocr = True
			processing_record.updated_at = datetime.utcnow()
			processing_record.save()

	# find processing records to fix
	processing_record_to_fix = dataframe[(dataframe["has_text_folder"]==True) 
									& (dataframe["is_ingested_ocr"]==False) 
									& (dataframe["lbdocument"].notnull())]
	if len(processing_record_to_fix) > 0:
		print("\nDetected records in `processing` for which text (html) and an ingested document exist but `is_ingested_ocr` is False:")
		for i, row in processing_record_to_fix.iterrows():
			processing_record = Processing.objects(id=ObjectId(row["processing_mongo_id"])).first()
			print(processing_record.id)
			assert processing_record is not None
			processing_record.is_ingested_ocr = True
			processing_record.updated_at = datetime.utcnow()
			processing_record.save()
	return

def fix_page_annotations(dataframe):
	"""
	two cases:
	1. `annotations_ids` is a non-empty list and `is_annotated` == False -> fix!
	2. `annotations_ids` is an empty list and `is_annotated` == True ->
	"""
	documents = [LBDocument.objects(id=ObjectId(row["lbdocument"])).first() 
						for i,row in dataframe.iterrows() if row["lbdocument"] is not None]

	# case 1. `annotations_ids` is an empty list and `is_annotated` == True
	pages = [page["_id"] for page in db.pages.find({"is_annotated":True,"annotations_ids":{"$exists":True,"$size":0}},{"_id":1})]
	for page in pages:
		Page.objects(id=page).update_one(set__is_annotated=False)
		print("Fixed `is_annotated` field in page %s" % str(page))
	print("%i pages were fixed" % len(pages))

	# case 2.
	pages = [page for page in db.pages.find({"is_annotated":False,"annotations_ids":{"$exists":True,"$ne":[]}},{"_id":1})]
	for page in pages:
		Page.objects(id=page).update_one(set__is_annotated=True)
		print("Fixed `is_annotated` field in page %s" % str(page))
	print("%i pages were fixed" % len(pages))
	return

def isolate_not_ingested(dataframe, dest_folder):
	not_ingested = dataframe[(dataframe["has_text_folder"]==True) & (dataframe["lbdocument"].isnull())]
	if len(not_ingested) > 0:
		print("The following folders were, in fact, not ingested:")
		for i, row in not_ingested.iterrows():
			print(row["text_folder"])
			issue = {
				"bid" : row["bid"]
				, "number" : row["issue_number"].replace(".", "_")
				, "path" : row["text_folder"]
			}
			move_issue_folder(issue, dest_folder)
	return

def gather_data(mongoengine_connection, image_base_path, text_base_path):
	temp = []
	for processing_record in tqdm(Processing.objects(is_digitized=True), desc="checking"):
		record = {}
		record["processing_mongo_id"] = processing_record.id
		record["bid"] = processing_record.bid
		record["issue_number"] = processing_record.number
		record["type"] = processing_record.type_document
		record["is_img"] = processing_record.is_img
		record["is_ocr"] = processing_record.is_ocr
		record["checked_at"] = datetime.utcnow()
		subfolder_mappings = {"monograph":"books", "issue":"journals"}
		# treat the journal issues separately because the `foldername` in `processing`
		# in some cases is not realiable
		if record["type"]=="issue":
			subfolders_with_bid = [directory 
										for directory in os.listdir("%s%s/"%(image_base_path, subfolder_mappings[record["type"]])) 
																							if processing_record.bid in directory]
			record["has_image_folder"] = False
			for folder in subfolders_with_bid:
				path = "%s%s/%s/%s"%(image_base_path, subfolder_mappings[record["type"]], folder, processing_record.number)
				if os.path.isdir(path):
					record["has_image_folder"] = True
					record["image_folder"] = path
					break
				else:
					path = "%s%s/%s/%s"%(image_base_path, subfolder_mappings[record["type"]], folder, processing_record.number.replace(".", "_"))
					if os.path.isdir(path):
						record["has_image_folder"] = True
						record["image_folder"] = path
						break
		else:
			record["has_image_folder"] = os.path.isdir("%s%s"%(image_base_path, processing_record.foldername))
			if record["has_image_folder"]:
				record["image_folder"] = "%s%s"%(image_base_path, processing_record.foldername)
		
		# get the count of image numbers
		if record["has_image_folder"]:
			record["number_image_files"] = len(glob.glob("%s/*.jpg"%record["image_folder"]))
		else:
			record["number_image_files"] = np.nan

		if record["type"]=="monograph": 
			text_folder = "%singested/%s/%s" % (text_base_path, subfolder_mappings[record["type"]], record["bid"])
		else:
			text_folder = "%singested/%s/%s/%s" % (text_base_path, subfolder_mappings[record["type"]], record["bid"], record["issue_number"])
			
			if not os.path.isdir(text_folder):
				text_folder = "%singested/%s/%s/%s" % (text_base_path
													, subfolder_mappings[record["type"]]
													, record["bid"]
													, convert_issue_number(record["issue_number"]))
				if not os.path.isdir(text_folder):
					text_folder = "%singested/%s/%s/%s" % (text_base_path
														, subfolder_mappings[record["type"]]
														, record["bid"]
														, record["issue_number"].replace(".","_"))
		record["is_ingested_ocr"] = processing_record.is_ingested_ocr
		
		# check whether a text folder exists
		if os.path.isdir(text_folder):
			record["text_folder"] = text_folder
			record["has_text_folder"] = True
		else:
			record["text_folder"] = None
			record["has_text_folder"] = False

		if record["type"]=="monograph": 
			document = db.documents.find_one({"bid":processing_record.bid})
			if document is not None:
				record["lbdocument"] = str(document["_id"])
				record["number_document_pages"] = len(document["pages"])
				if record["has_text_folder"]:
					record["number_text_files"] = len(glob.glob("%s/*.htm"%record["text_folder"]))
			else:
				record["lbdocument"] = None
		else:
			document = db.documents.find_one({"bid":processing_record.bid, "number":processing_record.number})
			if document is None:
				document = db.documents.find_one({"bid":processing_record.bid, "number":processing_record.number.replace(".","_")})
			if document is not None:
				record["lbdocument"] = str(document["_id"])
				record["number_document_pages"] = len(document["pages"])
				if record["text_folder"] is None:
					from_annotations = True if "annotations_to_ingest" in document["path"] else False
					html_in_path = True if "/html/" in document["path"] else False
					if from_annotations:
						record["text_folder"] = document["path"].replace(
														"/home/projects/linkedbooks/annotations_to_ingest/lb_1_renamed/quarantine/"
														, "/home/projects/linkedbooks/annotations_to_ingest/2017_01_16/journals/"
														)
					if html_in_path:
						record["text_folder"] = "%singested/%s/%s/%s" % (text_base_path
												, subfolder_mappings[record["type"]]
												, folder
												, processing_record.number.replace(".", "_"))
					if record["text_folder"] is not None and os.path.isdir(record["text_folder"]):
						record["has_text_folder"] = True
						if from_annotations:
							record["number_text_files"] = len(glob.glob("%s/*.txt"%record["text_folder"]))
						else:
							record["number_text_files"] = len(glob.glob("%s/*.htm"%record["text_folder"]))
					else:
						#logger.debug("No text folder for %s-%s" % (record["bid"], record["issue_number"]))
						pass
				else:
					record["number_text_files"] = len(glob.glob("%s/*.htm"%record["text_folder"]))
			else:
				record["lbdocument"] = None

		temp.append(record)
	return pd.DataFrame(temp)

def main(arguments):
	# initialise the logger; prints to stdout
	global logger
	logger.setLevel(logging.DEBUG)
	if(LOG_FILE is not None):
	    handler = logging.FileHandler(filename=LOG_FILE, mode='w')
	else:
	    handler = logging.StreamHandler()
	formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	logger.info("Logger initialised")

	# initiate mongoengine connection
	connect(arguments["<dbname>"]
	        , username=USER
	        , password=PASSWORD
	        , authentication_source=AUTHDB
	        , host=arguments["<host>"]
	        , port=PORT
	        )

	global db, DBNAME, HOST
	client = MongoClient(arguments["<host>"])
	client[arguments["<dbname>"]].authenticate(USER,PASSWORD,source=AUTHDB)
	db = client[arguments["<dbname>"]]
	logger.debug(db)

	DBNAME = arguments["<dbname>"]
	HOST = arguments["<host>"]
	
	#pdb.set_trace()
	dataframe = gather_data(db, "/media/linkedbooks/journals_jpg/", "/media/linkedbooks/text/")
	#pdb.set_trace()
	#fix_processing(dataframe)
	#fix_page_annotations(dataframe)
	#isolate_problematic_documents(dataframe, "/media/linkedbooks/text/temporary/journals/")
	#dataframe = gather_data(db, "/media/linkedbooks/journals_jpg/", "/media/linkedbooks/text/")
	dataframe.to_csv("sanity_check_%s_%s.csv" % (arguments["<host>"], arguments["<dbname>"]), encoding="utf-8")
	#isolate_problematic_documents(dataframe, "/media/linkedbooks/text/temporary/journals/")
	#isolate_not_ingested(dataframe, "/media/linkedbooks/text/to_ingest/journals/")
	#pdb.set_trace()

if __name__ == '__main__':
	arguments = docopt(__doc__)
	#pdb.set_trace()
	main(arguments)
