# -*- coding: utf-8 -*-
"""
Disambiguation primary
"""
__author__ = """Giovanni Colavizza"""
#TODO: Matteo, I'd put all disambiguations in one parser..

import pdb
import logging
from collections import OrderedDict
from configparser import ConfigParser
from datetime import datetime
import time
from pymongo import MongoClient
from mongoengine import connect as engineconnect
from commons.dbmodels import *
from sklearn.externals import joblib
from disambiguation.primary.model_dev.supporting_functions import cleanup

def ps_disamb(reference_string,crf1,crf2,threshold=0.9):
	# TODO: batch process, not one by one.
	"""
	Checks if a reference is ASVe and disambiguates it.
	:param reference_string: text of a string
	:param threshold: confidence of is_asve classification
	:return: None or the ASVe internal_id
	"""

	t = cleanup(reference_string)

	if crf1.predict_proba([t])[0][1] >= threshold and crf1.predict([t])[0] == 1:
		# is ASVe
		return crf2.predict([t])[0]
	else:
		return None

if __name__ == "__main__":

	logging.basicConfig(filename="disambiguation/logs/parser.log", level=logging.INFO)
	logger = logging.getLogger(__name__)

	# choose the database to parse. Only documents that have not been parsed, and that have their full text available will be considered.
	db = "mongo_dev"  # "mongo_prod" "mongo_dev" "mongo_sand"
	config = ConfigParser(allow_no_value=False)
	config.read("disambiguation/primary/model_dev/config.conf")

	mongo_db = config.get(db, 'db-name')
	mongo_user = config.get(db, 'username')
	mongo_pwd = config.get(db, 'password')
	mongo_auth = config.get(db, 'auth-db')
	mongo_host = config.get(db, 'db-host')
	mongo_port = config.get(db, 'db-port')
	client = MongoClient(mongo_host)
	db = client[mongo_db]
	db.authenticate(mongo_user, mongo_pwd, source=mongo_auth)

	logger.debug(engineconnect(mongo_db, username=mongo_user
	              , password=mongo_pwd
	              , authentication_source=mongo_auth
	              , host=mongo_host
	              , port=int(mongo_port)))

	# load references
	docs_to_process = list()
	for p in Processing.objects(is_parsed=True,is_disambiguated_p=False):
		docs_to_process.append((p.bid,p.number))

	docs = list()
	for y in docs_to_process:
		try:
			d = LBDocument.objects(bid=y[0],number=y[1]).get()
			docs.append(d.id)
		except:
			print("missing doc "+str(y))

	t = time.time()
	refs = list()
	for d in docs:
		refs.extend([x for x in Reference.objects(document_id=d,ref_type="primary")])

	logger.info("Loaded %d primary references"%(len(refs)))
	logger.info("Elapsed time %f"%(time.time()-t))

	# load models for PS
	crf1 = joblib.load('disambiguation/primary/model_dev/models/is_asve.pkl')
	crf2 = joblib.load('disambiguation/primary/model_dev/models/asve_ids.pkl')

	# dict of all asve tags
	asve_tags = dict()
	disambiguations = list()
	for r in refs:
		if r.ref_type == "primary":
			asve_tag = ps_disamb(r.reference_string,crf1,crf2)
			if asve_tag:
				# find asve tag
				if asve_tag not in asve_tags.keys():
					try:
						asve_item = ArchivalRecordASVE.objects(internal_id=asve_tag).get()
						asve_tags[asve_tag] = asve_item.id
					except:
						logger.warning("Missing asve id in bibliodb_asve: %s"%asve_tag)
						continue
				disambiguations.append({"surface":r.reference_string,"reference":r.id,"archival_document":asve_tags[asve_tag],
				                                       "checked":False,"correct":True,"type":"reference_disambiguation","provenance":"processing","document_id":r.document_id.id})
		else:
			# add more disambiguation tasks
			continue

	# store disambiguations
	ps_ok = True
	try:
		db.disambiguations.insert_many(disambiguations)  # insert disambiguations.
	except Exception as e:
		logger.warning("Error in inserting disambiguations")
		logger.warning(e)
		ps_ok = False

	# update docs in processing
	for d in list(set(docs_to_process)):
		try:
			if not d[0] or len(d[1]) == 0:
				processing_info = Processing.objects(type_document="monograph", bid=d[0]).get()
			else:
				processing_info = Processing.objects(type_document="issue", number=d[1], bid=d[0]).get()
			if not processing_info.is_disambiguated_p and ps_ok:
				processing_info.is_disambiguated_p = True
				processing_info.updated_at = datetime.now()
				processing_info.save()
				logger.info("Updated item in Processing for disambiguated_p: %s, %s" % (d[0], d[1]))
		except Exception as e:
			logger.warning(e)
			logger.warning("Missing item in Processing for disambiguated_p: %s, %s" % (d[0], d[1]))
			continue
