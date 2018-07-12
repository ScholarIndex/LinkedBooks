# -*- coding: utf-8 -*-
"""
Loads checked and correct disambiguations AND a sample of references which are NOT checked-
"""
__author__ = """Giovanni Colavizza"""

import logging
from collections import defaultdict
from configparser import ConfigParser
from pymongo import MongoClient

def load_is_asve_dataset(config_file_name="config.conf",db="mongo_sand"):
	"""
	Returns a dataset with disambiguated (corrected and checked) references to ASVe, and a sample of references (primary) not to ASVe (i.e. from disambiguated objects)
	:param config_file_name: db config
	:param db: database connection
	:return: a dictionary with results
	"""

	logging.basicConfig(filename="logs/loader.log", level=logging.INFO)
	logger = logging.getLogger(__name__)

	config = ConfigParser(allow_no_value=False)
	config.read(config_file_name)
	mongo_db = config.get(db, 'db-name')
	mongo_user = config.get(db, 'username')
	mongo_pwd = config.get(db, 'password')
	mongo_auth = config.get(db, 'auth-db')
	mongo_host = config.get(db, 'db-host')
	mongo_port = config.get(db, 'db-port')

	client = MongoClient(mongo_host)
	db = client[mongo_db]
	db.authenticate(mongo_user, mongo_pwd, source=mongo_auth)

	asve_ids = {k["_id"]:k["internal_id"] for k in db.bibliodb_asve.find()}
	logger.info("Loaded %d ASVe IDs" % len(asve_ids))
	data = dict()
	query = {"type":"reference_disambiguation","archival_document": { "$ne": None },"checked":True,"correct":True}
	positive_references = list()
	for d in db.disambiguations.find(query,no_cursor_timeout=True):
		data[str(d["reference"])] = {"disamb_id":str(d["_id"]),"asve":asve_ids[d["archival_document"]],"y":1,"ref_type":"","surface":"","components":list()}
		positive_references.append(d["reference"])

	logger.info("Loaded %d positive references"%len(data))
	# load data from references using pymongo for speed
	client = MongoClient(mongo_host)
	db = client[mongo_db]
	db.authenticate(mongo_user, mongo_pwd, source=mongo_auth)
	query = {"_id":{"$in":positive_references}}

	for ref in db.references.find(query,no_cursor_timeout=True):
		data[str(ref["_id"])]["ref_type"] = ref["ref_type"]
		data[str(ref["_id"])]["surface"] = ref["reference_string"]
		data[str(ref["_id"])]["components"] = sorted([(int(x),y["surface"],y["tag"]) for x,y in ref["contents"].items()],key=lambda x:x[0],reverse=False)

	logger.info("Added info for %d positive references" % len(data))

	# OK docs: select references only from disambiguated docs!
	ok_docs = [(d["bid"],d["number"]) for d in db.processing.find({"is_disambiguated_p":True})]
	# negative sampling: take a number twice the positive examples, remove them if present and sample again to an equal number
	for ref in db.references.find({"ref_type":"primary"}):
		if str(ref["_id"]) in data.keys():
			continue # it's a positve reference
		if (ref["bid"],ref["issue"]) not in ok_docs:
			continue
		data[str(ref["_id"])] = {"disamb_id": None, "asve": None, "y": 0, "ref_type": "",
		                     "surface": "", "components": list()}
		data[str(ref["_id"])]["ref_type"] = ref["ref_type"]
		data[str(ref["_id"])]["surface"] = ref["reference_string"]
		data[str(ref["_id"])]["components"] = sorted([(int(x), y["surface"], y["tag"]) for x, y in ref["contents"].items()],key=lambda x:x[0],reverse=False)

	logger.info("Added info for %d negative references" % (len(data)-len(positive_references)))

	return data

"""
if __name__ == "__main__":

	data = load_is_asve_dataset()
	print(len(data))
	print(len([x for x in data.values() if x["y"]==1]))
"""