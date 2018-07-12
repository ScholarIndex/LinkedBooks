# -*- coding: utf-8 -*-
"""
USED una tantum to add new fidls in references
"""
__author__ = """Giovanni Colavizza"""

from datetime import datetime
import logging
logging.basicConfig(filename="reference_parsing/scripts/logs/xml_parser.log", level=logging.INFO)
logger = logging.getLogger(__name__)
from configparser import ConfigParser
# Mongo
from pymongo import MongoClient, TEXT, ASCENDING

# Establish Mongo connections
config = ConfigParser(allow_no_value=False)
config.read("reference_parsing/scripts/config.conf")
logger.info('Read configuration file.')

# SANDBOX the playground
db = "mongo_sand"  # "mongo_prod" "mongo_dev" "mongo_sand"
mongo_db = config.get(db, 'db-name')
mongo_user = config.get(db, 'username')
mongo_pwd = config.get(db, 'password')
mongo_auth = config.get(db, 'auth-db')
mongo_host = config.get(db, 'db-host')
mongo_port = config.get(db, 'db-port')

client = MongoClient(mongo_host,port=int(mongo_port), **{"socketKeepAlive":True})
db = client[mongo_db]
db.authenticate(mongo_user, mongo_pwd, source=mongo_auth)

documents = {(k["bid"],k["number"]):k["_id"] for k in db.documents.find()}
# errors corrected
documents[('LO10439164','1866_1.1')] = documents[('LO10439164','1866_1_1')]
documents[('LO10439164','1866_1.2')] = documents[('LO10439164','1866_1_2')]
documents[('LO10439164','1866_1.3')] = documents[('LO10439164','1866_1_3')]

logger.info("Loaded %d documents"%len(documents))
counter = 0
for reference in db.references.find(no_cursor_timeout=True):
	s_img = reference["contents"]["1"]["single_page_file_number"]
	e_img = s_img
	try:
		#e_img = reference["contents"][str(len(reference["contents"]))]["single_page_file_number"]
		e_img = reference["contents"][[x for x in reference["contents"].keys()][-1]]["single_page_file_number"]
	except:
		logger.warning("Error in contents")
		logger.info("Ref id: %s"%str(reference["_id"]))
	if e_img > s_img:
		hold = s_img
		e_img = s_img
		s_img = hold
	db.references.update_one({"_id":reference["_id"]},{"$set":{"updated_at":datetime.now(),"document_id":documents[(reference["bid"],reference["issue"])],"start_img_number":s_img,"end_img_number":e_img}},upsert=False)
	counter += 1
db.references.create_index([('document_id', ASCENDING), ('start_img_number', ASCENDING), ('end_img_number', ASCENDING)], default_language='none')
logger.info("Updated %d references"%counter)