# -*- coding: utf-8 -*-
"""
USED una tantum to correct contents with more than 10 items
"""
__author__ = """Giovanni Colavizza"""

from collections import OrderedDict
import logging
logging.basicConfig(filename="logs/xml_parser.log", level=logging.INFO)
logger = logging.getLogger(__name__)
from configparser import ConfigParser
from datetime import datetime
# Mongo
from pymongo import MongoClient, TEXT, ASCENDING
from mongoengine import connect as engineconnect
# Test models
from commons.dbmodels import *

# Establish Mongo connections
config = ConfigParser(allow_no_value=False)
config.read("config.conf")
logger.info('Read configuration file.')

# SANDBOX the playground
db = "mongo_sand"
mongo_user = config.get(db, 'username')
mongo_pwd = config.get(db, 'password')
mongo_auth = config.get(db, 'auth-db')
mongo_host = config.get(db, 'db-host')
mongo_port = config.get(db, 'db-port')
con = MongoClient(mongo_host,port=int(mongo_port), **{"socketKeepAlive":True})
con.linkedbooks_sandbox.authenticate(mongo_user, mongo_pwd, source=mongo_auth)
db_sand = con.linkedbooks_sandbox

counter = 0
for reference in db_sand.references.find(no_cursor_timeout=True):
    if len(reference["contents"].items()) > 9:
        contents = OrderedDict(sorted(reference["contents"].items(), key=lambda x: int(x[0])))
        rs = " ".join([x["surface"] for x in contents.values()])
        #print(contents)
        #print(rs)
        db_sand.references.update_one({"_id":reference["_id"]},{"$set":{"reference_string":rs,"contents":contents}},upsert=False)
        counter += 1
print(counter)