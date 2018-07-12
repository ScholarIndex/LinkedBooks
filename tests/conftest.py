# keep together and at the top of file or it won't work...
import logging,sys
sys.path += ["../","./","../../../"]
import pdb
import json
import pkg_resources
from flask import Flask
from flask_mongoengine import MongoEngine
from werkzeug.contrib.fixers import ProxyFix
#from api.venicescholar_api.cache import cache
#from api.venicescholar_api import api, api_blueprint
#from api.run_api import create_app
from pymongo import MongoClient
from mongoengine import connect
from pytest import fixture

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger()

try:  # Python 2.7+
    from ConfigParser import ConfigParser
except ImportError:
    from configparser import ConfigParser

@fixture(scope="session")
def config_parser(config_file_name="tests.conf"):
    """Returns a parsed configuration file for the tests."""
    config_file = pkg_resources.resource_filename(__name__,config_file_name)
    config = ConfigParser(allow_no_value=True)
    config.read(config_file)
    logger.info('Read configuration file %s'%config_file)
    return config


@fixture(scope="session")
def test_db(config_parser):
    """
    Returns a connection to the test MongoDB (conn parameters as per config file).
    """
    mongo_host = config_parser.get('mongo','db-host')
    mongo_db = config_parser.get('mongo','db-name')
    mongo_port = config_parser.getint('mongo','db-port')
    mongo_user = config_parser.get('mongo','username')
    mongo_pwd = config_parser.get('mongo','password')
    mongo_auth = config_parser.get('mongo','auth-db')
    client = MongoClient(mongo_host)
    db = client[mongo_db]
    db.authenticate(mongo_user,mongo_pwd,source=mongo_auth)
    logger.debug("Successfully connected to %s"%db)
    return db


@fixture(scope="module")
def clear_test_db(test_db):
    to_drop = ["processing"
                , "metadata"
                , "documents"
                , "pages"
                , "annotations"
                , "references"
                , "bibliodb_authors"
                , "bibliodb_journals"
                , "bibliodb_articles"
                , "bibliodb_books"
                , "disambiguations"]
    for collection in to_drop:
        test_db.drop_collection(collection)
        logger.info("The collection %s was dropped from %s" % (collection
                                                              , test_db))


@fixture(scope="session")
def mongoengine_connection(config_parser):
    mongo_db = config_parser.get('mongo','db-name')
    mongo_user = config_parser.get('mongo','username')
    mongo_pwd = config_parser.get('mongo','password')
    mongo_auth = config_parser.get('mongo','auth-db')
    mongo_host = config_parser.get('mongo', 'db-host')
    mongo_port = config_parser.get('mongo', 'db-port')
    logger.info(mongo_host)
    # let `mongoengine` establish a connection to the MongoDB via `mongoengine.connect()`
    return connect(mongo_db
        ,username=mongo_user
        ,password=mongo_pwd
        ,connect=False
        ,authentication_source=mongo_auth
        ,host=mongo_host
        ,port=int(mongo_port))

"""
@fixture(scope="session")
def app():
    return create_app(config_file="../api/config/test.cfg")
"""
