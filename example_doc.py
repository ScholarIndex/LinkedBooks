#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Example of documentation for the whole project
"""

__author__ = """Giovanni Colavizza, Matteo Romanello"""

import os

def hello():
    """
    prints "Hello World" to the standard output.

    :return: None
    """
    print("Hello World!")
def get_mongodb_sandbox(config_file=os.path.join(_current_dir,"../annotation_connector/config_files/LB_machine_sandbox.conf")):
    """
    Initialise a connection to the MongoDB according
    to the specified configuration file.
    
    :param config_file: the path to the configuration file
    :return: a DB connection (type = `pymongo.database.Database`)
    """
    config = ConfigParser.ConfigParser(allow_no_value=True)
    config.read(config_file)
    mongo_host = config.get('mongo','db-host')
    mongo_db = config.get('mongo','db-name')
    mongo_port = config.getint('mongo','db-port')
    mongo_user = config.get('mongo','username')
    mongo_pwd = config.get('mongo','password')
    mongo_auth = config.get('mongo','auth-db')
    loader = ContentLoader(mongo_host,mongo_db,mongo_user,mongo_pwd,mongo_auth)
    return loader.db