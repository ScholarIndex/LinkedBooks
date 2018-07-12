#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
TODO.

Usage:
    commons/run_api.py --config=<file>
"""

import sys
sys.path += ["../", "./","../../"]
import pdb
import json
from docopt import docopt
from flask import Flask
from flask_mongoengine import MongoEngine
from werkzeug.contrib.fixers import ProxyFix
from api.venicescholar_api.cache import cache
from api.venicescholar_api import api, api_blueprint

def create_app(config_file="config/dev_prod.cfg"):
	"""
	Returns an instance of the LinkedBooks API as a flask app.
	"""
	app = Flask(__name__)
	app.config.from_pyfile(config_file)
	cache.init_app(app)
	db = MongoEngine()
	db.init_app(app)
	app.wsgi_app = ProxyFix(app.wsgi_app)
	app.register_blueprint(api_blueprint)
	return app

app = create_app(config_file="config/prod.cfg")

#pdb.set_trace()

if __name__ == "__main__":
	arguments = docopt(__doc__)
	app = create_app(config_file=arguments["--config"])
	print(app)
	app.run()
