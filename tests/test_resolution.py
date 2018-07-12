#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
The tests for the `resolution` module.
"""

__author__ = """Matteo Romanello, Giovanni Colavizza"""

import pdb
import json
import logging
import pkg_resources
from datetime import datetime
from pytest import mark
from commons.dbmodels import *
from resolution.lookup_authors import *
from resolution.resolvers import *

logger = logging.getLogger(__name__)

def test_lookup_authors_mongo(mongoengine_connection):
	"""
	Test for function `resolution.lookup_authors.lookup_author_mongo`.
	"""
	author_name = "Francesco T. Roffarè"
	cleaned_author_name = clean_authors(author_name)
	result = lookup_author_mongo(cleaned_author_name)
	assert result.viaf_id == "250457414"

def test_viaf_lookup(mongoengine_connection):
	"""
	Test for function `resolution.lookup_authors.viaf_lookup`.
	"""
	
	test_author = Author.objects[0]
	assert test_author.viaf_id == viaf_lookup(test_author.author_final_form)[1]
	
	# this raised an issue/exception related to the quote signs
	author = viaf_lookup('McCreary, Joseph "Foley"')
	assert author is not None
	print(author)
	
def test_resolve_author(mongoengine_connection):
	# the author is not contained in the MongoDB
	author_count_before = Author.objects.count()
	
	# the author name is resolved, and a corresponding record is added
	resolve_author("Murru, Pierfranco", "processing")

	author_count_after = Author.objects.count()

	# let's check that one author was acutally added to the MongoDB
	assert author_count_before < author_count_after

def test_resolve_by_document(mongoengine_connection):
	bids_not_disambiguated = [(document.bid, document.number) for document in Processing.objects(is_parsed=True, is_disambiguated_s=False)]
	logger.info("References in %i documents have not yet been disambiguated" % len(bids_not_disambiguated))

	for bid, issue_number in bids_not_disambiguated:
		# process references in document
		if issue_number == "":
			resolve_references_by_document(bid)
		else:
			resolve_references_by_document(bid, issue_number)

		# update document record in processing
		if issue_number == "":
			record = Processing.objects(bid=bid).get()
		else:
			record = Processing.objects(bid=bid, number=issue_number).get()

		record.updated_at = datetime.utcnow()
		record.is_disambiguated_s = True
		record.save()

	bids_not_disambiguated = [document.bid for document in Processing.objects(is_parsed=True, is_disambiguated_s=False)]
	assert len(bids_not_disambiguated) == 0
