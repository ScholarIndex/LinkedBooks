#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
The tests for the `resolution` module.
"""

__author__ = """Giovanni Colavizza"""

import pdb
import time
from sklearn.externals import joblib
from pytest import mark
from commons.dbmodels import *
from disambiguation.primary.parser import ps_disamb

@mark.run(order=9)
def test_disamb_ps(mongoengine_connection, test_db):

	# load references
	docs_to_process = list()
	for p in Processing.objects(is_parsed=True, is_disambiguated_s=False, is_disambiguated_p=False,
	                            is_disambiguated_partial=False):
		docs_to_process.append((p.bid, p.number))

	docs = list()
	for y in docs_to_process:
		try:
			d = LBDocument.objects(bid=y[0], number=y[1]).get()
			docs.append(d.id)
		except:
			print("missing doc " + str(y))

	t = time.time()
	refs = list()
	for d in docs:
		refs.extend([x for x in Reference.objects(document_id=d)])

	print("Loaded %i references" % (len(refs)))
	print("Elapsed time %f" % (time.time() - t))

	# load models for PS
	crf1 = joblib.load('disambiguation/primary/model_dev/models/is_asve.pkl')
	crf2 = joblib.load('disambiguation/primary/model_dev/models/asve_ids.pkl')

	# dict of all asve tags
	disambiguations = list()
	for r in refs:
		if r.ref_type == "primary":
			asve_tag = ps_disamb(r.reference_string, crf1, crf2)
			if asve_tag:
				# find asve tag
				disambiguations.append({"surface": r.reference_string, "reference": r.id, "archival_document": None,
					 "checked": False, "correct": True, "type": "reference_disambiguation",
					 "provenance": "processing", "document_id":r.document_id})

	# store disambiguations (no need to do this)
	print("Number of disambiguations: %d"%len(disambiguations))
	if len(disambiguations)>0:
		print(disambiguations[0])

	# update docs in processing
	for d in list(set(docs_to_process)):
		try:
			if not d[0] or len(d[1]) == 0:
				processing_info = Processing.objects(type_document="monograph", bid=d[0]).get()
			else:
				processing_info = Processing.objects(type_document="issue", number=d[1], bid=d[0]).get()
			if not processing_info.is_disambiguated_p:
				processing_info.is_disambiguated_p = True
				processing_info.updated_at = datetime.now()
				processing_info.save()
				print("Updated item in Processing for disambiguated_p: %s, %s" % (d[0], d[1]))
		except Exception as e:
			print(e)
			print("Missing item in Processing for disambiguated_p: %s, %s" % (d[0], d[1]))
			continue
			
