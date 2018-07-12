#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
TBD
"""
__author__ = """Matteo Romanello, matteo.romanello@epfl.ch"""

import logging
import numpy as np
logger = logging.getLogger(__name__) 
from elasticsearch import Elasticsearch 
from .supporting_functions import *
from .fuzzy_matching import fuzzyContainment, fuzzyContainmentML

def cleanup_reference_monographs(reference):
	"""
	TODO
	"""
	cleanedup_ref = {}
	if("author" in reference):
		cleanedup_ref["author"] = take_upper(cleanup_string(remove_hyphenation(reference["author"]))).lower()
	if("title" in reference):
		cleanedup_ref["title"] = filter_stop(cleanup_string(remove_hyphenation(reference["title"]).lower()))
	if("year" in reference):
		cleanedup_ref["year"] = cleanup_string(reference["year"])
	if("publicationyear" in reference):
		cleanedup_ref["year"] = cleanup_string(reference["publicationyear"])
	if("publicationplace" in reference):
		cleanedup_ref["place"] = cleanup_string(remove_hyphenation(reference["publicationplace"])).lower()
	if("publisher" in reference):
		cleanedup_ref["publisher"] = filter_stop(cleanup_string(remove_hyphenation(reference["publisher"]).lower()))
	return cleanedup_ref

def cleanup_reference_journals(reference):
	"""
	TODO
	"""
	cleanedup_ref = {}
	if("author" in reference):
		cleanedup_ref["author"] = take_upper(cleanup_string(remove_hyphenation(reference["author"]))).lower()
	if("title" in reference):
		cleanedup_ref["title"] = filter_stop(cleanup_string(remove_hyphenation(reference["title"]).lower()))
	if("year" in reference):
		#cleanedup_ref["year"] = cleanup_string(remove_hyphenation(reference["year"]))
		cleanedup_ref["year"] = cleanup_string(reference["year"])
	if("publicationyear" in reference):
		#cleanedup_ref["year"] = cleanup_string(remove_hyphenation(reference["publicationyear"]))
		cleanedup_ref["year"] = cleanup_string(reference["publicationyear"])
	if("publicationplace" in reference):
		cleanedup_ref["place"] = cleanup_string(remove_hyphenation(reference["publicationplace"])).lower()
	if("publisher" in reference):
		cleanedup_ref["publisher"] = filter_stop(cleanup_string(remove_hyphenation(reference["publisher"]).lower()))
	return cleanedup_ref

def cleanup_sbn_record(record):
	"""
	TODO
	"""
	if("author" in record.keys()):
		record["author"] = take_upper(cleanup_string(remove_hyphenation(record["author"]))).lower()
	if("title" in record.keys()):
		record["title"] = filter_stop(cleanup_string(remove_hyphenation(record["title"]).lower()))
	if("year" in record.keys() and record["year"] is not None):
		record["year"] = cleanup_string(remove_hyphenation(record["year"]))
	if("publisher" in record.keys() and record["publisher"] is not None):
		record["publisher"] = filter_stop(cleanup_string(remove_hyphenation(record["publisher"]).lower()))
	if("place" in record.keys()):
		record["place"] = cleanup_string(remove_hyphenation(record["place"])).lower()
	return record

def fetch_candidates(title, n_words=2):
	"""
	Search for the first n_words in ES.
	"""
	split_marks = [".",":"]
	search_string = None
	clean_title = filter_stop(cleanup_string(remove_hyphenation(title)))
	search_string = " ".join(clean_title.split()[:n_words])
	return search_string, search_ES(search_string)

def search_ES(input_string, limit=2000, es_server="localhost:9200"):
	"""
	TODO
	"""
	es_client = Elasticsearch([es_server])
	languages = ["de","en","fr","it"]
	temp = {}
	logger.debug("searching for %s"%input_string)
	for lang in languages:
		q = query = {
					 'query': {
						'match': {
								'titolo.%s'%lang:{
									"query":input_string,
									"operator":"and",
									"fuzziness":"AUTO"
									}
						}
					}
				}
		result = es_client.search(index="iccu", doc_type="recordsbn", body=query, size=limit, request_timeout=3000)
		# avoid to return duplicates
		for record in result["hits"]["hits"]:
			temp[record["_source"]["codiceIdentificativo"]] = record["_source"]
	return list(temp.values())

def prune_candidates(reference_title, candidates, best_n):
	"""
	TODO
	"""
	pruned_candidates = []
	for candidate in candidates:
		clean_result_title = candidate["title"]
		clean_input_title = filter_stop(cleanup_string(remove_hyphenation(reference_title))).lower()
		title_similarity = fuzzyContainmentML(clean_result_title
											, clean_input_title)
		logger.debug("The score of fuzzyContainmentML between %s and %s is %s"%(
														clean_result_title
														,clean_input_title
														,title_similarity
														 ))
		pruned_candidates.append((title_similarity, candidate))
	best_matches = sorted(pruned_candidates, key = lambda x:x[0], reverse=True)
	return best_matches[:best_n]

def compare(reference, catalog_record, title_similarity_score):
	"""
	This function supersedes the omonymous function in `lookuppers.py`.

	TODO:
	- add a comparison with the editor (if it's in the reference)

	"""
	score_explanation = "(%s=%s"%("title_similarity",title_similarity_score)
	scores = [title_similarity_score]
	# TODO: this needs to be improved! right now returns too highly a value for wrong matches
	# compare the `author` field
	if("author" in reference and "author" in catalog_record and catalog_record["author"] is not None 
																	and len(catalog_record["author"])>2):
		score = fuzzyContainment(reference["author"],catalog_record["author"])
		scores.append(score)
		logger.debug("[author] The score of fuzzyContainment between %s and %s is %s"%(reference["author"]
																					  , catalog_record["author"]
																					  , score))
	else:
		score = 0.01
		scores.append(score)
	score_explanation = "%s %s=%s"%(score_explanation,"+ author_similarity",score)
	# compare the `year` field
	if("year" in reference and catalog_record["year"] is not None and len(catalog_record["year"])>2):
		if("-" in reference["year"]):
			first_part = reference["year"].split("-")[0].replace(" ","")
			second_part = reference["year"].split("-")[1].replace(" ","")
			score = first_part in catalog_record["year"] or second_part in catalog_record["year"]
		else:
			score = reference["year"] == catalog_record["year"]
		logger.debug("[year] The similarity between %s and %s is %s"%(reference["year"], catalog_record["year"], score))
		scores.append(score)
	else:
		score = 0.01
		scores.append(score)
	score_explanation = "%s %s=%s"%(score_explanation,"+ year_similarity",score)
	if("place" in reference and "place" in catalog_record and catalog_record["place"] is not None 
														   and len(catalog_record["place"])>2):
		score = fuzzyContainment(reference["place"], catalog_record["place"])
		logger.debug("[publicationplace] The score of fuzzyContainment between %s and %s is %s"%(reference["place"]
																								, catalog_record["place"]
																								, score))
		scores.append(score)
	else:
		score = 0.01
		scores.append(score)
	score_explanation = "%s %s=%s"%(score_explanation,"+ publplace_similarity",score)
	if("publisher" in reference and "place" in catalog_record["publisher"] 
														   and catalog_record["publisher"] is not None 
														   and len(catalog_record["publisher"])>2):
		score = fuzzyContainment(reference["publisher"], catalog_record["publisher"])
		logger.debug("[publisher] The score of fuzzyContainment between %s and %s is %s"%(reference["publisher"]
																						 , catalog_record["publisher"]
																						 , score))
		scores.append(score)
	else:
		score = 0.01
		scores.append(score)
	score_explanation = "%s %s=%s)"%(score_explanation,"+ publisher_similarity",score)
	global_score = sum(scores)/len(reference)
	score_explanation = "%s / %s = %s"%(score_explanation,len(reference),global_score)
	message = """
	Input reference: %s
	Record compared: %s
	Global score: %s
	Score's explanation: %s
	"""%(reference, catalog_record, global_score, score_explanation)
	return global_score, score_explanation

def lookup_SBN(reference, n_words=3, pruning_threshold=150, similarity_threshold=0.3, ref_type="journals"):
	"""
	The goal of this lookup is to work with references to secondary sources extracted
	both from monographs and journals.
	"""
	logger.info("settings: n_words=%s, pruning_threshold=%s, similarity_threshold=%s"%(n_words, pruning_threshold, similarity_threshold))
	try:
		search_string, candidates = fetch_candidates(reference["title"],n_words)
		bid_candidates = [normalize_bid(candidate["codiceIdentificativo"]) 
																   for candidate in candidates]
		prepared_candidates = [prepare_sbn_record(candidate) for candidate in candidates]
		cleaned_candidates = [cleanup_sbn_record(candidate) for candidate in prepared_candidates]
		pruned = prune_candidates(reference["title"], cleaned_candidates, pruning_threshold)
		pruned_bids = [record["bid"] for score,record in pruned]
		if(ref_type=="journals"):
			reference = cleanup_reference_journals(reference)
		elif(ref_type=="monographs"):
			reference = cleanup_reference_monographs(reference)
		comparison_results = [] 
		for title_similarity, candidate in pruned:
			similarity_score, score_explanation = compare(reference,candidate,title_similarity)
			if(similarity_score >= similarity_threshold):
				comparison_results.append((similarity_score, candidate, score_explanation))
		comparison_results = sorted(comparison_results,key=lambda x:x[0],reverse=True)
		return comparison_results
	# TODO: revise
	except Exception as e:
		logger.error(e)
		return None

def lookup_SBN_eval(datum, n_words=3, pruning_threshold=150, similarity_threshold=0.3, ref_type="journals"):
	"""
	Same as `lookup_SBN`, only tailored for the evaluation of the lookup.
	It compares the result of lookup with the expected one, and retains some intermediate
	information that are useful for debug purposes.

	TODO: say what structure should `datum` have
	"""
	logger.info("settings: n_words=%s, pruning_threshold=%s, similarity_threshold=%s"%(n_words, pruning_threshold, similarity_threshold))
	try:
		search_string, candidates = fetch_candidates(datum["title"],n_words)
		bid_candidates = [normalize_bid(candidate["codiceIdentificativo"]) 
																   for candidate in candidates]
		prepared_candidates = [prepare_sbn_record(candidate) for candidate in candidates]
		cleaned_candidates = [cleanup_sbn_record(candidate) for candidate in prepared_candidates]
		pruned = prune_candidates(datum["title"], cleaned_candidates, pruning_threshold)
		pruned_bids = [record["bid"] for score,record in pruned]
		if(ref_type=="journals"):
			reference = cleanup_reference_journals(datum["reference"])
		elif(ref_type=="monographs"):
			reference = cleanup_reference_monographs(datum["reference"])
		comparison_results = [] 
		for title_similarity, candidate in pruned:
			similarity_score, score_explanation = compare(reference,candidate,title_similarity)
			if(similarity_score >= similarity_threshold):
				comparison_results.append((similarity_score, candidate, score_explanation))
		comparison_results = sorted(comparison_results,key=lambda x:x[0],reverse=True)
		if(len(comparison_results)>0):
			max_score = max([similarity_score for similarity_score, candidate, score_explanation in comparison_results])
		else:
			max_score = np.nan
		single_best = [candidate["bid"]
							for similarity_score, candidate, score_explanation in comparison_results
																			if similarity_score == max_score]
		datum["single_best_bid"] = single_best
		datum["bids_gt_threshold"] = [candidate["bid"]
											for similarity_score, candidate, score_explanation in comparison_results]
		datum["bids_gt_threshold_similarity"] = ", ".join(["%s (%s)"%(candidate["bid"],similarity_score)
											for similarity_score, candidate, score_explanation in comparison_results])
		datum["number_candidates"] = len(candidates)
		datum["bid_candidates"] = bid_candidates
		datum["bid_in_candidates"] = len(set(bid_candidates).intersection(datum["groundtruth_BID"]))>0
		datum["bid_in_pruned_candidates"] = len(set(pruned_bids).intersection(datum["groundtruth_BID"]))>0
		datum["search_string"] = search_string
		datum["raised_error"] = False
		datum["correct_match"] = len(set(single_best).intersection(datum["groundtruth_BID"]))>0
		print("[%s] Lookup result: correct = %s"%(datum["mongoid"],datum["correct_match"]))
	except Exception as e:
		logger.error("Lookup of reference %s raised the following error: %s"%(datum["mongoid"],e))
		datum["raised_error"] = True
	return datum
