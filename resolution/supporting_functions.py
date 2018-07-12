#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import print_function
from __future__ import division

"""
TBD
"""
__author__ = """Matteo Romanello, matteo.romanello@epfl.ch"""


import logging
logger = logging.getLogger(__name__) 
import re
import string
import nltk
from nltk.corpus import stopwords
from pyes import ES, MatchQuery, TermQuery
from commons.dbmodels import *

def take_upper(input_string):
	"""
	NB: when `input_string` contains "weird" unicode characters this function produces considerably different 
	results under py2 as opposed to py3 (prefer the latter).

	take out capital letters and nothing else
	"""
	regex = r"[A-Z]\w+"
	return " ".join(re.findall(regex,input_string))

def filter_stop(input_string, len_filter=3):
	"""
	Removes stopwords and shorter or eq. than len_filter words from all NLTK supported languages
	"""
	return " ".join([x for x in nltk.word_tokenize(input_string) if x not in stopwords.words() and len(x) > len_filter])

def cleanup_string(input_string):
	"""
	cleanup punctuation (except hyphen)
	"""
	input_string = input_string.replace(u"’"," ")
	input_string = input_string.replace(u"'"," ")
	to_exclude = set(string.punctuation)-set(["-"])
	return ''.join(ch for ch in input_string if ch not in to_exclude)

def remove_hyphenation(string):
	"""
	Removes hyphenation from the input string
	"""
	#regex = r"(\w+)(- )(\w+)?"
	#return re.sub(regex,r"\1\3",string)
	try:
		regex = r"(\w+)(- )(\w+)?"
		return re.sub(regex,r"\1\3",string)
	except Exception as e:
		return string

def parse_field_pubblicazione(field):
	"""
	Extracts year, place and publisher from the field `pubblicazione` by applying a cascade of regexps.
	"""
	exp2 = r'^(?P<place>\D+)(?:\s?\W\s?)(?P<publisher>.*?)\D{1}?(?P<year>\d+)?$'
	exp1 = r'^(?P<place>.*?)(?::)(?P<publisher>.*?)\D{1}?(?P<year>\d+)?$'
	exp3 = r'(?:.*?)?(?P<year>\d{4})'
	exp4 = r'^(?P<place>\D{3,})$'
	not_matched = 0
	partly_matched = 0
	result = {}
	result1 = re.match(exp1,field)
	if(result1 is None):
		result2 = re.match(exp2,field)
		if(result2 is None):
			result3 = re.match(exp3,field)
			if(result3 is None):
				result4 = re.match(exp4,field)
				if(result4 is None):
					not_matched += 1
				else:
					result = result4.groupdict()
			else:
				result = result3.groupdict()
		else:
			result = result2.groupdict()
	else:
		result = result1.groupdict()
	return result

def to_iccu_bid_old(bid):
	"""
	'AGR0000002' => u'IT\\ICCU\\AGR\\0000002'
	"""
	return "IT\\ICCU\\%s\\%s"%(bid[:3],bid[3:])

def to_iccu_bid(bid):
	"""
	'AGR0000002' => u'IT\\ICCU\\AGR\\0000002'
	"""
	exp = r'^(?P<part1>\D+)(?P<part2>.*?)?$'
	result = re.match(exp, bid).groupdict()
	if(len(result["part1"])>=3 and result["part2"].isdigit()):
		return "IT\\ICCU\\%s\\%s"%(result["part1"],result["part2"])
	elif(len(result["part1"])<3 and result["part2"].isdigit()):
		return "IT\\ICCU\\%s\\%s"%(bid[:3],bid[3:])
	else:
		return "IT\\ICCU\\%s\\%s"%(bid[:4],bid[4:])

def normalize_bid(iccu_bid):
	"""
	u'IT\\ICCU\\AGR\\0000002' => 'AGR0000002'
	"""
	return "".join(iccu_bid.split("\\")[-2:])

def is_BID_in_SBN(bid, es_server="localhost:9200"):
	sbn_bid = to_iccu_bid(bid)
	q = TermQuery('codiceIdentificativo',sbn_bid)
	es_conn = ES(server=es_server)
	resultset = list(es_conn.search(query=q,indices="iccu"))
	if(len(resultset)>0):
		return True
	else:
		return False

def find_BID_in_SBN(bid, es_server="localhost:9200"):
	sbn_bid = to_iccu_bid(bid)
	q = TermQuery('codiceIdentificativo',sbn_bid)
	es_conn = ES(server=es_server)
	resultset = list(es_conn.search(query=q,indices="iccu"))
	if(len(resultset)>0):
		return resultset
	else:
		return None

def get_dewey(record):
	if record is not None and "dewey" in record:
		dewey_classification = record["dewey"]
		return dewey_classification
	else:
		return None

def parse_pubblicazione_bruteforce(sbn_record):
	exp = r'\d{4}'
	def is_valid_year(number):
		return number > 1400 and number <= 2016
	if("pubblicazione" in sbn_record):
		pubblicazione = sbn_record["pubblicazione"]
		result = re.findall(exp, pubblicazione)
		if(len(result)>=1):
			year = int(result[0])
			if is_valid_year(year):
				return year
			else:
				return None
		else:
			return None
	else:
		return None

def prepare_sbn_record(record):
	"""
	(duplicate of function in lookup_monographs; this one should be kept)
	"""
	# codiceIdentificativo => bid (normalized)
	new_record = {
		"year" : ""
		,"publisher" : ""
		,"publicationplace" : ""
		,"author" : ""
	}
	new_record["bid"] = normalize_bid(record["codiceIdentificativo"])
	new_record["title_orig"] = record["titolo"]
	
	if("contenutoIn" in record.keys()):
		new_record["title"] = record["contenutoIn"]
		new_record["title_orig"] = record["contenutoIn"]
	else:
		new_record["title"] = record["titolo"]
	
	# try to remove the author/editor from the title (if present)
	if("/" in new_record["title"]):
		new_record["title"] = "".join(new_record["title"].split(" /")[:-1]) # to handle the case of multiple slashes
		if(not "autorePrincipale" in record.keys()):
			new_record["author"] = "".join(record["titolo"].split(" /")[-1])
	
	# autorePrincipale => author
	if("autorePrincipale" in record.keys()):
		new_record["author"] = record["autorePrincipale"]
	
	# `pubblicazione` (when present) => year, place and publisher
	if("nomi" in record.keys()):
		new_record["names"] = [nome for nome in record["nomi"] if "," in nome]
	if("pubblicazione" in record.keys()):
		new_record.update(parse_field_pubblicazione(record["pubblicazione"]))
	return new_record

def enrich_metadata(bid):
	metadata = {}
	if bid is not None:
		record = find_BID_in_SBN(bid)
		if record is not None:
			record = record[0]
			metadata = prepare_sbn_record(record)
			
			metadata_record = Metadata.objects(bid=bid).first()
			document_record = LBDocument.objects(bid=bid).first()
			
			date = ""
			if metadata_record is not None:
				date = metadata_record.date
			else:
				date = parse_pubblicazione_bruteforce(record)

			metadata["year"] = date if date is not None else ""
			metadata["digitization_provenance"] = metadata_record.provenance if metadata_record is not None else ""
			metadata["document_id"] = document_record
			
			del metadata["publicationplace"]

			metadata["dewey_classifications"]  = get_dewey(record)
			
			if "localizzazioni" in record:
				metadata["available_at"] = record["localizzazioni"]
			
			if "linguaPubblicazione" in record:
				metadata["publication_language"] = record["linguaPubblicazione"]
			
			if "paesePubblicazione" in record:
				metadata["publication_country"] = record["paesePubblicazione"]
			
			if "numeri" in record:
				metadata["identifiers"] = record["numeri"]
				
	return metadata

