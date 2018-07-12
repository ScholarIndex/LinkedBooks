# -*- coding: utf-8 -*-
"""
Evaluates PS disambiguations in the database using the groundtruth
"""
__author__ = """Giovanni Colavizza"""

"""
After evaluation, results need to be manually checked for minor inconsistencies.
Evaluation on 41 articles (691 ground references), full primary references, after parsing and manual correction:
Precision: 0.985
Recall: 0.9116 (largely due to imprecision of the parser)
These scores are the final evaluation of the over 20k disambiguations ingested during spring 2017 and marked as checked.
"""

import codecs,csv
from collections import defaultdict
from configparser import ConfigParser
from mongoengine import connect as engineconnect
from commons.dbmodels import *

def evaluation_full(filename="disambiguation/groundtruth/primary_full_23052017_1.csv",config_file_name="disambiguation/primary/config.conf",db="mongo_sand"):
	"""
	Calculates the precision and recall of the primary disambiguations (full) in the DB
	:param filename: file of ground truth
	:param config_file_name: Mongo config
	:param db: Mongo db
	:return: Prints results and stores files with errors for manual inspection
	"""

	config = ConfigParser(allow_no_value=False)
	config.read(config_file_name)
	mongo_db = config.get(db, 'db-name')
	mongo_user = config.get(db, 'username')
	mongo_pwd = config.get(db, 'password')
	mongo_auth = config.get(db, 'auth-db')
	mongo_host = config.get(db, 'db-host')
	mongo_port = config.get(db, 'db-port')

	engineconnect(mongo_db
	                     , username=mongo_user
	                     , password=mongo_pwd
	                     , authentication_source=mongo_auth
	                     , host=mongo_host
	                     , port=int(mongo_port))

	# load ground truth
	ground_per_article = defaultdict(list)
	surface_ground = dict()
	with codecs.open(filename, encoding="utf-8") as f:
		reader = csv.reader(f)
		next(reader,None) # skip headers

		for row in reader:
			article_id, article_url, article_title, image_number, reference, asve_id, note = row
			article_id = article_id.strip()
			if len(article_id) > 0:
				ground_per_article[article_id].append(asve_id.strip()[:-1]) # append removing last dot of the ID
				if not asve_id.strip()[:-1] in surface_ground.keys():
					surface_ground[asve_id.strip()[:-1]] = dict()
				if not article_id in surface_ground[asve_id.strip()[:-1]].keys():
					surface_ground[asve_id.strip()[:-1]][article_id] = [reference]
				else:
					surface_ground[asve_id.strip()[:-1]][article_id].append(reference)
	print(len(ground_per_article))
	print(sum([len(x) for x in ground_per_article.values()]))

	# LOAD disambiguations fo every article, check precision and recall
	# load articles
	articles = dict()
	for aid in ground_per_article.keys():
		try:
			article = Article.objects(internal_id=aid).get()
			articles[aid] = article
		except:
			print(aid)
			continue

	print("loaded %d articles"%len(articles))

	# load references for these articles
	article_refs = dict()
	article_disambs = dict()
	surface_disamb = dict()
	asve_ids = {a.id:a.internal_id for a in ArchivalRecordASVE.objects()}
	print("Loaded ASVe ids")
	for aid,a in articles.items():
		print(a.document_id)
		article_refs[aid] = [x for x in Reference.objects(document_id=a.document_id,start_img_number__gte=a.start_img_number,end_img_number__lte=a.end_img_number)]
		print(len(article_refs[aid]))
		article_disambs[aid] = list()
		for ref in article_refs[aid]:
			disamb = None
			try:
				disamb = Disambiguation.objects(reference=ref.id,archival_document__ne=None).get()
			except Exception as e:
				#print(e)
				continue
			if disamb:
				asve_id_internal = asve_ids[disamb.archival_document.id]
				article_disambs[aid].append(asve_id_internal)
				if not asve_id_internal in surface_disamb.keys():
					surface_disamb[asve_id_internal] = dict()
				if not aid in surface_disamb[asve_id_internal].keys():
					surface_disamb[asve_id_internal][aid] = [ref.reference_string]
				else:
					surface_disamb[asve_id_internal][aid].append(ref.reference_string)
		print(len(article_disambs[aid]))

	article_disambs = {x:list(set(y)) for x,y in article_disambs.items()}
	ground_per_article = {x:list(set(y)) for x,y in ground_per_article.items()}
	print("loaded references and disambiguations")

	# calculate precision and recall
	# recall
	n = 0
	d = 0
	in_g_not_p = dict()
	for article, ground in ground_per_article.items():
		n += len(set(ground).intersection(set(article_disambs[article])))
		d += len(ground)
		in_g_not_p[article] = list(set(ground).difference(set(article_disambs[article])))
	print("Recall: %f"%(n/d))
	with codecs.open("disambiguation/primary/in_g_not_p.csv","w",encoding="utf8") as f:
		for article, l in in_g_not_p.items():
			for item in l:
				f.write(article+";"+item+";\""+", ".join([x for x in surface_ground[item][article]])+"\"\n")

	# precision
	n = 0
	d = 0
	in_p_not_g = dict()
	for article, ground in ground_per_article.items():
		n += len(set(article_disambs[article]).intersection(set(ground)))
		d += len(article_disambs[article])
		in_p_not_g[article] = list(set(article_disambs[article]).difference(set(ground)))
	print("Precision: %f"%(n/d))
	with codecs.open("disambiguation/primary/in_p_not_g.csv","w",encoding="utf8") as f:
		for article, l in in_p_not_g.items():
			for item in l:
				f.write(article+";"+item+";\""+", ".join([x for x in surface_disamb[item][article]])+"\"\n")

if __name__ == "__main__":

	database = "mongo_prod"  # "mongo_prod" "mongo_dev" "mongo_sand"
	evaluation_full(db=database)