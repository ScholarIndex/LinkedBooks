# -*- coding: utf-8 -*-
"""
USE una tantum: ingests already corrected disambiguations of PS
"""
__author__ = """Giovanni Colavizza"""

import codecs, logging, csv
logging.basicConfig(filename="logger.log", level=logging.WARNING)
logger = logging.getLogger(__name__)
from configparser import ConfigParser
from mongoengine import connect as engineconnect
from commons.dbmodels import *

# load data
ref_dict = list()
with codecs.open("data.csv",encoding="utf-8") as f:
    reader = csv.reader(f,delimiter=",",quotechar='"')
    next(reader, None)
    for row in reader:
        article_id,article_url,article_title,article_author,img_number,order_in_page,ref_new,ref_class,ref_id,score,surface,notes = row
        bid,issue,id = article_id.split(":")
        ref_dict.append((bid,issue,article_title,img_number,order_in_page,ref_new,ref_id))

print(len(ref_dict))
logger.info('Read %d references to ingest'%len(ref_dict))

# CONFIG THE DATABASE
db = "mongo_sand"#mongo_dev mongo_prod

config = ConfigParser(allow_no_value=False)
config.read("config.conf")
logger.info('Read configuration file')

mongo_db = config.get(db, 'db-name')
mongo_user = config.get(db, 'username')
mongo_pwd = config.get(db, 'password')
mongo_auth = config.get(db, 'auth-db')
mongo_host = config.get(db, 'db-host')
mongo_port = config.get(db, 'db-port')

logger.debug(engineconnect(mongo_db
                     , username=mongo_user
                     , password=mongo_pwd
                     , authentication_source=mongo_auth
                     , host=mongo_host
                     , port=int(mongo_port)))

# drop previously inserted stuff
logger.info("Removed disambiguations: %d"%len(Disambiguation.objects(type="reference_disambiguation",archival_document__ne=None)))
Disambiguation.objects(type="reference_disambiguation",archival_document__ne=None).delete()

#db.disambiguations.remove( {"type":"reference_disambiguation","archival_document": { "$ne": null } } )
#db.disambiguations.count( {"type":"reference_disambiguation","archival_document": { "$ne": null } } )

issues_dict = list()
asve_dict = dict()
exceptions = list()
already_matched_references = list()
saved_refs = list()
for r in ref_dict:
    try:
        ref = Reference.objects(bid=r[0],reference_string=r[5],order_in_page=r[4])
        if len(ref)>1 and len(r[3])>0:
            for refs in ref:
                if refs.contents["1"]["single_page_file_number"] == int(r[3]) and not refs.id in already_matched_references:
                    ref = refs
                    break
        elif len(ref)>1:
            for refs in ref:
                if not refs.id in already_matched_references:
                    ref = refs
        else:
            ref = ref[0]
        ref_str = ref.issue.replace("_","")
        ref_str = ref_str.replace(".", "")
        r_issue = r[1].replace("_","")
        r_issue = r_issue.replace(".", "")
        assert ref_str==r_issue # check issue is matching
        if len(r[3]) > 0:
            assert ref.contents["1"]["single_page_file_number"] == int(r[3]) # check image number is matching
        # check not to disambiguate the same reference twice
        if not ref.id in already_matched_references:
            already_matched_references.append(ref.id)
        else:
            logger.warning("Already matched: %s" %str(ref.id))
            continue
        issues_dict.append((r[0],ref.issue))
        # find matching asve record (first look in the local dict for fast lookup)
        asve_ref = r[-1]
        if asve_ref[-1] == ".":
            asve_ref = asve_ref[:-1]
        if asve_ref not in asve_dict.keys():
            try:
                a_ref = ArchivalRecordASVE.objects(internal_id=asve_ref)
                asve_dict[asve_ref] = a_ref[0]
            except:
                logger.warning("Missing record in asve: "+asve_ref)
        if asve_ref in asve_dict.keys():
            # prepare new disambiguation for ingestion
            r = Disambiguation(**{"reference":ref,"archival_document":asve_dict[asve_ref],"checked":True,"correct":True,"provenance":"processing","type":"reference_disambiguation"})
            r.updated_at = datetime.now()
            #r.validate()
            r.save()
            r.reload()
            saved_refs.append(r.id)
            logger.info("Inserted disamb: " + str(r.id))
        else:
            logger.warning("Skipped reference: "+str(ref))
    except Exception as e:
        logger.warning("Exception: " + str(e))
        exceptions.append(r)

# store the ids of new disambiguations
with open("new_objects.csv","w") as f:
    for d in saved_refs:
        f.write(str(d))
        f.write("\n")

# report exceptions
logger.warning("Number of exceptions: %d"%len(exceptions))
for e in exceptions:
    print(e)

# update processing
for x in list(set(issues_dict)):
    bid, issue = x
    try:
        if not issue or len(issue) == 0:
            processing_info = Processing.objects(type_document="monograph", bid=bid).get()
        else:
            processing_info = Processing.objects(type_document="issue", number=issue, bid=bid).get()
        if not processing_info.is_disambiguated_p:
            processing_info.is_disambiguated_p = True
            processing_info.updated_at = datetime.now()
            #processing_info.validate()
            processing_info.save()
    except:
        logger.warning("Missing item in Processing: %s, %s" % (bid, issue))
        continue