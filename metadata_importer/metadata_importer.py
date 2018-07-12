# -*- coding: utf-8 -*-
"""
Parser for matadata in each folder/digital object
Takes in input a folder structure from Venice with .xml files and outputs csv files and updates the Mongo metadata collection
"""
__author__ = """Giovanni Colavizza"""

import os, codecs, logging, re
from datetime import datetime
logger = logging.getLogger(__name__) 
from bs4 import BeautifulSoup
from configparser import ConfigParser
from mongoengine import connect as engineconnect
from dbmodels import *
from supporting_functions import walklevel, sanitize_strings

class Title():
    """
    An object Title.
    """

    def __init__(self, surface="", responsible="", specifications="", publisher="", materiality=""):
        self.surface = surface
        self.responsible = responsible
        self.materiality = materiality
        self.specifications = specifications
        self.publisher = publisher

    def export_json(self):
        return {"surface": self.surface, "responsible": self.responsible, "specifications": self.specifications, "publisher": self.publisher, "materiality": self.materiality}

class Relation:
    """
    An object Relation.
    """

    def __init__(self, title=Title(), type_relation=""):
        self.title = title
        self.type_relation = type_relation

    def export_json(self):
        return {"title": self.title.export_json(), "type": self.type_relation}

class Issue:
    """
    An obeject Issue.
    """

    def __init__(self, foldername="", year=None, imgpagestart_img_number=None, imgpagestart_page_number=None, imgpagestart_type="", imgindex="", digitisation_note="", provenance="", issue="", operator="", marked_as_removed=False):
        self.foldername = foldername
        self.year = year
        self.imgpagestart_img_number = imgpagestart_img_number
        self.imgpagestart_page_number = imgpagestart_page_number
        self.imgpagestart_type = imgpagestart_type
        self.imgindex = imgindex
        self.digitisation_note = digitisation_note
        self.provenance = provenance
        self.issue = issue
        self.operator = operator
        self.marked_as_removed = marked_as_removed

    def export_json_imgpagestart(self):
        return {"img_number": self.imgpagestart_img_number, "page_number": self.imgpagestart_page_number, "type": self.imgpagestart_type}
    def export_json(self):
        return {"operator": self.operator, "imgpagestart": {"img_number": self.imgpagestart_img_number, "page_number": self.imgpagestart_page_number, "type": self.imgpagestart_type}, "imgindex": self.imgindex,
                "digitisation_note": self.digitisation_note, "foldername": self.foldername, "provenance": self.provenance, "year": self.year, "issue": str(self.issue), "marked_as_removed": self.marked_as_removed}

class meta_object:
    """
    A metaobject.
    """
    def __init__(self, title=Title(), creator=""):
        self.title = title
        self.creator = creator
        self.subjects = list()
        self.issues = list()
        self.relations = list()
        self.id1 = ""
        self.id2 = ""
        self.provenance = ""
        self.date = ""
        self.type1 = ""
        self.filename = ""
        self.language = ""
        self.type2 = "monograph"
        self.operator = ""
        self.scan = ""
        self.bib = list()
        self.marked_as_removed = False

    def add_subject(self, subject):
        self.subjects.append(subject)

    def is_empty(self):
        if len(self.title.surface) < 1 and len(self.creator) < 1:
            return True
        else:
            return False

    def export_json(self):
        title = self.title.export_json()
        relations = [r.export_json() for r in self.relations]
        issues = [r.export_json() for r in self.issues]
        if len(self.issues) > 0:
            self.type2 = "journal"
        return {"title": title, "creator": self.creator, "subjects": self.subjects, "issues": issues, "relations": relations,
                "sbn_id": self.id1, "bid": self.id2, "provenance": self.provenance, "date": self.date, "type_catalogue": self.type1,
                "type_document": self.type2, "foldername": self.filename, "language": self.language, "operator": self.operator,
                "digitisation_note": self.scan, "img_bib": self.bib, "marked_as_removed": self.marked_as_removed}


def title_parser(title):
    """
    Title extractor (titles are formatted according to REICAT rules).
    :param title: a string.
    :return: an instance of Title
    """
    # remove squared brackets for inferred information
    title = title.replace("[","")
    title = title.replace("]","")
    title = title.replace("*","")

    # get title and publication information
    if " / " in title:
        if len(title.split(" / ")) > 2:
            title = title.split(" - ")
            return Title(surface = title[0], publisher = title[1], materiality = title[2])
        title, pub = title.split(" / ")
    elif " - " in title:
        return Title(surface = title.split(" - ")[0], publisher = title.split(" - ")[1])
    else:
        return Title(title)

    # process details
    pub = pub[:pub.find("((")-1]
    pub_data = pub.split(" - ")
    if len(pub_data) == 4:
        responsible = pub_data[0].strip()
        specifications = pub_data[1].strip()
        publisher = pub_data[2].strip()
        materiality = pub_data[3].strip()
    elif len(pub_data) == 3:
        responsible = pub_data[0].strip()
        specifications = ""
        publisher = pub_data[1].strip()
        materiality = pub_data[2].strip()
    elif len(pub_data) == 2:
        responsible = pub_data[0].strip()
        specifications = ""
        publisher = pub_data[1].strip()
        materiality = ""
    elif len(pub_data) == 1:
        responsible = pub_data[0].strip()
        specifications = ""
        publisher = ""
        materiality = ""
    else:
        logger.warning("PROBLEM with Title %s"%title)
        return Title(title)
    materiality = materiality[:materiality.find("(")-1].strip()

    return Title(title, responsible, specifications, publisher, materiality)

def relation_parser(relation):
    """
    Relation extractor (relations are formatted according to REICAT rules).
    Example: <relation>Titolo="*Venetiae. - Venezia : Albrizzi, [19..]-." Relationship="FA PARTE DI"</relation>

    :param relation: a string with a relation
    :return: an instance of Title
    """
    if "Titolo=" in relation and "Relationship" in relation:
        return title_parser(relation[8:relation.find("\" Relationship")]), relation[relation.find("\" Relationship")+16:-1]
    else:
        return Title()

def img_bib_parser(imgbib):
    """
    Parser for the contents of imgbibs tags.
    Example: <imgbib>138-140</imgbib>

    :param imgbib: a string
    :return: a list of ints with the numbers of images where the list of references is for a book.
    """
    # case of multiple intervals
    bib = list()
    if not imgbib or imgbib == "None" or len(imgbib) < 1:
        return []
    for b in imgbib.split(";"):
        b = b.strip()
        if "-" in b:
            interval = b.split("-")
        else:
            interval = b.split(".")
        for n,i in enumerate(interval):
            if len(i) < 1:
                del interval[n]
                continue
            i = i.strip()
            # remove all non-numbers
            i = re.sub(r"\D", "", i)
            try:
                i = int(i)
            except:
                print(i)
            interval[n] = i
        if len(interval) > 1:
            bib.extend(range(interval[0], interval[1]+1))
        else:
            bib.append(interval[0])
    return bib

def parser(xml_dir  = "xml_output"):
    """
    Parser which goes through all xml files and generates a collection of metadata objects, ready for ingestion.
    :param xml_dir: the root of the folder containing xml files
    :return: a list of records to ingest in the database
    """

    logger.info("New parsing job started")

    # misspells of tags, taken from all_tags below:
    img_bibs = ['imgbib', 'imgib', 'imgibib']
    subjects = ['subject', 'sub2.', 'sub2']
    operators = ['operatore', 'operator', 'operaor', 'operatora']
    img_indexes = ['imgindex', 'imgondex', 'imgimdex', 'imgndex']
    scans = ['scan', 'sca']
    img_pg_starts = ['imgpagestart', 'imgppagestart', 'impagestart', 'imgpagstart', 'imgpagestaret', 'imgpagestar',
                     'imgpagastart']

    # data structures
    records = list()
    record = meta_object()

    for root, dirs, files, level in walklevel(xml_dir, 4):
        logger.info("Number of folders: "+str(len(dirs)))
        for fil in files:
            if ".xml" in fil and level == 3: # parse monographs or journals
                logger.info(root)
                bid = root.split("/")[-1].split("_")[1]
                text = codecs.open(os.path.join(root, fil), "r", "utf-8")
                soup = BeautifulSoup(text.read(), "html.parser")
                logger.info("SOUP made")
                if len(soup.find_all("dc")) < 1:
                    logger.warning("Missing DC in xml elements for %s"%root)
                    continue
                all_elements = [a.name for a in soup.dc.descendants]
                logger.info("Elements loaded")
                # dump previous record if not empty
                if not record.is_empty():
                    if len(record.issues)>0:
                        record.type2 = "journal"
                    record.marked_as_removed = False
                    records.append(record)
                logger.info("Dump of previous record done")
                record = meta_object()
                title = sanitize_strings(soup.dc.title.string)
                record.title = title_parser(title)
                for s in subjects:
                    for subject in soup.find_all(s):
                        record.add_subject(sanitize_strings(subject.string))
                ids = soup.find_all("identifier")
                if len(ids) >= 2:
                    record.id1 = sanitize_strings(ids[0].string)
                    record.id2 = bid
                else:
                    if "-" in ids[0].string:
                        record.id1 = sanitize_strings(ids[0].string)
                        record.id2 = bid
                    else:
                        record.id1 = ""
                        record.id2 = bid
                creator = ""
                for cre in soup.find_all("creator"):
                    if len(creator) < 1:
                        creator = cre.string
                    else:
                        creator += "; "+cre.string
                record.creator = sanitize_strings(creator)
                if "relation" in all_elements:
                    for r in soup.dc.findAll("relation"):
                        rel = sanitize_strings(r.string)
                        r_title, r_type = relation_parser(rel)
                        if r_title.surface != "":
                            record.relations.append(Relation(r_title, r_type))
                if "date" in all_elements:
                    record.date = sanitize_strings(soup.dc.date.string)
                else:
                    record.date = ""
                if "type" in all_elements:
                    record.type1 = sanitize_strings(soup.dc.type.string)
                else:
                    record.type1 = ""
                if "language" in all_elements and soup.dc.language.text:
                    record.language = sanitize_strings(soup.dc.language.string)
                else:
                    record.language = ""
                if "provenance" in all_elements:
                    record.provenance = sanitize_strings(soup.dc.provenance.string)
                else:
                    record.provenance = ""
                record.filename = fil.replace(".xml","")
                record.bib = []
                for ib in img_bibs:
                    if ib in all_elements:
                        el = soup.find(ib)
                        bib = img_bib_parser(el.string)
                        if len(bib) > 0:
                            record.bib = bib
                for o in operators:
                    if o in all_elements and soup.find(o).text:
                        record.operator = sanitize_strings(soup.find(o).text)
                for s in scans:
                    if s in all_elements and soup.find(s).text:
                        record.scan = sanitize_strings(soup.find(s).string)
            elif ".xml" in fil and level == 4: # parse journal issues
                logger.info(root)
                j_bid, j_issue = root.split("/")[-2:]
                j_bid = j_bid.split("_")[1]
                if j_bid != record.id2:
                    logger.warning("Colliding metadata folders for BID and journals, possible absence of metadata file in "+j_bid)
                    continue
                text = codecs.open(os.path.join(root, fil), "r", "utf-8")
                soup = BeautifulSoup(text.read(), "html.parser")
                all_elements = [a.name for a in soup.descendants]
                issue = Issue()
                for o in operators:
                    if o in all_elements and soup.find(o).text:
                        issue.operator = sanitize_strings(soup.find(o).text)
                for ips in img_pg_starts:
                    if ips in all_elements and soup.find(ips).text:
                        try:
                            issue.imgpagestart_img_number = int(soup.find(ips).string)
                        except:
                            issue.imgpagestart_img_number = None
                            logger.warning("Bad imgpagestart_img_number %s, %s" % (j_bid, soup.find(ips).string))
                        try:
                            issue.imgpagestart_page_number = int(soup.find(ips)["n"])
                        except:
                            issue.imgpagestart_page_number = None
                            logger.warning("Bad imgpagestart_page_number %s, %s"%(j_bid,soup.find(ips)["n"]))
                        issue.imgpagestart_type = soup.find(ips)["type"]
                for ii in img_indexes:
                    if ii in all_elements and soup.find(ii).text:
                        issue.imgindex = soup.find(ii).string
                for s in scans:
                    if s in all_elements and soup.find(s).text:
                        issue.digitisation_note = sanitize_strings(soup.find(s).string)
                if "provenance" in all_elements:
                    issue.provenance = sanitize_strings(soup.provenance.string)
                # parse year and issue numbers, add folder (same as issue)
                issue.foldername = j_issue
                x = j_issue.split("_")
                if len(x) == 2:
                    try:
                        issue.year = int(x[0])
                    except:
                        issue.year = None
                        logger.warning("Problem with issue year for %s, %s" % (j_bid, x[0]))
                    issue.issue = x[1]
                elif len(x) == 1:
                    try:
                        issue.year = int(x[0])
                    except:
                        issue.year = None
                        logger.warning("Problem with issue year for %s, %s"%(j_bid,x[0]))
                else:
                    try:
                        issue.year = int(x[0])
                    except:
                        issue.year = None
                        logger.warning("Problem with issue year for %s, %s"%(j_bid,x[0]))
                    issue.issue = "-".join(x[1:])
                # marked as removed
                issue.marked_as_removed = False
                record.issues.append(issue)

    # dump LAST record if not empty
    if not record.is_empty():
        if len(record.issues) > 0:
            record.type2 = "journal"
        record.marked_as_removed = False
        records.append(record)
    logger.info("Dump of previous record done")

    return records

def create_meta_objects(r):
    """
    Creates a Meta Object from a dict record (from parser, exported as json)
    :param r: a record from parser, as a dict
    :return: a Metaobject
    """

    r["created_at"] = datetime.now()
    r["updated_at"] = datetime.now()
    r["title"] = MetadataTitle(**r["title"]) #title
    for x in r["relations"]: #relations
        x["title"] = MetadataTitle(**x["title"])
    r["relations"] = [MetadataRelation(title=x["title"],type=x["type"]) for x in r["relations"]]
    for x in r["issues"]: #issues
        x["imgpagestart"] = ImgPageStart(**x["imgpagestart"])
    r["issues"] = [MetadataIssue(**i) for i in r["issues"]]
    r = Metadata(**r)
    r.validate()
    return r

def import_metadata(records,config_file_name="config.conf",db="mongo_dev"):
    """
    Imports the metadata with an update logic
    :param records: the output of parser()
    :param config_file_name: a valid conf file with connection details
    :param db: a valid database to connect to (header of config)
    :return: None (updates the database)
    """

    logger.info("New import job started")

    config = ConfigParser(allow_no_value=False)
    config.read(config_file_name)
    logger.info('Read configuration file %s' % config_file_name)

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

    objects = Metadata.objects
    for o in objects: #run inital updates, set all to be removed or default
        o.marked_as_removed = True
        if o.type_document == "journal":
            o.provenance = ""
            for l in o.issues:
                l.marked_as_removed = True
                try:
                    l.year = int(l.year)
                except:
                    l.year = None
        o.updated_at = datetime.now()
        o.save()

    # create a dict of merged records to ingest/update
    records_dict = dict()
    for r in records:
        if r.id2 not in records_dict.keys():
            records_dict[r.id2] = r
        else:
            if r.type2 == "journal":
                r.provenance = ""
                for i in r.issues:
                    if i.foldername not in [x.foldername for x in records_dict[r.id2].issues]:
                        records_dict[r.id2].issues.append(i)
                    else:
                        logger.warning("Issue present twice %s, %s"%(r.id2,i.foldername))
            else:
                logger.warning("Book present twice %s" % (r.id2))
                records_dict[r.id2] = r

    # phase 1: UPDATE
    records_updated = list()
    for o in objects:
        # case 1: object exists, update
        if o.bid in records_dict.keys():
            # updates: marked as removed, provenance, imgbib,
            o.marked_as_removed = False
            o.provenance = records_dict[o.bid].provenance
            records_updated.append(o.bid)
            if o.type_document == "journal":
                o.provenance = "" # not relevant for journals
                # update issues
                matched_issues = list()
                for i in o.issues:
                    match = [x for x in records_dict[o.bid].issues if x.foldername == i.foldername]
                    if len(match) > 1:
                        logger.warning("Undefined foldername for issue in Meta %s, %s" % (o.bid, str(match)))
                    elif len(match) == 1:
                        matched_issues.append(match[0].foldername)
                        i.marked_as_removed = False
                        i.provenance = match[0].provenance
                        i.year = match[0].year
                        i.imgpagestart = ImgPageStart(**match[0].export_json_imgpagestart())
                for i in records_dict[o.bid].issues:
                    if i.foldername not in matched_issues:
                        o.issues.append(MetadataIssue(**i.export_json()))
                # sort issues in the object
                o.issues = sorted(o.issues, key=lambda i:i['foldername'], reverse=False)
        o.updated_at = datetime.now()
        o.save()
        logger.info("Saved an update record: %s"%o.bid)

    # phase 2: NEW RECORDS
    meta_records = [create_meta_objects(r.export_json()) for k,r in records_dict.items() if k not in records_updated] # create Meta objects
    for m in meta_records:
        m.save()# insert all
    logger.info("Saved new records: %d" %(len(records_dict.keys())-len(records_updated)))

    # Processing update
    # create preprocessing objects
    processing = dict()
    for k, v in records_dict.items():
        if v.type2 == "monograph":
            processing[k] = {"bid":k, "number":"", "type_document":v.type2,
                                   "foldername":"books/" + v.provenance + "_" + k, "is_digitized":True,
                                   "is_ingested_metadata":True, "created_at":datetime.now(), "updated_at":datetime.now()}
        else:
            for i in v.issues:
                processing[(k,i.foldername)] = {"bid":k, "number":i.foldername,
                                       "foldername":"journals/" + i.provenance + "_" + k + "/" + i.foldername,
                                       "type_document":'issue', "is_digitized":True, "is_ingested_metadata":True, "created_at":datetime.now(), "updated_at":datetime.now()}
    # update existing processing objects
    objects = Processing.objects
    processing_updated = list()
    for o in objects:
        if o.bid in processing.keys():
            o.is_digitized = True
            o.is_ingested_metadata = True
            processing_updated.append(o.bid)
        elif (o.bid,o.number) in processing.keys():
            o.is_digitized = True
            o.is_ingested_metadata = True
            processing_updated.append((o.bid,o.number))
        o.updated_at = datetime.now()
        o.save()
    logger.info("Updated %d processing records"%len(processing_updated))

    # create new processing objects
    processing_records = [Processing(**r) for k, r in processing.items() if
                    k not in processing_updated]  # create objects
    for p in processing_records:
        p.save()# insert all
    logger.info("Saved new processing records: %d" % (len(processing.keys()) - len(processing_updated)))

    # create indexes
    Metadata.create_index("bid", background=True)
    Processing.create_index(["bid", "number"], background=True)

if __name__ == "__main__":

    # change here the database in use. Be careful!!
    xml_folder = "xml_output" # the xml folder, after running xml_parser
    database = "mongo_sand" #"mongo_prod" "mongo_dev" "mongo_sand"
    # uncomment to do your stuff
    records = parser(xml_folder)
    print(len(records))
    import_metadata(records, db=database)