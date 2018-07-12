# -*- coding: utf-8 -*-
"""
Linked Books

Part of annotation_connector.

This script produces jobs given an input config file and an output directory.
A job is a set of folders containing BRAT enabled files, possibly already with annotations, plus a report file to follow the job.
"""
__author__ = """Giovanni Colavizza"""

import os, argparse, logging, codecs, time, datetime, json

# default directories
data_dir = "/Volumes/Linked Books"
out_dir = "output"
test_config = "config_files/test.conf"
logging.basicConfig(filename="logs/exporter.log", level=logging.DEBUG)

# Mongo connectors
from pymongo import MongoClient
con = MongoClient("128.178.21.35")
con.linkedbooks.authenticate('scripty', 'L1nk3dB00ks', source='admin')
db = con.linkedbooks

# supporting functions
def padding(text, number = 4):
    while len(text) < number:
        text = "0"+text
    return text

# main routines
if __name__ == "__main__":

    # parse arguments
    # configuration file, output directory, if to start from scratch (i.e. without annotations from annotations collection) or not.
    parser = argparse.ArgumentParser(description='BRAT jobs exporter for Linked Books.')
    parser.add_argument('-c', dest="conf_file", nargs='?', default="config_files/test.conf",
                   help='valid configuration file.')
    parser.add_argument('-o', dest='out_dir', nargs='?', default="output",
                   help='valid output directory.')
    parser.add_argument('--scratch', dest='from_scratch', action='store_true', default=False,
                   help='if a job should start from scratch, without considering previously stored annotations, if available.')

    args = parser.parse_args()
    if not os.path.isdir(args.out_dir):
        logging.error("Output must be a valid directory.")
        exit()
    if not os.path.isfile(args.conf_file):
        logging.error("Configuration file must be a valid file.")
        exit()

    # parse config file
    collection = db.metadata
    jobs = list()
    with codecs.open(args.conf_file, encoding="utf-8") as f:
        data = f.read()
        # sanity check
        if not "bid" in data.split(",")[0] and "issue" in data.split(",")[1]:
            logging.warning("Malformed configuration file.")
        for line in data.split("\n")[1:]:
            try:
                bid, issue = line.split(",")
            except:
                continue
            bid = bid.strip()
            issue = issue.strip()
            # verify bid
            obj = collection.find_one({"bid": bid})
            if not obj:
                logging.warning("BID %s absent from database." % bid)
                continue
            # verify issues
            found = False
            if len(issue) > 0:
                for i in obj["issues"]:
                    if i["foldername"] == issue:
                        jobs.append((bid, issue))
                        found = True
                if not found:
                    logging.warning("issue %s of BID %s absent from database." % (issue, bid))
                    continue
            else:
                jobs.append((bid,-1))

    #print(jobs)

    # get data
    collection = db.metadata
    collection_docs = db.documents
    collection_pages = db.pages
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    job_output_file = os.path.join(args.out_dir, "TASK-"+st+".csv")
    outputter = open(job_output_file, "w+")
    separator = ";"
    outputter.write('"BID"'+separator+'"issue"'+separator+'"file_number"'+separator+'"status"\n')
    for job in jobs:
        doc = collection.find_one({"bid": job[0]})
        logging.info("Retrieved Document BID %s" % doc["bid"])
        issues = list()
        # case with issue not specified
        if job[1] == -1:
            issues = [x["foldername"] for x in doc["issues"]]
        else:
            issues = [job[1]]
        logging.info("Retrieved issues for Document BID %s, in the number of %d" % (job[0], len(issues)))

        # produce output
        output_base_dir = os.path.join(args.out_dir,job[0])
        for issue in issues:
            output_dir = os.path.join(output_base_dir,issue)
            if not os.path.isdir(output_dir):
                os.makedirs(output_dir)
            # get list of pages
            print({"bid": job[0], "number": issue})
            document = collection_docs.find_one({"bid": job[0], "number": issue})
            if not document:
                logging.warning("Missing issue in Documents: %s, %s" % (job[0], issue))
                continue
            for page in document["pages"]:
                page_data = collection_pages.find_one({"_id": page})
                # create files
                page_n = page_data["filename"].split("-")[-1]
                page_n = page_n.split(".")[0]
                #print(page_n)
                # create files
                # txt
                with open(os.path.join(output_dir, "image-"+page_n+".txt"), 'w+') as f:
                    f.write(page_data["fulltext"])
                # ann
                # TODO: connect to annotations collection and get proper data
                open(os.path.join(output_dir, "image-"+page_n+".ann"), 'a+').close()
                # json
                with codecs.open(os.path.join(output_dir, "image-"+page_n+".json"), 'w+', encoding="utf-8") as f:
                    json.dump({"bid": job[0], "issue": issue, "page_number": page_n, "page_mongo_id": str(page)}, f)
                outputter.write('"'+job[0]+'"'+separator+'"'+issue+'"'+separator+'"'+page_n+'"'+separator+'""\n')
                logging.info("Created page %s for Document BID %s, in issue %s" % (page_n, job[0], issue))

    outputter.close()
    logging.info("DONE with TASK %s" % st)
    print("ALL DONE")