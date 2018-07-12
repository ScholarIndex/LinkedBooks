# -*- coding: utf-8 -*-
"""
Takes two different ground truths and outputs the disagreements for manual check, with basic statistics.
"""
__author__ = """Giovanni Colavizza"""

import codecs, logging, csv, copy
logging.basicConfig(filename="logs/PF.log", level=logging.WARNING)
logger = logging.getLogger(__name__)

def clean_bid(bid):

    if bid.startswith("IT"):
        bid = bid.split("\\")
        bid = "".join(bid[-2:])
    return bid

def process_bid(bids):

    if len(bids) < 10:
        return []
    else:
        if len(bids.split(",")) > 1:
            spl = bids.split(",")
            spl = [clean_bid(x.strip()) for x in spl]
            return spl
        elif len(bids.split()) > 1:
            spl = bids.split()
            spl = [clean_bid(x.strip()) for x in spl]
            return spl
        else:
            return [clean_bid(bids.strip())]


def adjudication_secondary(file_1,file_2):

    #references_1 = dict()
    references_1_byart = dict()
    #references_2 = dict()
    references_2_byart = dict()

    # load files
    with codecs.open(file_1) as f1:
        reader = csv.reader(f1, delimiter=',', quotechar='"')
        next(reader, None)  # skip the headers
        for n,row in enumerate(reader):
            article_id, article_url, article_title, image_number, reference, BID_SBN, BID_LBC, type_pub, linkedbooks_article_id, note = row
            if len(article_id) == 0 or len(article_title) == 0 or len(image_number) == 0 or len(reference) == 0 or len(BID_SBN) == 0 or len(type_pub) == 0:
                logging.warning("Missing data in row: %d"%(n+2))
                continue
            if article_id not in references_1_byart.keys():
                references_1_byart[article_id] = {image_number:process_bid(BID_SBN)}
            else:
                if image_number not in references_1_byart[article_id].keys():
                    references_1_byart[article_id][image_number] = process_bid(BID_SBN)
                else:
                    references_1_byart[article_id][image_number].extend(process_bid(BID_SBN))

    with codecs.open(file_2) as f2:
        reader = csv.reader(f2, delimiter=',', quotechar='"')
        next(reader, None)  # skip the headers
        for n,row in enumerate(reader):
            article_id, article_url, article_title, image_number, reference, BID_SBN, BID_LBC, type_pub, linkedbooks_article_id, note = row
            if len(article_id) == 0 or len(article_title) == 0 or len(image_number) == 0 or len(reference) == 0 or len(BID_SBN) == 0 or len(type_pub) == 0:
                continue
            if article_id not in references_2_byart.keys():
                references_2_byart[article_id] = {image_number:process_bid(BID_SBN)}
            else:
                if image_number not in references_2_byart[article_id].keys():
                    references_2_byart[article_id][image_number] = process_bid(BID_SBN)
                else:
                    references_2_byart[article_id][image_number].extend(process_bid(BID_SBN))

    print(set(references_1_byart.keys()).difference(set(references_2_byart.keys())))
    print(set(references_2_byart.keys()).difference(set(references_1_byart.keys())))
    print(len(set(references_1_byart.keys()).difference(set(references_2_byart.keys()))))
    print(len(set(references_2_byart.keys()).difference(set(references_1_byart.keys()))))
    print(len(set(references_1_byart.keys())))
    print(len(set(references_2_byart.keys())))

    with codecs.open("adj_secondary_full.csv","w",encoding="utf-8") as f:
        writer = csv.writer(f,delimiter=";",quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(["article","img_number","problem","bid"])

        for article in list(set(references_1_byart.keys()).intersection(set(references_2_byart.keys()))):
            for img_number in sorted(set(references_1_byart[article].keys()).union(references_2_byart[article].keys())):
                if img_number not in references_1_byart[article].keys():
                    writer.writerow([article,img_number,"Missing page in file 1",""])
                elif img_number not in references_2_byart[article].keys():
                    writer.writerow([article, img_number, "Missing page in file 2", ""])
                else: # image in both, find mismatched citations
                    r1 = copy.deepcopy(references_1_byart[article][img_number])
                    r2 = copy.deepcopy(references_2_byart[article][img_number])
                    for ref in references_1_byart[article][img_number]:
                        if ref in r2:
                            r1.remove(ref)
                    for ref in references_2_byart[article][img_number]:
                        if ref in r1:
                            r2.remove(ref)
                    for ref in r1:
                        writer.writerow([article, img_number, "Missing reference in file 2", ref])
                    for ref in r2:
                        writer.writerow([article, img_number, "Missing reference in file 1", ref])

def adjudication_primary(file_1,file_2):

    references_1_byart = dict()
    references_2_byart = dict()

    # load files
    with codecs.open(file_1) as f1:
        reader = csv.reader(f1, delimiter=',', quotechar='"')
        next(reader, None)  # skip the headers
        for n,row in enumerate(reader):
            article_id, article_url, article_title, image_number, reference, asve_id, note = row
            if len(article_id) == 0 or len(article_title) == 0 or len(image_number) == 0 or len(reference) == 0 or len(asve_id) == 0:
                logging.warning("Missing data in row: %d"%(n+2))
                continue
            if article_id not in references_1_byart.keys():
                references_1_byart[article_id] = {image_number:process_bid(asve_id)}
            else:
                if image_number not in references_1_byart[article_id].keys():
                    references_1_byart[article_id][image_number] = process_bid(asve_id)
                else:
                    references_1_byart[article_id][image_number].extend(process_bid(asve_id))

    with codecs.open(file_2) as f2:
        reader = csv.reader(f2, delimiter=',', quotechar='"')
        next(reader, None)  # skip the headers
        for n,row in enumerate(reader):
            try:
                article_id, article_url, article_title, image_number, reference, asve_id, note = row
            except:
                print(row)
            if len(article_id) == 0 or len(article_title) == 0 or len(image_number) == 0 or len(reference) == 0 or len(
                    asve_id) == 0:
                continue
            if article_id not in references_2_byart.keys():
                references_2_byart[article_id] = {image_number: process_bid(asve_id)}
            else:
                if image_number not in references_2_byart[article_id].keys():
                    references_2_byart[article_id][image_number] = process_bid(asve_id)
                else:
                    references_2_byart[article_id][image_number].extend(process_bid(asve_id))

    print(set(references_1_byart.keys()).difference(set(references_2_byart.keys())))
    print(set(references_2_byart.keys()).difference(set(references_1_byart.keys())))
    print(len(set(references_1_byart.keys()).difference(set(references_2_byart.keys()))))
    print(len(set(references_2_byart.keys()).difference(set(references_1_byart.keys()))))
    print(len(set(references_1_byart.keys())))
    print(len(set(references_2_byart.keys())))

    with codecs.open("adj_primary_full.csv","w",encoding="utf-8") as f:
        writer = csv.writer(f,delimiter=";",quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(["article","img_number","problem","asve_id"])

        for article in list(set(references_1_byart.keys()).intersection(set(references_2_byart.keys()))):
            for img_number in sorted(set(references_1_byart[article].keys()).union(references_2_byart[article].keys())):
                if img_number not in references_1_byart[article].keys():
                    writer.writerow([article,img_number,"Missing page in file 1",""])
                elif img_number not in references_2_byart[article].keys():
                    writer.writerow([article, img_number, "Missing page in file 2", ""])
                else: # image in both, find mismatched citations
                    r1 = copy.deepcopy(references_1_byart[article][img_number])
                    r2 = copy.deepcopy(references_2_byart[article][img_number])
                    for ref in references_1_byart[article][img_number]:
                        if ref in r2:
                            r1.remove(ref)
                    for ref in references_2_byart[article][img_number]:
                        if ref in r1:
                            r2.remove(ref)
                    for ref in r1:
                        writer.writerow([article, img_number, "Missing reference in file 2", ref])
                    for ref in r2:
                        writer.writerow([article, img_number, "Missing reference in file 1", ref])

if __name__ == "__main__":

    # load
    file_1 = "secondary_full_23052017_1.csv"
    file_1_ps = "primary_full_23052017_1.csv"
    file_2 = "secondary_full_10052017_2.csv"
    file_2_ps = "primary_full_10052017_2.csv"

    #adjudication_secondary(file_1,file_2)
    adjudication_primary(file_1_ps, file_2_ps)