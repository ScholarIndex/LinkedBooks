# -*- coding: utf-8 -*-
"""
Parser that extracts all the tags used in the xml files, and prints them to standard output. To be used only to update the filtering lists in metadata_importer
"""
__author__ = """Giovanni Colavizza"""

import os, codecs, argparse
from collections import defaultdict, OrderedDict
from bs4 import BeautifulSoup

def tags_extractor(in_folder):
    """
    Parses the folder structure of the xml files and prints out all the tags used, with the number of occurrences for each one.

    :param in_folder: Folder pointing to the Linked Books main storage.
    :return: prints the list of unique tags to SO, one per line.
    """

    # basic checks
    assert os.path.isdir(in_folder)

    tags = defaultdict(int)

    for root, dirs, files in os.walk(in_folder):
        for f in files:
            if ".xml" in f:
                # get the tags
                metadata = codecs.open(os.path.join(root, f), "r", "utf-8").read()
                soup = BeautifulSoup(metadata, "html.parser")
                for el in soup.find_all():
                    tags[el.name] += 1

    tags = OrderedDict(sorted(tags.items(),key=lambda x:x[1], reverse=True))
    print("\n".join([k+": "+str(v) for k,v in tags.items()]))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Parser the Linked Books xml metadata files and export the list of tags used.')
    parser.add_argument('--in_folder', dest='in_folder',
                        help='The root folder of the xml files, resulting from xml_parser.')

    args = parser.parse_args()

    tags_extractor(args.in_folder)

"""
relation: 11449
subject: 6799
identifier: 3874
imgpagestart: 3123
provenance: 2663
operator: 2629
scan: 2016
dc: 1939
title: 1939
language: 1939
type: 1938
date: 1920
sub2: 1438
creator: 1326
contributor: 1270
imgindex: 1080
imgbib: 859
coverage: 21
publisher: 18
imgpagestar: 10
sca: 3
imgpagstart: 2
imgibib: 2
description: 2
impagestart: 1
imgib: 1
imgpagastart: 1
imgimdex: 1
operaor: 1
imgppagestart: 1
operatore: 1
imgondex: 1
sub2.: 1
imgpagestaret: 1
imgndex: 1
operatora: 1


# misspells of tags, taken from all_tags below:
img_bibs = ['imgbib','imgib','imgibib']
subjects = ['subject','sub2.','sub2']
operators = ['operatore','operator','operaor','operatora']
img_indexes = ['imgindex','imgondex','imgimdex','imgndex']
scans = ['scan','sca']
img_pg_starts = ['imgpagestart','imgppagestart','impagestart','imgpagstart','imgpagestaret','imgpagestar','imgpagastart']
"""