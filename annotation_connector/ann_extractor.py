# -*- coding: utf-8 -*-
"""
Linked Books

Parser that extracts annotations (i.e. files with annotations) and export a series of pickle objects to be used by patter matching facilities

Exports:
1- lines with citations and without citations for Sup and Unsup extraction of lines with citations
2- annotations and list of tags (for consistency checks)
"""
__author__ = """Giovanni Colavizza"""

import os, codecs, pickle, jellyfish
from collections import defaultdict
from scripts.fs_crawlers import walklevel
from scripts.text_processing import find_all
from collections import OrderedDict
from parsers.data_structures import annotation, page, c_line
import matplotlib.pyplot as plt
#from nltk.tokenize import WordPunctTokenizer
#tokenizer = WordPunctTokenizer()

# constants definition

data_directory = "extraction"
base_dir = "/Users/colavizz/Projects/working_directory/annotations/lb_1"
out_dir = "/Users/colavizz/Projects/working_directory/annotations/extracted_annotations"
logger = codecs.open("parsers/output/ann_extractor_logger.csv", "w", "utf-8")
separator = "&"
separator_train = " "
general_annotations_primary = ['Primary-Partial','Primary-Full']
general_annotations_secondary = ['Secondary-Partial','Secondary-Full']
general_annotations_partial = ['Primary-Partial','Secondary-Partial']
general_annotations_full = ['Primary-Full','Secondary-Full']
general_annotations = ['Primary-Partial','Primary-Full','Secondary-Partial','Secondary-Full']
general_annotations_discard = ['Implicit','Full','Partial']
#specific_annotations = []
# TODO: also consider to MERGE categories, e.g. Editor and Author and Curator!
specific_annotations_discard = ['Other']#, 'Conjunction','TopicDate','Parchment','Chapter','Period','Column','Protocollo','Mazzo','Table','Voce','ArchivalUnit','Citation','Responsible','Box','Website']
citation_to_find = ["senato terra", "senato mar", "giustizia vecchia", "savi alle decime", "notarile", "provveditori al sal", "consiglio dei x", "consiglio x", "avogaria di comun", "notarile testamenti", "notarile, testamenti", "avogaria"]
citations_found = {x: {} for x in citation_to_find}

# TODO: this dump doesn't work, do it in annotation extractor.
#simply dumps an annotation for dedicated matching
def dump_annotation(text, category):
    text = text.replace("\n","")
    f = codecs.open(os.path.join(data_directory, "NOCONTEXT_tags_train"+".txt"), "a", "utf-8")
    t = codecs.open(os.path.join(data_directory, "NOCONTEXT_category_train"+".txt"), "a", "utf-8")
    a = codecs.open(os.path.join(data_directory, "NOCONTEXT_PRIMARY_train"+".txt"), "a", "utf-8")
    b = codecs.open(os.path.join(data_directory, "NOCONTEXT_SECONDARY_train"+".txt"), "a", "utf-8")
    c = codecs.open(os.path.join(data_directory, "NOCONTEXT_PARTIAL_train"+".txt"), "a", "utf-8")
    d = codecs.open(os.path.join(data_directory, "NOCONTEXT_FULL_train"+".txt"), "a", "utf-8")
    for n, word in enumerate(text.split()):
        word = word.replace("\r","")
        if category in general_annotations:
            t.write(word+separator_train+str(n)+separator_train+category+"\n")
            if category in general_annotations_primary:
                a.write(word+separator_train+str(n)+separator_train+category+"\n")
            elif category in general_annotations_secondary:
                b.write(word+separator_train+str(n)+separator_train+category+"\n")
            if category in general_annotations_full:
                d.write(word+separator_train+str(n)+separator_train+category+"\n")
            elif category in general_annotations_partial:
                c.write(word+separator_train+str(n)+separator_train+category+"\n")
        elif not category in specific_annotations_discard and not category in general_annotations_discard:
            f.write(word+separator_train+str(n)+separator_train+category+"\n")
    if not category in specific_annotations_discard and not category in general_annotations_discard and not category in general_annotations:
        f.write("\n")
    f.close()
    if category in general_annotations:
        t.write("\n")
    t.close()
    if category in general_annotations_primary:
        a.write("\n")
    a.close()
    if category in general_annotations_secondary:
        b.write("\n")
    b.close()
    if category in general_annotations_partial:
        c.write("\n")
    c.close()
    if category in general_annotations_full:
        d.write("\n")
    d.close()

def find_citations(text, bid, corpus):
    for c in citation_to_find:
        l = [x for x in find_all(text.lower(), c)]
        if len(l) > 0:
            if corpus in citations_found[c].keys():
                if bid in citations_found[c][corpus].keys():
                    citations_found[c][corpus][bid]["count"] += len(l)
                    citations_found[c][corpus][bid]["list"].extend([text[x-35:x+35] for x in l])
                else:
                    citations_found[c][corpus][bid] = {"count": len(l), "list": [text[x-35:x+35] for x in l]}
            else:
                citations_found[c][corpus] = {bid: {"count": len(l), "list": [text[x-35:x+35] for x in l]}}

def apply_annotations(line, ann_page):
    words = line.text.split()
    start = line.start
    for n, word in enumerate(words):
        line.annotations[n] = {"word": word, "citation_category": "None", "citation_tag": "None", "start": start, "pos_in_line": n, "pos_in_cat": 0}
        start += len(word)+1
    for ann in ann_page:
        #print(ann)
        #print(line.text)
        for n, word in line.annotations.items():
            if word["start"] in range(ann.span[0],ann.span[1]):
                if ann.category in general_annotations:
                    line.annotations[n]["citation_category"] = ann.category
                    matches_in_ann = list()
                    for m, w in enumerate(ann.text.split()):
                        # usually to fix punctuation not taken into account in annotation..
                        if jellyfish.levenshtein_distance(w, word["word"]) < 2:
                            matches_in_ann.append(m)
                    if len(matches_in_ann) == 1:
                        line.annotations[n]["pos_in_cat"] = matches_in_ann[0]
                    else:
                        for m in matches_in_ann:
                            context_minus = min(m, n)
                            context_max = min(len(ann.text.split()), len(words))
                            if ann.text.split()[m-context_minus:m+context_max] == words[n-context_minus:n+context_max]:
                                line.annotations[n]["pos_in_cat"] = m
                                break
                elif ann.category in general_annotations_discard or ann.category in specific_annotations_discard:
                    continue
                else:
                    line.annotations[n]["citation_tag"] = ann.category
    return line

# lines printer (to review)
def print_lines(lines, out_file, separator="&"):

    with codecs.open(out_file, "w", "utf-8") as f:
        for item in lines:
            for row in item[4].values():
                out = str(item[0])+separator+str(item[1])+separator+str(item[2])+separator+str(row["ann"])+separator+str(row["txt"])+"\n"
                f.write(out)

def main():

    # data structures
    annotations_store = list()
    lines_store = list()
    annotation_tags = set()
    previous_page = page()
    current_page = page()
    previous_ann_page = OrderedDict()
    current_ann_page = OrderedDict()
    continuations = list()
    annotations_by_year = defaultdict(int)
    ann_counter = 0

    # parse corpus
    for root, dirs, files in walklevel(base_dir, 2):
        for file in files:
            if ".ann" in file:
                ann_file = file
                txt_file = file.replace(".ann",".txt")
                corpus = root.split("/")[-2]
                bid = root.split("/")[-1]
                page_nr = int(file.split(".")[-2].split("_")[-1])
                try:
                    year = int(bid[:4])
                except:
                    year = 0
                full_text = codecs.open(os.path.join(root, txt_file), "r", "utf-8").read()
                find_citations(full_text, year, corpus)
                annotations = codecs.open(os.path.join(root, ann_file), "r", "utf-8").read()
                if not os.path.isfile(os.path.join(root, txt_file)):
                    logger.write(file+separator+"missing TXT file\n")
                    continue
                if not os.path.getsize(os.path.join(root, ann_file)) > 0:
                    continue
                annotations_by_year[year] += 1
                #print("Parsing "+corpus+" - "+bid+" - "+file)

                # get and store list of files with annotations (for each folder, BID)
                annotation_spans = list()
                previous_ann_page = current_ann_page
                current_ann_page = OrderedDict()
                hasContinuation = False

                for n, row in enumerate(annotations.split("\n")):
                    data = row.split("\t")
                    if len(data) > 1:
                        #if ann_file == "1998_15117.04.201518-19-26_page_35.ann":
                        #    print(data)
                        type = data[0][:1]
                        if type == "A" or type == "R":
                            if "Continuation" in data[1]:
                                continuations.append(data[1].split()[1])
                                hasContinuation = True
                            continue
                        id = data[0]
                        category = ""
                        span = ""
                        text = ""
                        if len(data) == 3 and len(data[2]) > 0:
                            category = data[1].split()[0]
                            span = " ".join(data[1].split()[1:]).strip()
                            span = (int(span.split()[0]), int(span.split()[-1]))
                            if category in general_annotations:
                                ann_counter += 1
                                annotation_spans.append(span)
                            text = data[2]
                        else:
                            text = data[1]
                        if len(category) > 0:
                            annotation_tags.add(category)
                        #if ann_file == "1998_15117.04.201518-19-26_page_35.ann":
                        #    print(category +" "+text+" "+str(span))
                        current_ann_page[n] = annotation(type, id, bid, corpus, txt_file, category, span, text)
                        dump_annotation(text, category)

                # TODO: if needed expand on the representation of annotations with hierarchy and link to continuations
                # sort and make a hierarchy of annotations

                # merge continuations

                # change and see if we need to process previous and last pages.
                for ann in current_ann_page.values():
                    annotations_store.append(ann)

                # process each corresponding text file
                previous_page = current_page
                current_page = page(bid, corpus, txt_file, full_text, page_nr, year, hasContinuation)
                row_spans = [x for x in find_all(full_text, "\n")]
                row_text = full_text.split("\n")
                assert len(row_spans) == (len(row_text)-1)
                pred = 0
                keys = list()
                for n, end in enumerate(row_spans):
                    keys.append((pred, n))
                    current_line = c_line(pred, end, n, row_text[n], False)
                    assert full_text[pred:end] == row_text[n]
                    current_page.addLine(current_line, n)
                    pred = end+1

                annotation_spans = sorted(annotation_spans, key= lambda t: t[0])
                keys = sorted(keys, key= lambda t: t[0])

                for span in annotation_spans:
                    key = max([x for x in keys if x[0] <= span[0]])
                    key_pos = keys.index(key)
                    while key[0] <= span[1]:
                        current_page.lines[key[1]].hasAnnotation = True
                        key_pos += 1
                        if key_pos <= len(keys)-1:
                            key = keys[key_pos]
                        else:
                            break

                # add annotations to the lines of the page
                for n, line in current_page.lines.items():
                    annotations = list()
                    for ann in current_ann_page.values():
                        if ann.span:
                            if line.start <= ann.span[0] or ann.span[1] < line.end or (ann.span[0] < line.start < line.end < ann.span[1]):
                                annotations.append(ann)
                    #if current_page.filename == "1979_11217.04.201518-19-26_page_80.txt":
                    #    for ann in annotations:
                    #        print(ann.category + " " + ann.text + " " + str(ann.span))
                    #if current_page.filename == "1998_15117.04.201518-19-26_page_35.txt" and line.hasAnnotation:
                    #    print(line.text)
                    current_page.lines[n] = apply_annotations(line, annotations)
                    #if current_page.filename == "1998_15117.04.201518-19-26_page_35.txt" and line.hasAnnotation:
                    #    print(line.annotations)

                # output, for each txt_file with annotations!
                lines_store.append(current_page)

    # store all data structures

                logger.write(ann_file+separator+"extracted\n")
                #print("Done "+corpus+" - "+bid+" - "+file)


    pickle.dump(annotations_store, open("parsers/output/annotations.p", "wb"))
    pickle.dump(annotation_tags, open("parsers/output/annotation_tags.p", "wb"))
    pickle.dump(lines_store, open("parsers/output/lines_store.p", "wb"))
    #print_lines(lines_store, "./output/lines.csv", separator)
    #print(annotations_store[17].text)
    print(annotation_tags)
    print(len(lines_store))
    print(ann_counter)
    print(len(continuations))
    for year, n in annotations_by_year.items():
        print(str(year) + ": " + str(n))
    #for line in lines_store[17].lines.values():
    #    print(line.annotations)
    logger.close()

if __name__ == "__main__":
    main()
    print("DONE!!")
    #print(citations_found)
    figsize = (15,10)
    year_start = 1960
    for series, data in citations_found.items():
        plt.figure(figsize=figsize)
        for corpus, years in data.items():
            data2 = sorted([(year, val["count"]) for year,val in years.items() if year > year_start], key=lambda t:t[0])
            plt.plot([point[0] for point in data2], [point[1] for point in data2], label=corpus)
        plt.legend(loc='best')
        title = series
        plt.title(title)
        plt.savefig("parsers/plots/"+title+'.pdf')