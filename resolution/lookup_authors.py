#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
Functions for the disamiguation of author names.
"""

import logging
logger = logging.getLogger(__name__)
import requests
from commons.dbmodels import Author
import jellyfish, codecs, csv
from collections import defaultdict
from .fuzzy_matching import fuzzyContainment
from mongoengine.errors import NotUniqueError

def clean_for_viaf(text):
    """
    assumes cleaned author form (see supporting_functions.clean_authors). Removes abbreviations and punctuation
    :param text: input
    :return: cleaned string
    """

    text = text.strip().lower()
    text = text.replace(".","")
    text = text.replace('"',"")
    text = text.split()
    new_text = ""
    for t in text:
        if len(t) > 1:
            new_text += t+" "
    return new_text.strip()

def clean_authors(author):
    """
    Consolidates author names and surnames to bring the surname forward
    :param author: String with author name
    :return: String with cleaned author name
    """

    final_author_string = ""

    # cleaning
    # TODO: ev. do this more systematically with regex
    if "(a cura di)" in author:
        author = author.replace("(a cura di)","").strip()
    if "a cura di" in author:
        author = author.replace("a cura di","").strip()
    if "<omonimi non identificati>" in author:
        author = author.replace("<omonimi non identificati>","").strip()
    # split multiple authors
    if "- " in author:
        authors = author.split("- ")
    elif "," in author:
        authors = author.split(",")
    elif " e " in author:
        authors = author.split(" e ")
    elif " ed " in author:
        authors = author.split(" ed ")
    else:
        authors = [author]

    #print(authors)
    for a in authors:
        if len(a.strip()) == 0:
            continue
        a = a.strip()
        a = a.split(".")
        if len(a) > 1:
            surname = a[-1].strip()
            name = [x.strip() for x in a[:-1]]
            final_author_string += surname+", "+" ".join([x+"." for x in name])
        else:
            a = a[0].split()
            final_author_string += a[-1]+", "+" ".join(a[:-1])
        final_author_string += " - "
    final_author_string = final_author_string[:-3]

    return final_author_string

def compare_authors(a1,a2,threshold=0.94):
    """
    Compare two surface forms and returns a T-F response
    :param a1: string, author 1, formatted as surname, name
    :param a2: string, author 2, formatted as surname, name
    :param threshold: threshold for the jaro distance
    :return: True or False
    """

    # first, if the two authors have no abbreviations, use fuzzy matching respecting order
    if "." not in a1 and "." not in a2:
        if fuzzyContainment(clean_for_viaf(a1),clean_for_viaf(a2)) > threshold:
            return True
    else: # there are abbreviations, compare with a different method
        # strip and lower
        a1 = a1.strip().lower()
        a2 = a2.strip().lower()
        # first, compare surnames, if they don't match, stop
        try:
            surname1, name1 = a1.split(", ")
            surname2, name2 = a2.split(", ")
        except:
            # TODO: properly manage authors without a name
            surname1 = a1.split(", ")[0]
            surname2 = a2.split(", ")[0]
            if jellyfish.jaro_distance(surname1, surname2) > threshold:
                return True
            else:
                return False
        if jellyfish.jaro_distance(surname1,surname2) < threshold:
            return False
        # find matches to all components of the shorter name
        return token_comparison(name1, name2)

    return False

def token_comparison(name1,name2,threshold=0.94):
    """
    Compares the names of two authors, trying to find matches to situations like "Edward E." and "E. E."
    :param name1: first name
    :param name2: second name
    :param threshold: string matching threshold
    :return: True or False
    """

    name1 = name1.split()
    name2 = name2.split()
    if len(name1) <= len(name2):
        needed_matches = len(name1)
        found_matches = 0
        for n1 in name1:
            cycle_done = False
            if "." not in n1:
                for n2 in name2:
                    if jellyfish.jaro_distance(n1, n2) >= threshold:  # first try to find a full match
                        found_matches += 1
                        cycle_done = True
                        break
                if not cycle_done:
                    for n2 in name2:
                        if "." in n2 and n2.startswith(n1[0]):  # then try to find an abbreviation
                            found_matches += 1
                            break
            else:  # this token is already an abbreviation, try to find a full match first
                for n2 in name2:
                    if "." not in n2 and n2.startswith(n1[0]):  # then try to find a full match
                        found_matches += 1
                        cycle_done = True
                        break
                if not cycle_done:
                    for n2 in name2:
                        if "." in n2 and n2.startswith(n1[0]):  # then try to find an abbreviation
                            found_matches += 1
                            break
        if found_matches == needed_matches:
            return True
    else:
        needed_matches = len(name2)
        found_matches = 0
        for n1 in name2:
            cycle_done = False
            if "." not in n1:
                for n2 in name1:
                    if jellyfish.jaro_distance(n1, n2) >= threshold:  # first try to find a full match
                        found_matches += 1
                        cycle_done = True
                        break
                if not cycle_done:
                    for n2 in name1:
                        if "." in n2 and n2.startswith(n1[0]):  # then try to find an abbreviation
                            found_matches += 1
                            break
            else:  # this token is already an abbreviation, try to find a full match first
                for n2 in name2:
                    if "." not in n2 and n2.startswith(n1[0]):  # then try to find a full match
                        found_matches += 1
                        cycle_done = True
                        break
                if not cycle_done:
                    for n2 in name2:
                        if "." in n2 and n2.startswith(n1[0]):  # then try to find an abbreviation
                            found_matches += 1
                            break
        if found_matches == needed_matches:
            return True
    return False

def lookup_author_mongo(name):
    """
    Disambiguates the author name (in input) by looking it up against the MongoDB.

    :param name: name to disambiguate in the form "Surname, Name"
    :param db_conn: a connection to the MongoDB (containing the `bibliodb_authors` collection)
    :return: The first match as dictionary or None if not match is found

    >>> from resolution.lookup_authors import lookup_author_mongo
    >>> from commons import mongo
    >>> test = u"Infelise, Mario" # must be a Unicode string
    >>> db = mongo.get_mongodb_development()
    >>> lookup_author_mongo(test, db)

    """
    surname = name.split(",")[0]
    candidates = Author.objects.search_text(surname)
    matches = {}
    for candidate in candidates:
        logger.debug("Comparing %s with %s" % (name, candidate["author_final_form"]))
        comparison = compare_authors(candidate["author_final_form"],name)
        if(comparison == True):
            matches[candidate.id] = candidate
    if(len(list(matches.values()))>=1):
        return list(matches.values())[0]
    else:
        return None

def viaf_lookup(author, conservative=False, clean_string=True, recompose_surnames=True, max_results=1):
    """
    Look up to the VIAF endpoint: http://www.viaf.org/viaf/AutoSuggest?query=rosa,&sortKeys=holdingscount
    See for documentation: https://www.oclc.org/developer/develop/web-services/viaf/authority-cluster.en.html
    Page of an ID: http://viaf.org/viaf/23594707

    :param author: author to search for
    :param conservative: if to search only for surnames if this is the only option possible, or not
    :param clean_string: wether to clean the string with a function (currently only clean_for_viaf)
    :param recompose_surnames: if to try to recompose surnames in case of no match
    :parm max_results: the number of matches to return
    :return: If max_results == 1 returns a tuple where [0] is a string with viaf ID or an empty string; and [1] is a string with VIAF term
        or an empty string. If max_results > 1 returns a list of tuples with the same structure
    """

    viaf_url = "http://www.viaf.org/viaf/AutoSuggest"
    payload = {"query":"","sortKeys":"holdingscount"}
    if clean_string:
        author = clean_for_viaf(author)
        if len(author.split(",")) == 1 and conservative:
            print("Only surname: "+author)
            return "",""
        payload["query"] = author
    else:
        payload["query"] = author

    r = requests.get(viaf_url, params=payload)
    if r.status_code == requests.codes.ok:
        viaf_data = r.json()
        # if empty
        if not viaf_data["result"]:
            if recompose_surnames and len(author.split(",")) > 1:
                author = author.split()
                new_author = author[-1]
                new_author += " "+" ".join(author[:-1])
                return viaf_lookup(new_author
                                   , conservative
                                   , clean_string
                                   , max_results=max_results
                                   , recompose_surnames = False)
            else:
                print("No result: " + author)
                return "", ""

        if not viaf_data["result"][0]["nametype"] == "personal":
            print("Not a personal name: "+author+" "+viaf_data["result"][0]["displayForm"])
            return "",""
        result = [(entry["displayForm"],entry["viafid"]) for entry in viaf_data["result"]]
        #viaf_term = viaf_data["result"][0]["displayForm"]
        #viaf_id = viaf_data["result"][0]["viafid"]
        if(max_results == 1):
            return result[0]
        else:
            return result[:max_results]
    else:
        print("error: "+r.status_code)
        return "",""
