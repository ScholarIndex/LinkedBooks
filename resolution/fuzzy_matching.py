# -*- coding: utf-8 -*-
"""
Collection of text parsers and string matchers
"""
__author__ = """Giovanni Colavizza"""

import re
import jellyfish
import string
from nltk import stem, tokenize

def normalizer(s, whitespace=True, punctuation=True, lowercase=True, stemandtoken=True):
    '''
    Some basic preprocessing functionalities

    >>> normalizer("Mons d'Estrades Marescial de campo", punctuation=False, stemandtoken=False)
    "mons d'estrades marescial de campo"

    '''
    if punctuation:
        for p in string.punctuation:
            s = s.replace(p, ' ')
    if whitespace:
        s = re.sub(r'[\s]+', ' ', s.strip())
    if lowercase:
        s = s.lower()
    if stemandtoken:
        stemmer = stem.PorterStemmer()
        words = tokenize.wordpunct_tokenize(s)
        s = ' '.join([stemmer.stem(w) for w in words])

    return s

def editDistance(s1, s2, demerau=True, dis=3):
    '''
    Levenshtein = Edit distance, or Hamming distance
    Demerau = the same but accounts for transposition of words as 1 distance, not 2. See: https://en.wikipedia.org/wiki/Damerau–Levenshtein_distance
    '''
    if demerau:
        return jellyfish.damerau_levenshtein_distance(s1, s2) <= dis
    else:
        return jellyfish.levenshtein_distance(s1, s2) <= dis

def fuzzyContainment(string1, string2):
    '''Return an estimate of string containment similarity.
    Made to be robust on parts of strings contained into another.
    '''

    a = b = ""

    if len(string1) == 0 or len(string2) == 0:
        return 0.0

    if len(string1) < len(string2):
        a = string1.split()
        b = string2.split()
    else:
        a = string2.split()
        b = string1.split()

    ratio = 0
    counter = 0
    internal_ratio = 0

    for worda in a:
        if len(worda) < 2:
            continue
        for wordb in b:
            if len(wordb) < 2:
                continue
            r = jellyfish.jaro_distance(worda, wordb)
            if r > internal_ratio:
                internal_ratio = r

        ratio += internal_ratio
        counter += 1
        internal_ratio = 0

    return ratio/max(len(string1.split()),len(string2.split()))

# revised version for metadata lookup, in essence maintains string sequence. Good for titles, not for metadata fields.
def fuzzyContainmentML(string1, string2, threshold=0.95):
    '''Return an estimate of string containment similarity.
    Made to be robust on parts of strings contained into another.
    '''
    # TODO: broken in the internal ration calculation.

    a = b = ""

    if len(string1) == 0 or len(string2) == 0:
        return 0.0

    if len(string1) <= len(string2):
        a = string1.split()
        b = string2.split()
    else:
        a = string2.split()
        b = string1.split()

    ratio = 0
    counter = 0
    internal_ratio = 0

    for worda in a:
        if len(worda) < 2:
            continue
        for n,wordb in enumerate(b):
            if len(wordb) < 2:
                continue
            r = jellyfish.jaro_distance(worda, wordb)
            #if r > threshold:
                #internal_ratio = r
                #b = b[n+1:]
                #break

        ratio += internal_ratio
        counter += 1
        internal_ratio = 0


    return ratio/min(len(string1.split()),len(string2.split()))

# Implementation of subsequence kernel from Lodhi2002. From http://metaoptimize.com/qa/questions/4262/string-kernel-implementation
# Remember to initialize a dictionary for cache.
def SSK(lamb, p):
    cache = dict()
    """Return subsequence kernel"""
    def SSKernel(xi,xj,lamb,p):
        mykey = (xi, xj) if xi>xj else (xj, xi)
        if not mykey in cache:
            dps = []
            for i in range(len(xi)):
                dps.append([lamb**2 if xi[i] == xj[j] else 0 for j in range(len(xj))])
            dp = []
            for i in range(len(xi)+1):
                dp.append([0]*(len(xj)+1))
            k = [0]*(p+1)
            for l in range(2, p + 1):
                for i in range(len(xi)):
                    for j in range(len(xj)):
                        dp[i+1][j+1] = dps[i][j] + lamb * dp[i][j+1] + lamb * dp[i+1][j] - lamb**2 * dp[i][j]
                        if xi[i] == xj[j]:
                            dps[i][j] = lamb**2 * dp[i][j]
                            k[l] = k[l] + dps[i][j]
            cache[mykey] = k[p]
        return cache[mykey]
    return lambda xi, xj: SSKernel(xi,xj,lamb,p)/(SSKernel(xi,xi,lamb,p) * SSKernel(xj,xj,lamb,p))**0.5

# For local alignment detection consider swalign
# Smith-Waterman local alignment configuration
# scoring = swalign.NucleotideScoringMatrix(2, -1)
# sw = swalign.LocalAlignment(scoring, gap_penalty=-2, gap_extension_penalty=-0.5)

if __name__ == '__main__':
    import doctest
    doctest.testmod()




