# -*- coding: utf-8 -*-
"""
Supporting functions for metadata_importer
"""
__author__ = """Giovanni Colavizza"""

import os

def walklevel(some_dir, level=1):
    """
    Walks a series of nested folders returning the nerting level as well as the root, dirs and files, as in os.walk.
    :param some_dir: a dir to start the walk
    :param level: how many levels down to go
    :return: Iterator returning root, dirs, files, current_level for every level
    """
    some_dir = some_dir.rstrip(os.path.sep)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        num_sep_this = root.count(os.path.sep)
        current_level = num_sep_this - num_sep + 1
        yield root, dirs, files, current_level
        if num_sep + level <= num_sep_this:
            del dirs[:]

def find_all(a_str, sub):
    """
    Finds all occurrences of a string in a text
    Example: list(find_all('spam spam spam spam', 'spam')) # [0, 5, 10, 15]
    :param a_str: a string
    :param sub: a string
    :return: the offsets where all occurrences of sub in a_str start
    """
    start = 0
    while True:
        start = a_str.find(sub, start)
        if start == -1: return
        yield start
        start += len(sub) # use start += 1 to find overlapping matches

def sanitize_strings(txt):
    """
    Removes newlines from a piece of text
    :param txt: a string.
    :return: a string without new lines.
    """
    if not txt:
        return ""
    if len(txt) > 0:
        return "".join(txt.splitlines())
    else:
        return ""