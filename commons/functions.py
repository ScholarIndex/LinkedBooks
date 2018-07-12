#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: Matteo Romanello, matteo.romanello@epfl.ch
# author: Giovanni Colavizza, giovanni.colavizza@epfl.ch

def split_and_tokenise_standoff(text, _len=len):
    """
    Takes a text in input and returns a list of tuples with surface, start and end offsets for each token.
    From: http://stackoverflow.com/questions/9518806/how-to-split-a-string-on-whitespace-and-retain-offsets-and-lengths-of-words
    :param text: a string of text
    :param _len: the way offsets are calculated, default using len of token
    :return: list of tuples with surface, start and end offsets for each token plus their position in the line and the line number
    """
    lines = text.split("\n")
    index = text.index
    offsets = []
    append = offsets.append
    running_offset = 0
    for l, line in enumerate(lines):
        words = line.split()
        for n,word in enumerate(words):
            word_offset = index(word, running_offset)
            word_len = _len(word)
            running_offset = word_offset + word_len
            append((word, word_offset, running_offset - 1, n+1, l+1))
    return offsets