# -*- coding: utf-8 -*-
"""
Linked Books

Container of data structures for extractors and parsers.
"""
__author__ = """Giovanni Colavizza"""

from collections import OrderedDict

# class to store info about an annotation
class annotation():
    def __init__(self, type="T", id=[], bid="", corpus="", filename="", category="", span=(0,0), text="", isRoot=False):
        self.type = type
        self.id = id
        self.bid = bid
        self.corpus = corpus
        self.filename = filename
        self.category = category
        self.text = text
        #self.span = self.process_span(span)
        self.span = span

    # TODO: implement some meaningful solution here
    def process_span(self, span):
        if self.type == "T":
            span = [y for x in span.split(";") for y in x.split()]
            if len(span) > 1:
                span = (int(span[0]), int(span[-1]))
            else:
                span = (span[0],span[0])
            return span
        else:
            return (0,0)

    def add_component(self, annotation):
        self.n_components += 1
        self.components[self.n_components] = annotation

class c_line():
    def __init__(self, start=0, end=0, number=0, text="", hasAnnotation=False):
        self.start = start
        self.end = end
        self.number = number
        self.text = text
        self.hasAnnotation = hasAnnotation
        self.annotations = dict()

class page():
    def __init__(self, bid="", corpus="", filename="", text="", page_nr=0, year=0, hasContinuation=False):
        self.bid = bid
        self.corpus = corpus
        self.filename = filename
        self.text = text
        self.page_nr = page_nr
        self.year = year
        self.hasContinuation = hasContinuation
        self.lines = dict()

    def addLine(self, line, n):
        self.lines[n] = line

class group():
    def __init__(self, primary=False, secondary=False, partial=False, full=False):
        self.primary = primary
        self.secondary = secondary
        self.partial = partial
        self.full = full
        self.annotations = OrderedDict()
        self.num_lines = 0

    def addAnnotation(self, filename, annotation):
        self.num_lines += 1
        self.annotations[self.num_lines] = (filename, annotation)