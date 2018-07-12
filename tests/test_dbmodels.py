#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
The tests for the `commons.dbmodels`.
"""

__author__ = """Giovanni Colavizza, Matteo Romanello"""

import pdb
import logging
from pytest import mark
from commons.dbmodels import *

logger = logging.getLogger(__name__)

@mark.run(order=-1)
def test_model_metadata(mongoengine_connection):
    objects = Metadata.objects
    logger.info("The database contains %s Metadata objects"%len(objects))
    logger.info("Printing the first 5 objects")
    for n, obj in enumerate(objects[:5]):
        logger.info("(%i) %s"%(n+1, repr(obj))) 

def test_model_document(mongoengine_connection):
    objects = LBDocument.objects
    logger.info("The database contains %s Document objects"%len(objects))
    logger.info("Printing the first 5 objects")
    for n, obj in enumerate(objects[:5]):
        logger.info("(%i) %s"%(n+1, repr(obj))) 

def test_model_page(mongoengine_connection):
    objects = Page.objects()[:5]
    logger.info("The database contains %s Page objects"%Page.objects.count())
    logger.info("Printing the first 5 objects")
    logger.info(objects)
    for n, obj in enumerate(objects[:5]):
        logger.info("(%i) %s"%(n+1, repr(obj))) 

def test_model_token(mongoengine_connection):
    page = Page.objects[0]
    logger.info("Page %s contains %i tokens"%(page.id, len([tok for line in page.lines
                                                                                      for tok in line.tokens])))
    logger.info("Printing all tokens")
    for line in page.lines:
        for token in line.tokens:
            logger.info(repr(token))

def test_model_annotation(mongoengine_connection):
    objects = Annotation.objects
    logger.info("The database contains %d Annotation objects"%len(objects))

def test_model_reference(mongoengine_connection):
    objects = Reference.objects
    limit = 10
    logger.info("The database contains %d Reference objects"%len(objects))
    logger.info("Validating the first %i references" % limit)
    [reference.validate() for reference in Reference.objects[:limit]]
