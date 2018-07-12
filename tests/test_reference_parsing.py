#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
The tests for the `commons.dbmodels`.
"""

__author__ = """Giovanni Colavizza, Matteo Romanello"""

from pytest import mark
from reference_parsing.reference_parsing import loader, parser, ingester

@mark.run(order=8)
def test_reference_parsing(test_db):

    # Load data to parse
    data = loader(test_db)

    # Parse
    data = parser(data)

    # Dump parsed data in references
    if len(data)>0:
        ingester(test_db, data)
