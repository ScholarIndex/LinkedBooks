#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
The tests for the `api.venichescholar_api module`.
"""

__author__ = """Giovanni Colavizza, Matteo Romanello"""

import pdb
import pprint
import logging
from flask import url_for

logger = logging.getLogger(__name__)

def test_stats(client):
    """
    /api/stats/
    """
    url = url_for('api.stats_stats_summary')
    reply = client.get(url)
    logger.debug(pprint.pprint(reply.json))
    return

def test_authors(client):
    """
    /api/authors/
    """
    reply = client.get(url_for('api.authors_author_list'))
    logger.debug(pprint.pprint(reply.json))
    return

def test_author(client):
    """
    /api/authors/<mongoid>
    """
    reply = client.get(url_for('api.authors_author_list'))
    author_ids = [record["author"]["id"] for record in reply.json]
    for author_id in author_ids:
        reply = client.get(url_for('api.authors_author', mongoid=author_id))
    return
