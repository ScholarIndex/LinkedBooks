#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
Functions for the pre-caching of API replies, with a basic CLI interface.

Usage:
    commons/api_pre_caching.py build --prefix=<p> --port=<n> [--object-type=<t> --log-file=<f>]
    commons/api_pre_caching.py clear --prefix=<p> --object-type=<t> [--log-file=<f>]
    commons/api_pre_caching.py count --prefix=<p>
"""

import logging
from docopt import docopt
import redis
import requests
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp


global PORT, API_BASEURI, AUTHOR_ENDPOINT, AUTHORS_ENDPOINT, ARTICLE_ENDPOINT,\
    ARTICLE_ENDPOINT, BOOKS_ENDPOINT, BOOK_ENDPOINT, PRIMARY_SOURCE_ENDPOINT, \
    PRIMARY_SOURCES_ENDPOINT, REFERENCES_ENDPOINT, REFERENCE_ENDPOINT,\
    STATS_ENDPOINT, CACHE_PREFIXES

CACHE_PREFIXES = {
    'authors' : 'Author',
    'primary_sources' : 'PrimarySource',
    'books' : 'Book',
    'articles' : 'Article',
    'references' : 'Reference',
    'stats' : 'Stats'
}

LOGGER = logging.getLogger(__name__)

####################
# API wrappers     #
####################

def get_author(author_id):
    try:
        r = requests.get(AUTHOR_ENDPOINT % author_id)
        code = r.status_code
        if code == 404:
            LOGGER.debug(r.url, code)
        return ('author', r.url, code)
    except Exception as e:
        return ('author', author_id, "error: %s" % e)

def get_authors(limit=100):
    LOGGER.debug("Fetching authors from %s" % AUTHORS_ENDPOINT)
    offset = 0
    response_size = limit
    author_ids = []
    while(response_size==limit):
        LOGGER.debug("...fetching %i records (starting from %i)" % (limit, offset))
        r = requests.get(AUTHORS_ENDPOINT, params={'offset':offset, 'limit':limit})
        response_size = len(r.json())
        offset += limit
        author_ids += [author['author']["id"] for author in r.json()]
    return author_ids

def get_primary_source(source_mongoid, archive="asve"):
    try:
        r = requests.get(PRIMARY_SOURCE_ENDPOINT % (archive, source_mongoid))
        code = r.status_code
        if code == 404:
            LOGGER.debug(r.url, code)
        return ('primary_source', r.url, code)
    except Exception as e:
        LOGGER.debug(e)
        return ('primary_source', source_mongoid, "error: %s" % e)

def get_primary_sources(archive, limit=100):
    LOGGER.debug("Fetching primary sources from %s" % PRIMARY_SOURCES_ENDPOINT % archive)
    offset = 0
    response_size = limit
    sources_ids = []
    while(response_size==limit):
        LOGGER.debug("...fetching %i records (starting from %i)" % (limit, offset))
        r = requests.get(PRIMARY_SOURCES_ENDPOINT % archive, params={'offset':offset, 'limit':limit})
        response_size = len(r.json())
        offset += limit
        sources_ids += [source["primary_source"]["id"] for source in r.json()]
    return sources_ids

def get_article(article_id):
    try:
        r = requests.get(ARTICLE_ENDPOINT % article_id)
        code = r.status_code
        if code == 404:
            LOGGER.debug(r.url, code)
        return ('article', r.url, code)
    except Exception as e:
        return ('article', article_id, "error: %s" % e)

def get_articles(limit=100):
    LOGGER.debug("Fetching articles from %s" % ARTICLES_ENDPOINT)
    offset = 0
    response_size = limit
    article_ids = []
    while(response_size==limit):
        LOGGER.debug("...fetching %i records (starting from %i)" % (limit, offset))
        r = requests.get(ARTICLES_ENDPOINT, params={'offset':offset, 'limit':limit})
        response_size = len(r.json())
        offset += limit
        article_ids += [article['article']["id"] for article in r.json()]
    return article_ids

def get_book(book_id):
    try:
        r = requests.get(BOOK_ENDPOINT % book_id)
        code = r.status_code
        if code == 404:
            LOGGER.debug(r.url, code)
        return ('book', r.url, code)
    except Exception as e:
        return ('book', book_id, "error: %s" % e)

def get_books(limit=100):
    LOGGER.debug("Fetching books from %s" % BOOKS_ENDPOINT)
    offset = 0
    response_size = limit
    book_ids = []
    while(response_size==limit):
        LOGGER.debug("...fetching %i records (starting from %i)" % (limit, offset))
        r = requests.get(BOOKS_ENDPOINT, params={'offset':offset, 'limit':limit})
        response_size = len(r.json())
        offset += limit
        book_ids += [book['book']["id"] for book in r.json()]
    return book_ids

def get_reference(reference_id):
    try:
        r = requests.get(REFERENCE_ENDPOINT % reference_id)
        code = r.status_code
        if code == 404:
            LOGGER.debug(r.url, code)
        return ('reference', r.url, code)
    except Exception as e:
        return ('reference', reference_id, "error: %s" % e)

def get_references(limit=100):
    LOGGER.debug("Fetching references from %s" % REFERENCES_ENDPOINT)
    offset = 0
    response_size = limit
    reference_ids = []
    while(response_size==limit):
        LOGGER.debug("...fetching %i records (starting from %i)" % (limit, offset))
        r = requests.get(REFERENCES_ENDPOINT, params={'offset':offset, 'limit':limit})
        response_size = len(r.json())
        offset += limit
        reference_ids += [reference["id"] for reference in r.json()]
    return reference_ids

#########################
# caching functions     #
#########################

def cache_authors(cache_prefix):

    LOGGER.debug("Pre-caching authors...")
    author_ids = get_authors(limit=500)
    LOGGER.debug("There are %i authors to cache" % len(author_ids))
    pool = mp.Pool(processes=8)
    replies = pool.map(get_author, author_ids)
    errors = [reply[1] for reply in replies if type(reply[-1])==type("s") and "error" in reply[-1]]

    if len(errors) > 0:
        LOGGER.debug("%i (out of %i) calls have failed: retrying now..." % (len(errors), len(replies)))
        for mongo_id in errors:
            get_author(mongo_id)

    # now let's look directly into the Redis cache
    r = redis.StrictRedis()
    cached_authors = list(r.scan_iter(match="%sAuthor*" % cache_prefix))
    LOGGER.debug("%i/%i authors are cached: %i authors missing" % (len(cached_authors), len(author_ids), len(author_ids)-len(cached_authors)))
    return

def cache_primary_sources(cache_prefix):

    LOGGER.debug("Pre-caching primary sources...")
    primary_sources_ids = get_primary_sources("asve", limit=1000)
    LOGGER.debug("There are %i primary sources to cache" % len(primary_sources_ids))
    pool = mp.Pool(processes=8)
    replies = pool.map(get_primary_source, primary_sources_ids)
    errors = [reply[1] for reply in replies if type(reply[-1])==type("s") and "error" in reply[-1]]
    LOGGER.debug("%i (out of %i) calls have failed: retrying now..." % (len(errors), len(replies)))

    for mongo_id in errors:
        get_primary_source(mongo_id)

    # now let's look directly into the Redis cache
    r = redis.StrictRedis()
    cached_psources = list(r.scan_iter(match="%sPrimarySource*" % cache_prefix))
    LOGGER.debug("%i/%i primary sources are cached: %i primary sources missing" % (len(cached_psources)
                                                                            , len(primary_sources_ids)
                                                                            , len(primary_sources_ids)-len(cached_psources)))
    return

def cache_articles(cache_prefix):

    LOGGER.debug("Pre-caching articles...")

    article_ids = get_articles(limit=500)
    LOGGER.debug("There are %i articles to cache" % len(article_ids))

    pool = mp.Pool(processes=8)
    replies = pool.map(get_article, article_ids)
    errors = [reply[1] for reply in replies if type(reply[-1])==type("s") and "error" in reply[-1]]
    LOGGER.debug("%i (out of %i) calls have failed: retrying now..." % (len(errors), len(replies)))

    for mongo_id in errors:
        get_article(mongo_id)

    # now let's look directly into the Redis cache
    r = redis.StrictRedis()
    cached_articles = list(r.scan_iter(match="%sArticle*" % cache_prefix))
    LOGGER.debug("%i/%i articles are cached: %i articles missing" % (len(cached_articles)
                                                            , len(article_ids)
                                                            , len(article_ids)-len(cached_articles)))
    return

def cache_books(cache_prefix):

    LOGGER.debug("Pre-caching books...")

    books_ids = get_books(limit=500)
    LOGGER.debug("There are %i books to cache" % len(books_ids))

    pool = mp.Pool(processes=8)
    replies = pool.map(get_book, books_ids)
    errors = [reply[1] for reply in replies if type(reply[-1])==type("s") and "error" in reply[-1]]
    LOGGER.debug("%i (out of %i) calls have failed: retrying now..." % (len(errors), len(replies)))

    for mongo_id in errors:
        get_book(mongo_id)

    # now let's look directly into the Redis cache
    r = redis.StrictRedis()
    cached_books = list(r.scan_iter(match="%sBook*" % cache_prefix))
    LOGGER.debug("%i/%i books are cached: %i books missing" % (len(cached_books)
                                                            , len(books_ids)
                                                            , len(books_ids)-len(cached_books)))
    return

def cache_references(cache_prefix):

    LOGGER.debug("Pre-caching references...")

    reference_ids = get_references(limit=1000)
    LOGGER.debug("There are %i references to cache" % len(reference_ids))

    pool = mp.Pool(processes=8)
    replies = pool.map(get_reference, reference_ids)
    errors = [reply[1] for reply in replies if type(reply[-1])==type("s") and "error" in reply[-1]]
    LOGGER.debug("%i (out of %i) calls have failed: retrying now..." % (len(errors), len(replies)))

    for mongo_id in errors:
        get_reference(mongo_id)

    # now let's look directly into the Redis cache
    r = redis.StrictRedis()
    cached_references = list(r.scan_iter(match="%sReference*" % cache_prefix))
    LOGGER.debug("%i/%i references are cached: %i references missing" % (len(cached_references)
                                                            , len(reference_ids)
                                                            , len(reference_ids)-len(cached_references)))
    return

def cache_stats(cache_prefix):

    for key in r.scan_iter(match="%sStats_*" % cache_prefix):
        r.delete(key)

    LOGGER.debug("Pre-caching statistics...")
    r = requests.get(STATS_ENDPOINT)
    LOGGER.debug(r.json())

def clear_cache(cache_prefix, object_type="all", object_prefixes=CACHE_PREFIXES):

    if object_type == "all":
        for object_type in object_prefixes:
            clear_cache(cache_prefix, object_prefixes[object_type])
    else:
        r = redis.StrictRedis()
        object_prefix = object_prefixes[object_type]
        scan_pattern = "%s%s*" % (cache_prefix, object_prefix)
        cached_objects = list(r.scan_iter(match=scan_pattern))

        LOGGER.debug("Found %i pre-cached %s (cache_prefix=%s)..." % (len(cached_objects)
                                                               , object_type
                                                               , cache_prefix
                                                               ))
        LOGGER.debug("Removed %i cached objects" % len([r.delete(key) for key in cached_objects]))


if __name__ == "__main__":
    arguments = docopt(__doc__)

    log_level = logging.DEBUG
    log_file = arguments["--log-file"]
    LOGGER.setLevel(log_level)
    handler = logging.FileHandler(filename=log_file, mode='w') if log_file is not None else logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)

    if arguments["clear"]:

        if arguments["--object-type"] is not None:

            if arguments["--object-type"] in CACHE_PREFIXES or arguments["--object-type"] == "all":
                clear_cache(arguments["--prefix"], arguments["--object-type"])
            else:
                # invalid object-type
                print("\'%s\' is an invalid cached object type! Valid types are: %s" \
                % (arguments["--object-type"], list(CACHE_PREFIXES.keys())))

    elif arguments["build"]:

        PORT = 8080 if arguments["--port"] is None else int(arguments["--port"])
        API_BASEURI = "http://cdh-dhlabpc6.epfl.ch:%i/api" % PORT
        AUTHOR_ENDPOINT = "%s/authors/%s" % (API_BASEURI, "%s")
        AUTHORS_ENDPOINT = "%s/authors/" % API_BASEURI
        ARTICLES_ENDPOINT = "%s/articles/" % API_BASEURI
        ARTICLE_ENDPOINT = "%s/articles/%s" % (API_BASEURI, "%s")
        BOOKS_ENDPOINT = "%s/books/" % API_BASEURI
        BOOK_ENDPOINT = "%s/books/%s" % (API_BASEURI, "%s")
        PRIMARY_SOURCE_ENDPOINT = "%s/primary_sources/%s/%s" % (API_BASEURI, "%s", "%s")
        PRIMARY_SOURCES_ENDPOINT = "%s/primary_sources/%s" % (API_BASEURI, "%s")
        REFERENCES_ENDPOINT = "%s/references/" % API_BASEURI
        REFERENCE_ENDPOINT = "%s/references/%s" % (API_BASEURI, "%s")
        STATS_ENDPOINT = "%s/stats/" % API_BASEURI

        cache_prefix = arguments["--prefix"]

        if arguments["--object-type"] is not None:

            if arguments["--object-type"] in CACHE_PREFIXES:
                if arguments["--object-type"] == "authors":
                    cache_authors(cache_prefix)
                elif arguments["--object-type"] == "articles":
                    cache_articles(cache_prefix)
                elif arguments["--object-type"] == "books":
                    cache_books(cache_prefix)
                elif arguments["--object-type"] == "references":
                    cache_references(cache_prefix)
                elif arguments["--object-type"] == "primary_sources":
                    cache_primary_sources(cache_prefix)
                elif arguments["--object-type"] == "stats":
                    cache_stats(cache_prefix)
            else:
                # invalid object-type
                print("\'%s\' is an invalid cached object type! Valid types are: %s" \
                % (arguments["--object-type"], list(CACHE_PREFIXES.keys())))

        else:
            # cache all
            cache_authors(cache_prefix)
            cache_articles(cache_prefix)
            cache_books(cache_prefix)
            cache_references(cache_prefix)
            cache_primary_sources(cache_prefix)
            cache_stats(cache_prefix)

    elif arguments["count"]:

        r = redis.StrictRedis()
        cache_prefix = arguments["--prefix"]

        for object_type in CACHE_PREFIXES:
            object_prefix = CACHE_PREFIXES[object_type]
            scan_pattern = "%s%s*" % (cache_prefix, object_prefix)
            cached_objects = list(r.scan_iter(match=scan_pattern))
            LOGGER.debug("%s :: %s%s %i" % (object_type, cache_prefix, object_prefix, len(cached_objects)))
