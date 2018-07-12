# coding: utf-8

import redis
import requests
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp


####################
#   API routes     #
####################

API_BASEURI = "http://cdh-dhlabpc6.epfl.ch:8888/api"
AUTHOR_ENDPOINT = "http://cdh-dhlabpc6.epfl.ch:8888/api/authors/%s"
AUTHORS_ENDPOINT = "http://cdh-dhlabpc6.epfl.ch:8888/api/authors/"
ARTICLES_ENDPOINT = "http://cdh-dhlabpc6.epfl.ch:8888/api/articles/"
ARTICLE_ENDPOINT = "http://cdh-dhlabpc6.epfl.ch:8888/api/articles/%s"
BOOKS_ENDPOINT = "http://cdh-dhlabpc6.epfl.ch:8888/api/books/"
BOOK_ENDPOINT = "http://cdh-dhlabpc6.epfl.ch:8888/api/books/%s"
PRIMARY_SOURCE_ENDPOINT = "http://cdh-dhlabpc6.epfl.ch:8888/api/primary_sources/%s/%s"
PRIMARY_SOURCES_ENDPOINT = "http://cdh-dhlabpc6.epfl.ch:8888/api/primary_sources/%s"
REFERENCES_ENDPOINT = "http://cdh-dhlabpc6.epfl.ch:8888/api/references/"
REFERENCE_ENDPOINT = "http://cdh-dhlabpc6.epfl.ch:8888/api/references/%s"

####################
# API wrappers     #
####################

def get_author(author_id):
    try:
        r = requests.get(AUTHOR_ENDPOINT % author_id)
        code = r.status_code
        if code == 404:
            print(r.url, code)
        return ('author', r.url, code)
    except Exception as e:
        return ('author', author_id, "error: %s" % e)

def get_authors(limit=100):
    print("Fetching authors from %s" % AUTHORS_ENDPOINT)
    offset = 0
    response_size = limit
    author_ids = []
    while(response_size==limit):
        #print(offset, limit)
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
            print(r.url, code)
        return ('primary_source', r.url, code)
    except Exception as e:
        print(e)
        return ('primary_source', source_mongoid, "error: %s" % e)

def get_primary_sources(archive, limit=100):
    print("Fetching primary sources from %s" % PRIMARY_SOURCES_ENDPOINT)
    offset = 0
    response_size = limit
    sources_ids = []
    while(response_size==limit):
        print(offset, limit)
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
            print(r.url, code)
        return ('article', r.url, code)
    except Exception as e:
        return ('article', article_id, "error: %s" % e)

def get_articles(limit=100):
    print("Fetching articles from %s" % ARTICLES_ENDPOINT)
    offset = 0
    response_size = limit
    article_ids = []
    while(response_size==limit):
        #print(offset, limit)
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
            print(r.url, code)
        return ('book', r.url, code)
    except Exception as e:
        return ('book', book_id, "error: %s" % e)

def get_books(limit=100):
    print("Fetching books from %s" % BOOKS_ENDPOINT)
    offset = 0
    response_size = limit
    book_ids = []
    while(response_size==limit):
        #print(offset, limit)
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
            print(r.url, code)
        return ('reference', r.url, code)
    except Exception as e:
        return ('reference', reference_id, "error: %s" % e)

def get_references(limit=100):
    print("Fetching references from %s" % REFERENCES_ENDPOINT)
    offset = 0
    response_size = limit
    reference_ids = []
    while(response_size==limit):
        #print(offset, limit)
        r = requests.get(REFERENCES_ENDPOINT, params={'offset':offset, 'limit':limit})
        response_size = len(r.json())
        offset += limit
        reference_ids += [reference["id"] for reference in r.json()]
    return reference_ids

#########################
# caching functions     #
#########################

def cache_authors(cache_prefix):
    print("Pre-caching authors...")
    author_ids = get_authors(limit=500)
    print("There are %i authors to cache" % len(author_ids))
    pool = mp.Pool(processes=8)
    replies = pool.map(get_author, author_ids)
    errors = [reply[1] for reply in replies if type(reply[-1])==type("s") and "error" in reply[-1]]

    if len(errors) > 0:
        print("%i (out of %i) calls have failed: retrying now..." % (len(errors), len(replies)))
        for mongo_id in errors:
            get_author(mongo_id)

    # now let's look directly into the Redis cache
    r = redis.StrictRedis()
    cached_authors = list(r.scan_iter(match="%sAuthor*" % cache_prefix))
    print("%i/%i authors are cached: %i authors missing" % (len(cached_authors), len(author_ids), len(author_ids)-len(cached_authors)))
    return

def cache_primary_sources(cache_prefix):
    print("Pre-caching primary sources...")
    primary_sources_ids = get_primary_sources("asve", limit=1000)
    print("There are %i primary sources to cache" % len(primary_sources_ids))
    pool = mp.Pool(processes=8)
    replies = pool.map(get_primary_source, primary_sources_ids)
    errors = [reply[1] for reply in replies if type(reply[-1])==type("s") and "error" in reply[-1]]
    print("%i (out of %i) calls have failed: retrying now..." % (len(errors), len(replies)))
    
    for mongo_id in errors:
        get_primary_source(mongo_id)

    # now let's look directly into the Redis cache
    r = redis.StrictRedis()
    cached_psources = list(r.scan_iter(match="lb-devPrimarySource*"))
    print("%i/%i primary sources are cached: %i primary sources missing" % (len(cached_psources)
                                                                            , len(primary_sources_ids)
                                                                            , len(primary_sources_ids)-len(cached_psources)))
    return

def cache_articles(cache_prefix):
    print("Pre-caching articles...")
    
    article_ids = get_articles(limit=500)
    print("There are %i articles to cache" % len(article_ids))
    
    pool = mp.Pool(processes=8)
    replies = pool.map(get_article, article_ids)
    errors = [reply[1] for reply in replies if type(reply[-1])==type("s") and "error" in reply[-1]]
    print("%i (out of %i) calls have failed: retrying now..." % (len(errors), len(replies)))
    
    for mongo_id in errors:
        get_article(mongo_id)

    # now let's look directly into the Redis cache
    r = redis.StrictRedis()
    cached_articles = list(r.scan_iter(match="%sArticle*" % cache_prefix))
    print("%i/%i articles are cached: %i articles missing" % (len(cached_articles)
                                                            , len(article_ids)
                                                            , len(article_ids)-len(cached_articles)))
    return

def cache_books(cache_prefix):
    print("Pre-caching books...")
    
    books_ids = get_books(limit=500)
    print("There are %i books to cache" % len(books_ids))
    
    pool = mp.Pool(processes=8)
    replies = pool.map(get_book, books_ids)
    errors = [reply[1] for reply in replies if type(reply[-1])==type("s") and "error" in reply[-1]]
    print("%i (out of %i) calls have failed: retrying now..." % (len(errors), len(replies)))
    
    for mongo_id in errors:
        get_book(mongo_id)

    # now let's look directly into the Redis cache
    r = redis.StrictRedis()
    cached_books = list(r.scan_iter(match="%sBook*" % cache_prefix))
    print("%i/%i books are cached: %i books missing" % (len(cached_books)
                                                            , len(books_ids)
                                                            , len(books_ids)-len(cached_books)))
    return

def cache_references(cache_prefix):
    print("Pre-caching references...")
    
    reference_ids = get_references(limit=1000)
    print("There are %i references to cache" % len(reference_ids))
    
    pool = mp.Pool(processes=8)
    replies = pool.map(get_reference, reference_ids)
    errors = [reply[1] for reply in replies if type(reply[-1])==type("s") and "error" in reply[-1]]
    print("%i (out of %i) calls have failed: retrying now..." % (len(errors), len(replies)))
    
    for mongo_id in errors:
        get_reference(mongo_id)

    # now let's look directly into the Redis cache
    r = redis.StrictRedis()
    cached_references = list(r.scan_iter(match="%sReference*" % cache_prefix))
    print("%i/%i references are cached: %i references missing" % (len(cached_references)
                                                            , len(reference_ids)
                                                            , len(reference_ids)-len(cached_references)))
    return

if __name__ == "__main__":
    cprefix = "lb-dev"
    cache_books(cprefix)
    cache_primary_sources(cprefix)
    cache_articles(cprefix)
    cache_authors(cprefix)
    cache_references(cprefix)

