from flask_restplus import Namespace, Resource
from flask import current_app
from .cache import cache
from .parsers import europeana_req_parser, SUGGESTION_ENTITIES
from .models import europeana_suggestions, europeana_result, keywords_with_tfidf
import math
import requests
import re
import string
import collections
from nltk.corpus import stopwords

import logging
logger = logging.getLogger(__name__)

AUTHOR_WEIGHT = 2.4 # 2.4 is completely arbitrary
MAX_KEYWORDS = 5

EDM_API = "https://www.europeana.eu/api/v2/%s"
EDM_SEARCH_API = EDM_API % 'search.json'
EDM_TRANSLATE_API = EDM_API % 'translateQuery.json'
EDM_API_KEY = 'xbLwLxpy3'

SOLR_TF_FIELD = 'title'
DATE = re.compile('1?[0-9]{3}(-1?[0-9]{3})?')
DF_CHUNK_SIZE = 500
TRANSLATOR = str.maketrans('', '', string.punctuation)
MUTUALLY_EXCLUSIVE_PARAMETERS = [
    'author_id',
    'book_id',
    'article_id',
    'keyword',
    'cursor',
]

api = Namespace('europeana', description='Integration of Europeana\'s API')
api.models[europeana_suggestions.name] = europeana_suggestions
api.models[europeana_result.name] = europeana_result
api.models[keywords_with_tfidf.name] = keywords_with_tfidf

def get_api_data(resource_type, resource_id):
    '''
    Fetch API's data using an HTTP request to leverage cache and returns the
    author name the list of titles (or a list with the title of the
    book/article).
    '''
    author_names = []
    titles = []

    try:
        current_app.logger.info(
            "Getting Europeana suggestions for {} with id {}".format(
                resource_type,
                resource_id
            )
        )
        api_base_url = current_app.config['API_BASE_URL']
        api_endpoint = "%s/%ss/%s" % (api_base_url, resource_type, "%s")
        resource_url = api_endpoint % resource_id

        current_app.logger.info(resource_url)
        response = requests.get(resource_url).json()

        # concatenate all titles of various types of related publications
        titles = [
            publication.get('title', '')
            for pl in ['publications', 'cited', 'citing']
            for pt in ['articles', 'books']
            for publication in response.get(pl, {}).get(pt, [])
        ]
        current_app.logger.info("Titles: {}".format(titles))
        # if resource is book/article, add also its title
        if resource_type != 'author':
            titles.append(response[resource_type]['title'])

        if resource_type == 'author':
            author_names = [
                response['author']['name']
            ]
        else:
            if response[resource_type]['author'] is not None:
                author_names = [
                    author['name']
                    for author in response[resource_type]['author']
                ]
    except Exception as e:
        # raise an exception oustide of the try/except
        raise

    if not author_names and not titles:
        api.abort(500, "Unable to get data from %s" % (resource_url))

    return (author_names, titles)


def cleanup_name(name):
    '''
    Remove the date or date range sometimes poluting an author's name.
    '''
    return DATE.sub('', name).replace(',', '').replace('-', '').strip()


def select_keywords(keywords_with_tfidf):
    '''
    Select up to MAX_KEYWORDS keywords

    If possible only keywords with tf > 1 and df > tf are used,
    then sorted by tfidf and then tf.

    There is sometimes a df < tf probably because of the difference between
    what the api and solr considers a word, so we remove them to avoid
    tf.idf scores above 1
    '''

    appearing_more_than_once = []
    rest = []

    for item in keywords_with_tfidf:
        [keyword, tf, df, idf] = item
        if tf > 1 and df >= tf:
            appearing_more_than_once.append(item)
        else:
            rest.append(item)

    by_tfidf_then_tf = sorted(
        appearing_more_than_once,
        key=lambda item: (item[3], item[1]),
        reverse=True,
    )

    if len(by_tfidf_then_tf) < MAX_KEYWORDS:
        by_tfidf_then_tf += sorted(
            rest,
            key=lambda item: (item[3], item[1]),
            reverse=True,
        )

    return [{
        'keyword': keyword,
        'tf': tf,
        'df': df,
        'tfidf': tfidf,
    } for keyword, tf, df, tfidf in by_tfidf_then_tf[:MAX_KEYWORDS]]


def get_query(author_query, keyword_query):
    '''
    Finalize the query.

    Join author_query and keyword_query if necessary, then use Europeana's
    translation API to translate terms in other languages if possible.
    '''
    # TODO: should catch empty author query
    conjunction = ' OR ' if author_query != '' and keyword_query != '' else ''
    query = author_query + conjunction + keyword_query
    translation = requests.get(
        EDM_TRANSLATE_API,
        params={
            'wskey': EDM_API_KEY,
            'term': query,
            'sort': 'has_thumbnails',
            'languageCodes': ['it', 'en', 'de', 'fr', 'es']
        },
    ).json()
    translated_query = translation['translatedQuery']
    current_app.logger.info(translated_query)
    return (translated_query)


def get_dfs(terms):
    '''
    Query solr to get the document frequency of a list of terms.
    '''
    dfs = {}
    # we need to split the request to make sure it's not too big
    # (in terms of HTTP request)
    chunks = (
        terms[i:i + DF_CHUNK_SIZE]
        for i in range(0, len(terms), DF_CHUNK_SIZE)
    )
    for chunk in chunks:
        response = requests.get(
            current_app.config['SOLR_TERM_COMPONENT'],
            params={
                'wt': 'json',
                'terms.fl': SOLR_TF_FIELD,
                'terms.regex.flag': 'case_insensitive',
                'terms.regex': '|'.join(chunk), #[re.escape(t) for t in chunk] ?
                'terms.limit': len(chunk), #otherwise the default of 10 is used
            }
        )
        current_app.logger.info(response.request.path_url)
        flat_dfs = response.json().get('terms', {}).get(SOLR_TF_FIELD, {})
        for k, n in zip(flat_dfs[::2], flat_dfs[1::2]):
            dfs[k] = n

    if len(terms) != len(dfs):
        current_app.logger.warn(
            "Got only %d df's for %d terms" % (len(dfs), len(terms))
        )

    return dfs


def get_tfidf(tokens):
    '''
    Compute tf, df, and tfidf scores of each token.
    '''

    #TODO make more similar to solr's preprocessing
    initial_tokens = [token.lower().translate(TRANSLATOR) for token in tokens]
    tokens = [
        token
        for token in initial_tokens
        if not token.isdigit()
    ]

    tokens = [
        token
        for token in tokens
        if token and token not in stopwords.words('italian') +
        stopwords.words('french') +
        stopwords.words('german') +
        stopwords.words('english')
    ]

    dfs = get_dfs(tokens)

    # TODO: change how TF-IDF is computed (normalized on doc length + log)
    tfs = collections.Counter(tokens)
    tfidfs = []
    for token, tf in tfs.items():
        df = dfs.get(token, 0)
        norm_tf = tf / len(initial_tokens)
        norm_df = math.log(87721 / df) if df else 0
        # tfidfs.append((token, tf, df, tf / df if df else 0))
        tfidfs.append((token, tf, df, norm_tf * norm_df))

    return tfidfs


def run_query(query, cursor):
    '''
    Query Europeana's API.
    '''
    current_app.logger.info("Running query {}".format(query))
    response = requests.get(
        EDM_SEARCH_API,
        params={
            'wskey': EDM_API_KEY,
            'query': query,
            'cursor': cursor if cursor else '*',
        },
    ).json()

    return response


def parse_response(items):
    '''
    Extract useful data from Europeana's response.
    '''

    formatted_items = [{
        'title': item.get('title', [None])[0],
        'thumbnail': item.get('edmPreview', [None])[0],
        'direct_url': item.get('edmIsShownAt', [None])[0],
        'europeana_url': item.get('guid', None),
        'provider': item.get('provider', [None])[0],
        'type': item.get('type', None),
        'licence': item.get('rights', [None])[0],
        'year': item.get('year', [None])[0],
        'score': item.get('score', [None]),
        'lang': item.get('language', [None])[0],
    } for item in items]

    return [
        {
            k: v
            for k, v in item.items() if v is not None
        }
        for item in formatted_items
    ]


def get_cache_key(*args, **kwargs):
    '''
    Compute a cache key using the query's arguments.
    '''
    return 'EuropeanaSuggestion_%d' % (
        hash(str(europeana_req_parser.parse_args()))
    )


def suggest_for_author(author_names, titles):
    """Retrieve related resources from Europeana."""

    keywords = []
    author_query = 'who:(%s)^%.1f' % (
        ' OR '.join([
            cleanup_name(name)
            for name in author_names]),
        AUTHOR_WEIGHT,
    )
    response = run_query(author_query, None)
    current_app.logger.info(response)

    if response['totalResults'] > 0:
        response['strategy'] = 'author'
        return response, author_query, keywords

    # extract keywords from titles, compute tfidfs and prepare the query
    tokens = [
        token
        for title in titles
        for token in title.split(' ')
    ]
    keywords_with_tfidf = get_tfidf(tokens)
    selected_keywords = select_keywords(keywords_with_tfidf)

    keyword_query = ' AND '.join(
        [item['keyword'] for item in selected_keywords]
    )

    response = run_query(keyword_query, None)
    if response['totalResults'] > 0:
        response['strategy'] = '{}-keywords'.format(len(selected_keywords))
        return response, keyword_query, selected_keywords

    while(len(selected_keywords) > 1):
        selected_keywords = selected_keywords[:-1]
        current_app.logger.info("Suggest authors with {} keywords".format(
            len(selected_keywords)
        ))
        keyword_query = ' AND '.join(
            [item['keyword'] for item in selected_keywords]
        )
        query = get_query('', keyword_query)
        response = run_query(query, None)

        if len(selected_keywords) > 1:
            if response['totalResults'] > 0:
                response['strategy'] = '{}-keywords'.format(len(
                    selected_keywords)
                )
                return response, query, selected_keywords
        else:
            response['strategy'] = '{}-keywords'.format(len(selected_keywords))
            return response, query, selected_keywords


def suggest_for_publication(author_names, titles):
    """Retrieve related resources from Europeana.."""
    if len(author_names) > 0:
        author_query = 'who:(%s)^%.1f' % (
            ' OR '.join([
                cleanup_name(name)
                for name in author_names]),
            AUTHOR_WEIGHT,
        )
    else:
        author_query = ''

    tokens = [
        token
        for title in titles
        for token in title.split(' ')
    ]
    keywords_with_tfidf = get_tfidf(tokens)
    selected_keywords = select_keywords(keywords_with_tfidf)

    keyword_query = ' AND '.join(
        [item['keyword'] for item in selected_keywords]
    )
    query = get_query(author_query, keyword_query)
    response = run_query(query, None)
    pdb.set_trace()
    if response['totalResults'] > 0:
        response['strategy'] = '{}-keywords'.format(len(selected_keywords))
        return response, query, selected_keywords

    while(len(selected_keywords) > 1):
        selected_keywords = selected_keywords[:-1]
        current_app.logger.info("Suggest publication with {} keywords".format(
            len(selected_keywords)
        ))
        keyword_query = ' AND '.join(
            [item['keyword'] for item in selected_keywords]
        )
        query = get_query(author_query, keyword_query)
        response = run_query(query, None)

        if len(selected_keywords) > 1:
            if response['totalResults'] > 0:
                response['strategy'] = '{}-keywords'.format(len(
                    selected_keywords)
                )
                return response, query, selected_keywords
        else:
            response['strategy'] = '{}-keywords'.format(len(selected_keywords))
            return response, query, selected_keywords


def suggest_for_keywords(tokens, operator='AND'):
    """Query Europeana for keywords."""
    keywords_with_tfidf = get_tfidf(tokens)
    selected_keywords = select_keywords(keywords_with_tfidf)

    query = ' {} '.format(operator).join(
        [item['keyword'] for item in selected_keywords])

    # query = get_query(author_query, keyword_query)
    current_app.logger.info(
        "Query to be executed: {}".format(query)
    )
    return run_query(query, None), query


@api.route('/suggest')
class Suggestion(Resource):
    @cache.cached(key_prefix=get_cache_key, timeout=0)
    @api.doc('europeana_suggestions')
    @api.expect(europeana_req_parser)
    @api.marshal_with(europeana_suggestions)
    def get(self):
        '''
        Suggest Europeana items based on a book, article, author or keywords.

        The parameters can be exactly one of:

         * `author_id`
         * `article_id`
         * `book_id`
         * one or more `keyword`
         * `query` and `cursor`

        **Pagination** is supported via the `query` and `cursor` parameters,
        both returned in each response.
        '''

        args = europeana_req_parser.parse_args()

        request_types = [
            key
            for key, value in args.items()
            if value and key in MUTUALLY_EXCLUSIVE_PARAMETERS
        ]

        if len(request_types) != 1:
            # 404 ("The requested resource could not be found but may be
            # available in the future. Subsequent requests by the client are
            # permissible.") would be better and more precise, but for some
            # reason flask-plus adds "You have requested this URI
            # [/api/europeana/suggest] but did you mean /api/europeana/suggest
            # ?" to the message, and it seems preventable only at application
            # level: https://github.com/flask-restful/flask-restful/issues/449
            api.abort(
                400,
                ("Expecting exactly one type of parameters among %s "
                    "in the url parameters. Got: %s") % (
                    ', '.join(MUTUALLY_EXCLUSIVE_PARAMETERS),
                    str([key for key, value in args.items() if value]),
                )
            )

        request_type = request_types[0]

        # if `cursor` and `query` params are passed to the request
        # it's a pagination query (requesting for next page/cursor)
        if request_type == 'cursor':
            query = args.get('query')
            if not query:
                # 404 would be better too, see above
                api.abort(
                    400,
                    "Expecting both a query and cursor parameter. Got: %s" % (
                        str([key for key, value in args.items() if value]),
                    )
                )
            kws = None
            response = run_query(query, args.get('cursor'))
        else:
            resource_type = request_type.split('_')[0]
            if resource_type in SUGGESTION_ENTITIES:
                resource_id = args.get(request_type)

                (author_names, titles) = get_api_data(
                    resource_type,
                    resource_id
                )

                if resource_type == 'author':
                    response, query, kws = suggest_for_author(
                        author_names,
                        titles
                    )
                else:
                    response, query, kws = suggest_for_publication(
                        author_names,
                        titles
                    )
            else:  # keywords
                kws = []
                response, query = suggest_for_keywords(args.get(request_type))

        # response = run_query(query, args.get('cursor'))
        current_app.logger.debug(response)
        cursor = response['nextCursor'] if 'nextCursor' in response else None
        strategy = response['strategy'] if 'strategy' in response else None
        tot = response['totalResults'] if 'totalResults' in response else None

        result = {
            'query': query,
            'total': tot,
            'cursor': cursor,
            'strategy': strategy,
            'keywords': kws,
            'results': parse_response(response['items']),
        }

        return result
