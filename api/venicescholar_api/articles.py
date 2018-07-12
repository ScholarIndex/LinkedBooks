import sys
sys.path += ["../../", "../"]
import logging
from flask_restplus import Namespace, Resource, fields
from commons.dbmodels import Article as ArticleModel
from commons.dbmodels import Disambiguation as DisambiguationModel
from mongoengine import DoesNotExist
from .queries import get_citing_publications, get_cited_publications
from .references import reference_fields
from .primary_sources import primary_source_fields
from .models import author_fields, article_fields, book_fields, citing_publication_list, cited_publication_list
from .parsers import req_parser
from .cache import cache

logger = logging.getLogger('lbapi')

api = Namespace('articles', description='Articles related operations')

api.models[author_fields.name] = author_fields
api.models[article_fields.name] = article_fields
api.models[book_fields.name] = book_fields
api.models[citing_publication_list.name] = citing_publication_list
api.models[cited_publication_list.name] = cited_publication_list

article_data = api.model('ArticleData', {
    'article' : fields.Nested(article_fields, required=True, description="Information about the article"),
    'citing' : fields.Nested(citing_publication_list, required=False, description="A list of publications citing this article (incoming citations)"),
    'cited' : fields.Nested(cited_publication_list, required=False, description="A list of publications/sources cited by this article (outgoing citations)"),
    })

def get_article_data(article_id, concise=False):
    try:
        article = ArticleModel.objects(id=article_id).get()
        assert article is not None

        try:
            article.author = article.get_author()
        except DoesNotExist as e:
            logger.warning("Broken author disambiguation while retrieving article %s: %s" % (article_id, e))
            article.author = []

        if article.author is not None:
            for author in article.author:
                author.viaf_link = author.get_viaf_link()

        if not concise:
            article_data = {
                'article': article,
                'citing':get_citing_publications(article),
                'cited': get_cited_publications(article)
            }
        else:
            article_data = {'article':article}

        return article_data
    except DoesNotExist as e:
        print(e)
        api.abort(404, "Article %s does not exist in the database" % article_id)


@api.route('/')
@api.expect(req_parser)
class ArticleList(Resource):
    @api.doc('list_articles')
    @api.marshal_list_with(article_data)
    def get(self):
        """
        List all articles.

        This endpoint should be used **primarily** to get the IDs of all articles.
        Then each ID can be used to query the `articles/{id}` endpoint, in order to get the complete infomation.

        **Pagination** is supported via the `offset` and `limit` parameters.
        """ # noqa
        args = req_parser.parse_args()
        offset = args['offset']
        limit = offset + args['limit']
        return [
            get_article_data(article.id, concise=True)
            for article in ArticleModel.objects[offset:limit]
        ]


@api.route('/<mongoid>')
class Article(Resource):
    @cache.cached(key_prefix='Article_%s', timeout=0)
    @api.doc('get_article')
    @api.marshal_with(article_data)
    def get(self, mongoid):
        """
        Fetch an article given its MongoID.

        It returns:
        1. **`article`**: metadata about the article
        2. **`citing`**: the list of publications citing this article (incoming citations)
        3. **`cited`**: the list of publications cited by this article (outgoing citations)
        """ # noqa

        return get_article_data(mongoid)
