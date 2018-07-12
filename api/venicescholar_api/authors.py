import sys
import logging
sys.path += ["../../", "../"]
from flask_restplus import Namespace, Resource, fields
from commons.dbmodels import Author as AuthorModel
from mongoengine import DoesNotExist
from .cache import cache
from .queries import get_citing_publications, get_occurrences_count
from .queries import get_publications, get_cited_publications
from .models import author_fields, article_fields, book_fields
from .primary_sources import primary_source_fields
from .parsers import req_parser

logger = logging.getLogger(__name__)

api = Namespace('authors', description='Authors related operations')
api.models[author_fields.name] = author_fields

publication_list = api.model('AuthoredPublications', {
    'articles': fields.List(fields.Nested(article_fields)),
    'books': fields.List(fields.Nested(book_fields)),
})

citing_publication_list = api.clone('CitingPublications', publication_list)

cited_publication_list = api.clone('CitedPublications', publication_list, {
    'primary_sources': fields.List(fields.Nested(primary_source_fields))
})

author_data = api.model('AuthorData', {
    'author': fields.Nested(author_fields, required=True),
    'publications': fields.Nested(publication_list, required=False),
    'citing': fields.Nested(citing_publication_list, required=False),
    'cited': fields.Nested(cited_publication_list, required=False),
    'occurrences': fields.Integer(
        required=True,
        default=0,
        description="Number of disambiguations where the author's appear\
         in the DB"
    )
})


def get_author_data(author_id, concise=False):
    try:
        a = AuthorModel.objects(id=author_id).get()
        assert a is not None
        a["viaf_link"] = a.get_viaf_link()

        if not concise:
            author_publications = get_publications(author_id)
            author_data = {
                'author': a,
                'publications': author_publications,
                'citing': get_citing_publications(author_publications),
                'cited': get_cited_publications(author_publications),
                'occurrences': get_occurrences_count(author_id),
            }
        else:
            author_data = {
                'author': a
            }
        return author_data
    except DoesNotExist as e:
        print(e)
        api.abort(404, "Author %s does not exist in the database" % author_id)


@api.route('/')
class AuthorList(Resource):
    @api.doc('list_authors')
    @api.expect(req_parser)
    @api.marshal_list_with(author_data)
    def get(self):
        """
        List all authors.

        This endpoint should be used **primarily** to get the IDs of all authors. Then each
        ID can be used to query the `authors/{id}` endpoint, in order to get the complete infomation.

        **Pagination** is supported via the `offset` and `limit` parameters.
        """ # noqa
        args = req_parser.parse_args()
        offset = args['offset']
        limit = offset + args['limit']
        return [
            get_author_data(a.id, concise=True)
            for a in AuthorModel.objects[offset:limit]
        ]


@api.route('/<mongoid>')
class Author(Resource):
    @cache.cached(key_prefix='Author_%s', timeout=0)
    @api.doc('get_author')
    @api.marshal_with(author_data)
    def get(self, mongoid):
        """Fetch an author given its MongoID.

        It returns:
        1. **`author`**: metadata about the author
        2. **`publications`**: the author's list of publications
        3. **`cited`**: publications cited by the author
        4. **`citing`**: publications where the author is cited
        5. **`occurrences`**: number of DB statements involving the author
        """
        return get_author_data(mongoid)
