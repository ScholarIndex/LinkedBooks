import sys
sys.path += ["../../", "../"]
from flask_restplus import Namespace, Resource, fields
from commons.dbmodels import Article as ArticleModel
from commons.dbmodels import Book as BookModel
from commons.dbmodels import Disambiguation as DisambiguationModel
from mongoengine import DoesNotExist
from .cache import cache
from .queries import get_citing_publications, get_cited_publications
from .models import author_fields, book_fields, citing_publication_list, cited_publication_list
from .references import reference_fields
from .primary_sources import primary_source_fields
from .articles import article_fields
from .parsers import req_parser

api = Namespace('books', description='Books related operations')
api.models[book_fields.name] = book_fields
api.models[citing_publication_list.name] = citing_publication_list
api.models[cited_publication_list.name] = cited_publication_list

book_data = api.model('BookData', {
    'book': fields.Nested(
        book_fields,
        required=True,
        description="Information about the book"
    ),
    'citing': fields.Nested(
        citing_publication_list,
        required=True,
        description="A list of publications citing this book\
         (incoming citations)"
    ),
    'cited': fields.Nested(
        cited_publication_list,
        required=True,
        description="A list of publications/sources cited by this book\
         (outgoing citations)"),
})


def get_book_data(book_id, concise=False):
    try:
        book = BookModel.objects(id=book_id).get()
        assert book is not None
        book.author = book.get_author()
        book.is_digitized = book.in_library()
        book.is_oa = book.is_open_access()
        for author in book.author:
            author.viaf_link = author.get_viaf_link()
        if not concise:
            book_data = {
                'book': book,
                'citing': get_citing_publications(book),
                'cited': get_cited_publications(book)
            }
        else:
            book_data = {'book': book}
        return book_data
    except DoesNotExist as e:
        api.abort(404, "Book %s does not exist in the database" % book_id)


@api.route('/')
@api.expect(req_parser)
class BookList(Resource):
    @api.doc('list_books')
    @api.marshal_list_with(book_data)
    def get(self):
        """
        List all books.

        This endpoint should be used to get the IDs of all books.
        Then each ID can be used to query the `books/{id}` endpoint, in order to get the complete infomation.

        **Pagination** is supported via the `offset` and `limit` parameters.
        """ # noqa
        args = req_parser.parse_args()
        offset = args['offset'] if args['offset'] is not None else 0
        limit = args['limit'] if args['limit'] is not None else 100
        limit = offset + limit
        return [
            get_book_data(book.id, concise=True)
            for book in BookModel.objects[offset:limit]
        ]


@api.route('/<mongoid>')
class Book(Resource):
    @cache.cached(key_prefix='Book_%s', timeout=0)
    @api.doc('get_book')
    @api.marshal_with(book_data)
    def get(self, mongoid):
        '''Fetch a book given its MongoID.

        It returns:
        1. **`book`**: metadata about the book
        2. **`citing`**: the list of publications citing this book (incoming citations)
        3. **`cited`**: the list of publications cited by this book (outgoing citations)
        ''' # noqa
        return get_book_data(mongoid)
