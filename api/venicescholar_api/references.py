import sys
sys.path += ["../../", "../"]
from flask_restplus import Namespace, Resource, fields
from commons.dbmodels import Disambiguation as DisambiguationModel
from commons.dbmodels import Book as BookModel
from commons.dbmodels import Article as ArticleModel
from commons.dbmodels import Reference as ReferenceModel
from mongoengine import DoesNotExist
from .models import reference_fields
from .parsers import req_parser
from .cache import cache

api = Namespace('references', description='References-related operations')
api.models["Reference"] = reference_fields

def get_reference(mongoid, concise=False):
    try:
        reference = ReferenceModel.objects(id=mongoid).get()

        if not concise:
            containing_publication = reference.get_containing_publication()

            if containing_publication is not None:
                reference.containing_document_type = "article" if type(containing_publication)==ArticleModel else "book"
                reference.containing_document_id = containing_publication.id
            else:
                reference.containing_document_type = None
                reference.containing_document_id = None

            reference.document_id = str(reference.document_id.id)
            context_before, reference_string, context_after = reference.get_snippet()
            context_before = "[...]%s" % context_before if context_before != "" else context_before
            context_after = "%s[...]" % context_after if context_after != "" else context_after
            reference.snippet = "%s<reference>%s</reference>%s" % (context_before, reference_string, context_after)

        return reference
    except DoesNotExist as e:
        api.abort(404, "Reference %s does not exist in the database" % mongoid)


@api.route('/')
@api.expect(req_parser)
class ReferenceList(Resource):
    @api.doc('list_references')
    @api.marshal_list_with(reference_fields)
    def get(self):
        """
        List all references.

        This endpoint should be used **primarily** to get the IDs of all references.
        Then each ID can be used to query the `references/{mongoid}` endpoint, in order to get the complete infomation.

        **Pagination** is supported via the `offset` and `limit` parameters.
        """ # noqa
        args = req_parser.parse_args()
        offset = args['offset']
        limit = offset + args['limit']
        return [
            get_reference(reference.id, concise=True)
            for reference in ReferenceModel.objects[offset:limit]
        ]


@api.route('/<mongoid>')
class Reference(Resource):
    @cache.cached(key_prefix='Reference_%s', timeout=0)
    @api.doc('get_reference')
    @api.marshal_with(reference_fields)
    def get(self, mongoid):
        '''Fetch a Reference given its MongoID'''
        return get_reference(mongoid)
