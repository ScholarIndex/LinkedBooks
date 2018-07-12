import sys
sys.path += ["../../", "../"]
from flask_restplus import reqparse
from flask_restplus import Namespace, Resource, fields
from commons.dbmodels import Book as BookModel
from commons.dbmodels import Disambiguation as DisambiguationModel
from commons.dbmodels import ArchivalRecordASVE as ArchivalRecordASVEModel
from mongoengine import DoesNotExist
from .references import reference_fields
from .models import primary_source_fields, citing_publication_list, hierarchy_level_fields
from .queries import get_citing_publications
from .parsers import req_parser
from .cache import cache

api = Namespace('primary_sources', description='Primary sources-related operations')
api.models[primary_source_fields.name] = primary_source_fields
api.models[hierarchy_level_fields.name] = hierarchy_level_fields

primary_source_data = api.model('PrimarySourceData', {
    'primary_source' : fields.Nested(primary_source_fields, required=True, description="Information about the primary source"),
    'citing' : fields.Nested(citing_publication_list, required=False, description="A list of publications citing the primary source"),
    })

ARCHIVES = ["asve"]

def dispatch_get(archive, mongoid, concise=False):
    """
    TODO
    """
    if archive == "asve":

        try:
            document = ArchivalRecordASVEModel.objects(id=mongoid).get()
            assert document is not None
            document.hierarchy = document.get_hierarchy()
            return {
                'primary_source':document,
                'citing': get_citing_publications(document) if not concise else None
            }
        except DoesNotExist:
            api.abort(404, "Archive %s does not contain document %s" % (archive, mongoid))

    else:
        # for now we don't have other archives....
        api.abort(404, "Archive %s is not contained in the database" % archive)

def dispatch_get_all(archive, offset, limit):
    """
    TODO
    """
    if archive == "asve":
        return [dispatch_get(archive, document.id, concise=True) for document in ArchivalRecordASVEModel.objects[offset:limit]]

    else:
        # for now we don't have other archives....
        api.abort(404, "Archive %s is not contained in the database" % archive)


@api.route('/<archive>')
@api.expect(req_parser)
class PrimarySourceList(Resource):
    @api.doc('list_primary_sources')
    @api.marshal_list_with(primary_source_data)
    def get(self, archive):
        """
        List all primary sources by archive.

        This endpoint should be used **primarily** to get the IDs of all primary sources belonging to a certain archive.
        Then each ID can be used to query the `primary_sources/{archive}/{mongoid}` endpoint, in order to get the complete infomation.

        **Pagination** is supported via the `offset` and `limit` parameters.
        """ # noqa
        try:
            assert archive in ARCHIVES
            args = req_parser.parse_args()
            offset = args['offset']
            limit = offset + args['limit']
            docs = list(dispatch_get_all(archive, offset, limit))
            return docs
        except AssertionError:
            api.abort(404, "Archive %s is not contained in the database" % archive)


@api.route('/<archive>/<mongoid>')
class PrimarySource(Resource):
    @cache.cached(key_prefix='PrimarySource_%s', timeout=0)
    @api.doc('get_primary_source')
    @api.marshal_with(primary_source_data)
    def get(self, archive, mongoid):
        """
        Fetch a primary source given its archive ID and MongoID.

        It returns:
        1. **`primary_soruce`**: metadata about the primary source
        2. **`citing`**: a list of publications citing the primary source
        """ # noqa
        document = dispatch_get(archive, mongoid)
        return document
