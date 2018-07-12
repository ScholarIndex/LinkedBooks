import sys
sys.path += ["../../", "../"]
import logging
from flask_restplus import Namespace, Resource, fields
from mongoengine import DoesNotExist
from .models import stats_fields, stats_summary
from .queries import get_statistics
from .cache import cache

logger = logging.getLogger('lbapi')

api = Namespace('stats', description='Statistics related operations')
api.models[stats_summary.name] = stats_summary
api.models[stats_fields.name] = stats_fields


@api.route('/')
class StatsSummary(Resource):
    @cache.cached(key_prefix="Stats_%s", timeout=0)
    @api.doc('get_stats')
    @api.marshal_list_with(stats_summary)
    def get(self):
        """Return statistics.

        Provides the following statistics:
        - number of total scans
        - number of digitized books
        - number of digitized journals
        - number of digitized journal issues
        - number of individual journal articles
        - total number of authors
        - number of cited authors
        - number of cited articles
        - number of cited primary sources
        - total number of annoations
        - number of annotations in journals
        - number of annotations in books
        - total number of references
        - number of references to primary sources
        - number of references to secondary sources
        """
        return {"stats": get_statistics()}
