from flask import Flask, Blueprint
from flask_restplus import Api

from .authors import api as authors_api
from .books import api as books_api
from .articles import api as articles_api
from .primary_sources import api as primary_sources_api
from .references import api as references_api
from .stats import api as stats_api

api_blueprint = Blueprint('api', __name__, url_prefix='/api')

api = Api(
    api_blueprint,
    title='VeniceScholar API',
    version='1.0',
    contact='contact@scholarindex.eu',
    description="A (read-only) API that powers the [VeniceScholar](http://www.venicescholar.eu/).\
                This API exposes the same data that are visualized and made searchable in the [VeniceScholar](http://www.venicescholar.eu/).\n\
                All data served by this API are made available under a [Creative Commons public domain](https://creativecommons.org/publicdomain/zero/1.0/) dedication, in line with [OpenCitations](http://opencitations.net/).\n\
                The API was developed by [Matteo Romanello](https://orcid.org/0000-0002-7406-6286)\
                in the context of the [Linked Books project](https://dhlab.epfl.ch/page-127959-en.html).\n\
                Source code and further documentation can be found on [GitHub](https://github.com/ScholarIndex)." # noqa,
)

api.add_namespace(authors_api)
api.add_namespace(books_api)
api.add_namespace(articles_api)
api.add_namespace(primary_sources_api)
api.add_namespace(references_api)
api.add_namespace(stats_api)
