from flask_restplus import reqparse

req_parser = reqparse.RequestParser()
req_parser.add_argument(
    'offset',
    type=int,
    help='Number of results to skip',
    default=0
)
req_parser.add_argument(
    'limit',
    type=int,
    help='Number of results to retrieve',
    default=100
)
req_parser.add_argument(
    'archive',
    type=str,
    help='ID of the Archive (e.g. asve for Archivio di Stato di Venezia)',
)
req_parser.add_argument(
    'mongoid',
    type=str,
    help='The local ID of a given resource',
)



europeana_req_parser = reqparse.RequestParser()

SUGGESTION_ENTITIES = ['author', 'book', 'article']
for entity_type in SUGGESTION_ENTITIES:
    europeana_req_parser.add_argument(
        entity_type+'_id',
        type=str,
        help='The local ID of a ' + entity_type,
    )

europeana_req_parser.add_argument(
    'keyword',
    type=str,
    help='A keyword',
    action='append',
)

europeana_req_parser.add_argument(
    'cursor',
    type=str,
    help='Cursor to the next page of results',
)

europeana_req_parser.add_argument(
    'query',
    type=str,
    help='Query related to the cursor',
)
