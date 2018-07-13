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
"""
req_parser.add_argument(
    'keyword',
    type=str,
    help='A keyword',
    action="append"
)
"""
