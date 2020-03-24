from collections import namedtuple
import requests
import logging

LOGGER = logging.getLogger(__name__)


ProvenanceEntity = namedtuple(
    "ProvenanceEntity", [
        'resource_id',
        'type',
        'uri',
        'graph',
        'described_resource_id',
        'described_resource_type'
    ]
)

Entity = namedtuple(
    "Entity", [
        'resource_id',
        'mongo_id',  # do we need it
        'type',
        'uri',
        'graph',
    ]
)

PREFIX_MAPPINGS = {
    "br": "bibliographic_resource",
    "pa": "provenance_agent",
    "ra": "responsible_agent",
    "ca": "curatorial_activity"
}

TYPE_MAPPINGS = {
    "bibliographic_resource": "br",
    "provenance_agent": "pa",
    "responsible_agent": "ra",
    "curatorial_activity": "ca"
}

class APIWrapper(object):
    """A Python wrapper for the ScholarIndex API v1."""

    def __init__(self, base_uri: str):
        self._api_base = base_uri
        self._init_endpoints()

    def _init_endpoints(self):
        """Initialise resource endpoints based on ScholarIndex API v1."""

        self._author_endpoint = "{}/authors/{}".format(self._api_base, "%s")
        self._authors_endpoint = "{}/authors".format(self._api_base)
        self._article_endpoint = "{}/articles/{}".format(self._api_base, "%s")
        self._articles_endpoint = "{}/articles/".format(self._api_base)
        self._book_endpoint = "{}/books/{}".format(self._api_base, "%s")
        self._books_endpoint = "{}/books/".format(self._api_base)
        self._primary_source_endpoint = "{}/primary_sources/{}/{}".format(
            self._api_base, "%s", "%s"
        )
        self._primary_sources_endpoint = "{}/primary_sources/{}".format(
            self._api_base, "%s"
        )
        self._reference_endpoint = "{}/references/{}".format(
            self._api_base, "%s"
        )
        self._references_endpoint = "{}/references/".format(self._api_base)
        self._stats_endpoint = "{}/stats/".format(self._api_base)
        LOGGER.debug(
            "Initialised endpoints with base {}".format(self._api_base)
        )
        return

    def get_authors(self):
        """Retrieve list of authors."""
        offset = 0
        limit = 3
        r = requests.get(
            self._authors_endpoint,
            params={'offset': offset, 'limit': limit}
        )
        authors_data = r.json()
        return authors_data

    def get_author(self, author_id):
        """Retrieve data for an author."""
        url = self._author_endpoint % author_id
        r = requests.get(url)
        return (url, r.json())

    def get_book(self, book_id):
        """Retrieve data for a book."""
        url = self._book_endpoint % book_id
        r = requests.get(url)
        return (url, r.json())

    def get_primary_source(self, archive_id, primary_source_id):
        """Retrieve data for a primary source."""
        url = self._primary_source_endpoint % (archive_id, primary_source_id)
        LOGGER.debug(f"Fetching {url}")
        r = requests.get(url)
        return (url, r.json())
