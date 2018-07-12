import logging

logger = logging.getLogger(__name__)


class OCCSerializer(object):
    """Serialize an OpenCitationCorpus dataset to disk as JSON-LD."""

    def __init__(self, output_directory, queue_size=1000, json_context=None):
        self._context = json_context
        self._known_entity_types = []
        self._mappings = {
            'agent_role': 'ar',
            'bibliographic_resource': 'br',
            'responsible_agent': 'ra',
            'provenance_agent': 'pa',
            'snapshot_entity': 'se'
        }
        self._location_index = {}
        self._counters = {}  # really needed?
        self._queues = {}
        self._prov_queues = {}

        # TODO: clean the output dir

    def add(self, entity):
        """TODO.

        Check on the type of entity and handle accordingly (if provenance)
        """
        if entity.type not in self._queues:
            self._queues[entity.type] = []

        if entity.type not in self._prov_queues:
            self._prov_queues[entity.type] = {}

        if entity.type not in self._counters:
            self._counters["entity_type"] = 1

    def count(self, entity_type, is_provenance=False):
        """Get the entity count for a given entity type."""
        return 0

    def _check_queues(self):
        """Check if any queue is full: if so serialize and empty.
        - this funct gets called any time a new entity is added
        - checks if the queue size == self.queue_size
        """
        pass

    def _serialize(self, entities):
        """Serialize one or more graphs to JSON.

        - for each entity
            - determine the path where it will have to be serialized
            - if needed create directories
            - write to disk
            - update `self._location_index` to store `URI:path` in a `dict`
        """
        pass

    def flush(self):
        pass
