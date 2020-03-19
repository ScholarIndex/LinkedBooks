import os
import codecs
import logging
import ipdb as pdb
from rdflib import Graph

logger = logging.getLogger(__name__)


class OCCSerializer(object):
    """Serialize an OpenCitationCorpus dataset to disk as JSON-LD."""

    def __init__(self, output_directory, queue_size=1000, json_context=None):
        self._output_directory = output_directory
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
        self._queues = {} # a dict of lists
        self._prov_queues = {} # a dict of lists

        # TODO: clean the output dir

    def add(self, entity, is_provenance=False):
        """TODO.

        Check on the type of entity and handle accordingly (if provenance)
        """

        if is_provenance:
            if entity.type not in self._prov_queues:
                self._prov_queues[entity.type] = []
            self._prov_queues[entity.type].append(entity)
            n_items = len(self._prov_queues[entity.type])

        else:
            if entity.type not in self._queues:
                self._queues[entity.type] = []
            self._queues[entity.type].append(entity)
            n_items = len(self._queues[entity.type])

        logger.debug(f"Added {entity.uri} to queue {entity.type}")
        logger.debug(f"Queue {entity.type} contains {n_items} items")



        # TODO: before adding to queue, check if entity is already contained

        # self._check_queues()

    def count(self, entity_type, is_provenance=False):
        """Get the entity count for a given entity type."""
        if is_provenance:
            if entity_type not in self._prov_queues:
                return 0
            else:
                return len(self._prov_queues[entity_type])
        else:
            if entity_type not in self._queues:
                return 0
            else:
                return len(self._queues[entity_type])

    # implement later
    def _check_queues(self):
        """Check if any queue is full: if so serialize and empty.
        - this funct gets called any time a new entity is added
        - checks if the queue size == self.queue_size
        """
        pass

    # TODO: implement simple behavior for now
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
        """Should be called at the very end; writes to disk entities
        remaining in the queues."""

        # for development: serializes as JSON-LD in the output dir
        # one file per entity type

        output_graph = Graph()

        for entity_type in self._queues:
            for e in self._queues[entity_type]:
                output_graph += e.graph

        for entity_type in self._prov_queues:
            for e in self._prov_queues[entity_type]:
                output_graph += e.graph

        out_file_path = os.path.join(self._output_directory, 'test.json')
        with codecs.open(out_file_path, 'wb') as out_file:
            out_file.write(output_graph.serialize(
                format="json-ld",
                context=self._context)
            )
