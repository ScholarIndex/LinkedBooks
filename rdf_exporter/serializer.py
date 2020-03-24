import os
import codecs
import logging
import shutil
import pathlib
import ipdb as pdb
from typing import List
from rdflib import Graph

from helpers import Entity, ProvenanceEntity, TYPE_MAPPINGS, PREFIX_MAPPINGS
logger = logging.getLogger(__name__)


class OCCSerializer(object):
    """Serialize an OpenCitationCorpus dataset to disk as JSON-LD."""

    def __init__(self, output_directory, queue_size=1000, json_context=None):
        self._output_directory = output_directory
        self._context = json_context
        self._known_entity_types = []
        self._location_index = {}
        self._queues = {} # a dict of lists
        self._prov_queues = {} # a dict of lists
        self._global_counters = {}
        self._queue_size = queue_size

        # clean the output dir
        shutil.rmtree(self._output_directory)
        pathlib.Path(self._output_directory).mkdir()

    def add(self, entity, is_provenance=False) -> None:
        """Adds an entity to be serialized to a waiting queue.

        Each queue is emptied and written to disk as soon as it reaches
        the size limit as per `self._queue_size`. After adding an entity,
        the method `self._check_queues()` is triggered.
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

        # before adding to queue, check if entity is already contained
        self._check_queues()
        return

    def _check_queues(self):
        """Serializes and empties any queue that reached the limit size."""
        for entity_type in self._queues:
            if len(self._queues[entity_type]) == self._queue_size:
                if entity_type not in self._global_counters:
                    self._global_counters[entity_type] = 0
                self._global_counters[entity_type] += self._queue_size
                self._serialize(entity_type, self._queues[entity_type])
                self._queues[entity_type] = []
                # TODO: handle also related provenance entities

    # TODO: finish implementation
    def _serialize(self, entity_type: str, entities: List):
        """Serializes a list of entities to disk following OCDM's format."""
        records_chunk = self._queue_size * 10
        chunk = self._global_counters[entity_type] // (records_chunk)
        chunk_start = chunk * records_chunk if chunk else 0
        chunk_end = chunk_start + records_chunk

        # the 100th entity should go in <type_prefix>/100/100.json
        if self._global_counters[entity_type] == chunk_start:
            currdir = str(self._global_counters[entity_type])
        # but the 101th should go in <type_prefix>/200/110.json
        else:
            currdir = str(chunk_end)
        basedir = os.path.join(
            self._output_directory,
            TYPE_MAPPINGS[entity_type],
            currdir
        )

        # the output file for non provenance entities
        if len(entities) < self._queue_size:
            n = (self._global_counters[entity_type] // self._queue_size)
            filename = (n + 1) * self._queue_size
            pdb.set_trace()
        else:
            filename = self._global_counters[entity_type]

        output_path = os.path.join(
            basedir,
            f"{filename}.json"
        )

        # the base directory for provenance entities
        # all but provenance agents
        provenance_dir = os.path.join(
            basedir,
            f"{self._global_counters[entity_type]}",
            "prov"
        )
        print(basedir)
        print(output_path)
        print(provenance_dir)

        try:
            pathlib.Path(basedir).mkdir(parents=True)
        except FileExistsError:
            logger.debug(f'Output directory {basedir} already exists.')

        output_graph = Graph()
        for e in entities:
            output_graph += e.graph
            self._location_index[e.uri] = output_path
        with codecs.open(output_path, 'wb') as out_file:
            logger.debug(f'Writing {len(entities)} entities to {output_path}')
            out_file.write(output_graph.serialize(
                format="json-ld",
                context=self._context)
            )
        return

    # TODO: implement
    def update(self, graph):
        pass

    def flush(self):
        """Should be called at the very end; writes to disk entities
        remaining in the queues."""

        # for development: serializes as JSON-LD in the output dir
        # one file with all triples. this is just for development.
        # this logic will then be moved to _serialize() and be much more
        # sophisticated

        for entity_type in self._queues:

            if entity_type not in self._global_counters:
                self._global_counters[entity_type] = 0

            self._global_counters[entity_type] += len(
                self._queues[entity_type]
            )

            self._serialize(entity_type, self._queues[entity_type])
