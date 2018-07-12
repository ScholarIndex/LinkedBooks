"""
Functions/script to export the LinkedBooks data into OpenCitation's RDF format.

Usage:
    rdf_exporter/exporter.py --help
    rdf_exporter/exporter.py --api-base=<url> --out-dir=<path> --rdf-base=<uri>

Options:
    --api-base=<url>    http://api.venicescholar.eu/v1
    --rdf-base=<uri>    https://w3id.org/oc/corpus/
    --out-dir=<path>    ../LinkedBooks/LinkedBooksCitationCorpus/data
"""  # noqa: E501

import os
import logging
import ipdb as pdb
import pandas as pd

from docopt import docopt

from rdflib_jsonld.context import Context
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import FOAF, OWL, RDF, RDFS, DCTERMS, XSD
from rdflib import Namespace

from helpers import Entity, ProvenanceEntity, TYPE_MAPPINGS, PREFIX_MAPPINGS
from serializer import OCCSerializer

logger = logging.getLogger(__name__)

####################################
# declaration of rdflib namespaces #
####################################

oc_ns = Namespace("https://w3id.org/oc/ontology/")
spar_prov_ns = Namespace("http://purl.org/spar/pro/")
prov_ns = Namespace("http://www.w3.org/ns/prov#")
occ_ns = Namespace("https://w3id.org/oc/ontology/")
fabio_ns = Namespace("http://purl.org/spar/fabio/")
cito_ns = Namespace("http://purl.org/spar/cito/")


class RDFExporter(object):
    """todo"""

    def __init__(self, api_base_uri, rdf_base_uri, serializer):
        """Initialize the RDFExporter."""
        self._api_base = api_base_uri
        self._rdf_base = rdf_base_uri
        self._serializer = serializer
        self._entities_df = pd.DataFrame([], columns=Entity._fields)
        self._prov_entities_df = pd.DataFrame(
            [],
            columns=ProvenanceEntity._fields
        )
        self._init_endpoints()
        self._create_common_entities()

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
        logger.debug(
            "Initialised endpoints with base {}".format(self._api_base)
        )
        return

    def _create_common_entities(self):
        self._api_curation_agent = self._create_provenance_agent(
            "LinkedBooks API v1.0"
        )
        self._serialize([
            self._api_curation_agent
        ])
        return

    def _create_provenance_agent(self, name):
        """Create a provenance agent entity.

        :param name: the agent's name
        :type name: string
        :return: the provenance agent entity
        """
        # define fields
        entity_type = "provenance_agent"
        prefix = TYPE_MAPPINGS[entity_type]
        n = self._serializer.count(entity_type) + 1
        agent_uri = URIRef(os.path.join(self._rdf_base, prefix, str(n)))
        rdf_label = "provenance agent {} [{}/{}]".format(n, prefix, n)

        # create the rdflib Graph
        g = Graph()
        g.add((agent_uri, RDF.type, prov_ns.Agent))
        g.add((agent_uri, RDFS.label, Literal(rdf_label)))
        g.add((agent_uri, FOAF.name, Literal(name)))

        # create the intermediate Entity
        e = ProvenanceEntity(
            resource_id="{}/{}".format(prefix, n),
            type=entity_type,
            uri=agent_uri,
            graph=g,
            described_resource_id=None,
            described_resource_type=None
        )
        logger.debug("Created provenance agent: %s" % repr(e))
        return e

    def _serialize(self, entities):
        """Serialize a list of entities.

        The exporter does not actually do the serialization right away, but
        passes them to an `OCCSerializer`, which takes care of the whole
        serialization business. But it does keep track of the already exported
        entities (via a pandas DataFrame).

        :param entities: a list of entities to serialize
        :type entities: list of `Entity` instances
        """
        # TODO: differentiate between provenance and non-provenance entities
        temp_df = pd.DataFrame(entities, columns=Entity._fields)
        self._entities_df = self._entities_df.append(
            temp_df, ignore_index=True
        )
        for entity in entities:
            self._serializer.add(entity)
        return

    def export(self, limit=None):
        """If limit is not None export only the first n authors."""
        pass


def main():
    # read in CLI parameters
    arguments = docopt(__doc__)
    api_base_uri = arguments["--api-base"]
    rdf_base_uri = arguments["--rdf-base"]
    out_dir = arguments["--out-dir"]

    # handle logging
    log_file = None
    log_level = logging.DEBUG
    logger.setLevel(log_level)

    handler = logging.FileHandler(filename=log_file, mode='w')\
        if log_file is not None else logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # let's get down to businnes
    serializer = OCCSerializer(out_dir)
    exporter = RDFExporter(api_base_uri, rdf_base_uri, serializer)
    exporter.export()
    serializer.flush()
    pdb.set_trace()


if __name__ == "__main__":
    main()
