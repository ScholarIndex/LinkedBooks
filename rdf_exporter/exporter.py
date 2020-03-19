"""
Functions/script to export the LinkedBooks data into OpenCitation's RDF format.

Usage:
    rdf_exporter/exporter.py --help
    rdf_exporter/exporter.py --api-base=<url> --out-dir=<path> --rdf-base=<uri>

Options:
    --api-base=<url>    http://api.venicescholar.eu/v1
    --rdf-base=<uri>    https://w3id.org/oc/corpus/
    --out-dir=<path>    ../LinkedBooks/LinkedBooksCitationCorpus/data

Example:
    python -m ipdb rdf_exporter/exporter.py --api-base=https://api-venicescholar.dhlab.epfl.ch/v1/ \
    --out-dir=/Users/matteo/Downloads/ --rdf-base=https://w3id.org/oc/corpus/
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

from helpers import APIWrapper
from helpers import Entity, ProvenanceEntity, TYPE_MAPPINGS, PREFIX_MAPPINGS
from serializer import OCCSerializer

logger = logging.getLogger()

OCC_CONTEXT_URI = "https://w3id.org/oc/corpus/context.json"
DATASET_PREFIX = "0120"

####################################
# declaration of rdflib namespaces #
####################################

oc_ns = Namespace("https://w3id.org/oc/ontology/")
spar_prov_ns = Namespace("http://purl.org/spar/pro/")
prov_ns = Namespace("http://www.w3.org/ns/prov#")
occ_ns = Namespace("https://w3id.org/oc/ontology/")
fabio_ns = Namespace("http://purl.org/spar/fabio/")
cito_ns = Namespace("http://purl.org/spar/cito/")
datacite_ns =  Namespace("http://purl.org/spar/datacite/")


class RDFExporter(object):
    """todo"""

    def __init__(self, api_base_uri, rdf_base_uri, prefix, serializer):
        """Initialize the RDFExporter."""
        self._api_wrapper = APIWrapper(api_base_uri)
        self._rdf_base = rdf_base_uri
        self._serializer = serializer
        self._prefix = prefix

        # create entity dataframe based on namedtuple structure
        self._entities_df = pd.DataFrame(
            [],
            columns=Entity._fields
        )

        # create provenance dataframe based on namedtuple structure
        self._prov_entities_df = pd.DataFrame(
            [],
            columns=ProvenanceEntity._fields
        )

    def _save(self, entities) -> None:
        """Serialize a list of entities.

        The exporter does not actually do the serialization right away, but
        passes them to an `OCCSerializer`, which takes care of the whole
        serialization business. But it does keep track of the already exported
        entities (via a pandas DataFrame).

        :param entities: a list of entities to serialize
        :type entities: list of `Entity` instances
        """
        # differentiate between provenance and non-provenance entities
        normal_entities = [
            entity
            for entity in entities
            if isinstance(entity, Entity)
        ]

        provenance_entities = [
            entity
            for entity in entities
            if isinstance(entity, ProvenanceEntity)
        ]

        if normal_entities:
            self._entities_df = self._entities_df.append(
                pd.DataFrame(normal_entities, columns=Entity._fields),
                ignore_index=True
            )

            for entity in entities:
                self._serializer.add(entity)

        if provenance_entities:
            self._prov_entities_df = self._prov_entities_df.append(
                pd.DataFrame(
                    provenance_entities,
                    columns=ProvenanceEntity._fields
                ),
                ignore_index=True
            )

            for entity in provenance_entities:
                self._serializer.add(entity, is_provenance=True)
        return

    def mint_uri(self, entity_type: str) -> URIRef:
        prefix = TYPE_MAPPINGS[entity_type]
        count = self._serializer.count(entity_type) + 1
        resource_id = os.path.join(prefix, self._prefix + str(count))
        uri = URIRef(os.path.join(self._rdf_base, resource_id))
        return resource_id, uri

    def create_label(self, entity_type: str) -> str:
        prefix = TYPE_MAPPINGS[entity_type]
        res_id = self._prefix + str(self._serializer.count(entity_type) + 1)
        return f"{entity_type.replace('-', ' ')} {res_id} [{prefix}/{res_id}]"

    def export(self):
        """Exports data from ScholarIndex API into RDF."""

        ce = self._create_common_entities()
        self._save(ce)

        # export authors
        author_ids = self._api_wrapper.get_authors()

        for item in author_ids:
            author_id = item['author']['id']
            api_url, author_data = self._api_wrapper.get_author(author_id)
            self._export_author(author_data['author'], api_url)

        # export all primary sources (?)
        # export all books (this relies on authors already exported)
        # export all journal articles
        # export all references (this relies on book/articles/sources already exported)
        #   and requires updating the RDF graph of the citing bibl. resource
        pass

    ##################################################
    # methods to create RDF statements (provenance)  #
    ##################################################

    def _create_common_entities(self):
        self._api_curation_agent = self._create_provenance_agent(
            "LinkedBooks API v1.0"
        )
        return [self._api_curation_agent]

    def _create_provenance_agent(self, name: str) -> ProvenanceEntity:
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

    def _create_provenance_record(self) -> ProvenanceEntity:
        return []

    def _create_provenance_activity(self) -> ProvenanceEntity:
        pass

    def _create_provenance_role(self) -> ProvenanceEntity:
        pass

    def _create_provenance_snapshot(self) -> ProvenanceEntity:
        pass

    #####################################
    # methods to create RDF statements  #
    #####################################

    def _create_responsible_agent(self, author_data: dict) -> Entity:
        """Maps an author to a OCDM ResponsibleAgent."""
        # define fields
        entity_type = "responsible_agent"
        resource_id, agent_uri = self.mint_uri(entity_type)
        rdf_label = self.create_label(entity_type)
        lastname, firstname = author_data["name"].split(',')[:2]

        # create the rdflib Graph
        g = Graph()
        g.add((agent_uri, RDF.type, FOAF.agent))
        g.add((agent_uri, FOAF.givenName, Literal(firstname)))
        g.add((agent_uri, FOAF.familyName, Literal(lastname)))
        g.add((agent_uri, RDFS.label, Literal(rdf_label)))

        # TODO: check OCDM mapping instead of sameAs
        if author_data["viaf_link"] is not None:
            g.add((agent_uri, datacite_ns.viaf,

             URIRef(author_data["viaf_link"])))

        # create the intermediate Entity
        e = Entity(
            resource_id=resource_id,
            mongo_id=author_data["id"],
            type=entity_type,
            uri=agent_uri,
            graph=g
        )
        logger.debug("Serialised author: %s" % repr(e))
        return e

    def _create_bibliographic_resource(self, publication_data: dict) -> Entity:
        pass

    # TODO: implement
    def _create_agent_role(self):
        pass

    ####################################
    # methods to map API data to OCDM  #
    ####################################

    def _export_author(self, author_data: dict, api_url: str):
        """Creates RDF representation of an author according to OCDM."""

        new_entities = []
        new_entities.append(self._create_responsible_agent(author_data))
        new_entities += self._create_provenance_record() # pass api_url
        self._save(new_entities)

    def _export_publication(self):
        pass

    def _export_reference(self):
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
    serializer = OCCSerializer(out_dir, json_context=OCC_CONTEXT_URI)
    exporter = RDFExporter(
        api_base_uri,
        rdf_base_uri,
        DATASET_PREFIX,
        serializer
    )
    exporter.export()
    serializer.flush()
    pdb.set_trace()


if __name__ == "__main__":
    main()
