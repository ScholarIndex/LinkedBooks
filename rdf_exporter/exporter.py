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
from typing import List
from datetime import datetime

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

frbr_ns = Namespace("http://purl.org/vocab/frbr/core#")
oc_ns = Namespace("https://w3id.org/oc/ontology/")
spar_prov_ns = Namespace("http://purl.org/spar/pro/")
prov_ns = Namespace("http://www.w3.org/ns/prov#")
occ_ns = Namespace("https://w3id.org/oc/ontology/")
fabio_ns = Namespace("http://purl.org/spar/fabio/")
cito_ns = Namespace("http://purl.org/spar/cito/")
datacite_ns =  Namespace("http://purl.org/spar/datacite/")
dcterms_ns = Namespace("http://purl.org/dc/terms/")


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

    def count(self, entity_type, is_provenance=False, described_entity=None):
        """Get the entity count for a given entity type."""
        if is_provenance:
            df = self._prov_entities_df
            count = df[
                df.described_resource_id==described_entity.resource_id
            ].shape[0]
            return count
        else:
            df = self._entities_df
            count = df[df.type == entity_type].shape[0]
            return count

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

            for entity in normal_entities:
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
        count = self.count(entity_type) + 1
        resource_id = os.path.join(prefix, self._prefix + str(count))
        uri = URIRef(os.path.join(self._rdf_base, resource_id))
        return resource_id, uri

    def mint_provenance_uri(
        self,
        prov_entity_type: str,
        described_entity: Entity
    ) -> URIRef:
        prefix = TYPE_MAPPINGS[prov_entity_type]
        count = self.count(prov_entity_type, True, described_entity) + 1
        # resource_id = os.path.join(prefix, self._prefix + str(count))
        # TODO: clarify with Silvio
        resource_id = os.path.join(prefix, str(count))
        uri = URIRef(
            os.path.join(
                self._rdf_base,
                described_entity.resource_id,
                "prov",
                prefix,
                str(count)
            )
        )
        return resource_id, uri

    def create_label(self, entity_type: str) -> str:
        prefix = TYPE_MAPPINGS[entity_type]
        res_id = self._prefix + str(self.count(entity_type) + 1)
        return f"{entity_type.replace('_', ' ')} {res_id} [{prefix}/{res_id}]"

    def create_provenance_label(
            self,
            prov_entity_type: str,
            described_entity: Entity
        ) -> str:
        prefix = TYPE_MAPPINGS[prov_entity_type]
        count = self.count(prov_entity_type, True, described_entity) + 1
        # TODO: clarify with Silvio the use of dataset prefix in provenance
        # URIs
        prov_res_id = f"{prefix}/{str(count)}"
        prov_label = prov_entity_type.replace('_', ' ')
        describ_counter = described_entity.resource_id.split(
            '/'
        )[-1].replace(self._prefix, "")
        describ_label = described_entity.type.replace('_', ' ')
        return (
            f"{prov_label} {count} related to {describ_label} "
            f"{describ_counter} [{prov_res_id} -> "
            f"{described_entity.resource_id}]"
        )

    # TODO: implement
    def find_existing_uri(self, local_id, entity_type):
        """
        Given the local ID (mongo id) of an entity, check whether it was
        already processed by querying `self._entities_df`. If it exists
        returns the URI, otherwise returns None
        """
        matches = self._entities_df[self._entities_df.mongo_id==local_id]
        n_matches = matches.shape[0]

        if n_matches == 0:
            return None
        elif n_matches == 1:
            return list(matches.uri)[0]
        else:
            raise

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
        res_id, agent_uri = self.mint_uri(entity_type)
        rdf_label = self.create_label(entity_type)

        # create the rdflib Graph
        g = Graph()
        g.add((agent_uri, RDF.type, prov_ns.Agent))
        g.add((agent_uri, RDFS.label, Literal(rdf_label)))
        g.add((agent_uri, FOAF.name, Literal(name)))

        # create the intermediate Entity
        e = ProvenanceEntity(
            resource_id=res_id,
            type=entity_type,
            uri=agent_uri,
            graph=g,
            described_resource_id=None,
            described_resource_type=None
        )
        logger.debug(f"Created {entity_type.replace('_', ' ')}: {repr(e)}")
        return e

    def _create_provenance_record(
        self,
        entity: Entity,
        api_url: str
    ) -> List[ProvenanceEntity]:
        activity = self._create_provenance_activity(entity, "creation")
        role = self._create_provenance_role(entity)
        snapshot = self._create_provenance_snapshot(
            entity,
            activity_uri=activity.uri,
            source_uri=api_url
        )
        return [activity, snapshot, role]

    def _create_provenance_activity(
        self,
        entity: Entity,
        activity_type: str
    ) -> ProvenanceEntity:
        # define fields
        entity_type = "curatorial_activity"
        prefix = TYPE_MAPPINGS[entity_type]
        res_id, activity_uri = self.mint_provenance_uri(entity_type, entity)
        rdf_label = self.create_provenance_label(entity_type, entity)
        # pdb.set_trace()

        # create the rdflib graph
        g = Graph()
        g.add((activity_uri, RDF.type, prov_ns.Activity))
        g.add((activity_uri, RDFS.label, Literal(rdf_label)))
        # for now we support only creation as curatorial acitity
        if activity_type == 'creation':
            g.add((activity_uri, RDF.type, prov_ns.Create))
            desc = f"The entity \'{str(entity.uri)}\' has been created."
            g.add((activity_uri, DCTERMS.description, Literal(desc)))
        g.add(
            (
                activity_uri,
                prov_ns.qualifiedAssociation, self._api_curation_agent.uri
            )
        )

        # create the intermediate Entity
        e = ProvenanceEntity(
            resource_id=res_id,
            type=entity_type,
            uri=activity_uri,
            graph=g,
            described_resource_id=entity.resource_id,
            described_resource_type=entity.type
        )
        logger.debug(f"Created {entity_type.replace('_', ' ')}: {repr(e)}")
        return e

    # TODO: implement
    def _create_provenance_role(self, entity: Entity) -> ProvenanceEntity:
        # define fields
        entity_type = "curatorial_role"
        prefix = TYPE_MAPPINGS[entity_type]
        res_id, role_uri = self.mint_provenance_uri(entity_type, entity)
        rdf_label = self.create_provenance_label(entity_type, entity)

        # create the rdflib graph
        g = Graph()
        g.add((role_uri, RDF.type, prov_ns.Association))
        g.add((role_uri, RDFS.label, Literal(rdf_label)))
        g.add((role_uri, prov_ns.agent, self._api_curation_agent.uri))
        g.add((role_uri, prov_ns.hadRole, Literal('metadata_provider')))

        e = ProvenanceEntity(
            resource_id=res_id,
            type=entity_type,
            uri=role_uri,
            graph=g,
            described_resource_id=entity.resource_id,
            described_resource_type=entity.type
        )
        logger.debug(f"Created {entity_type.replace('_', ' ')}: {repr(e)}")
        return e

    def _create_provenance_snapshot(
        self,
        entity: Entity,
        activity_uri: URIRef,
        source_uri: str
    ) -> ProvenanceEntity:
        # define fields
        entity_type = "snapshot_entity_metadata"
        prefix = TYPE_MAPPINGS[entity_type]
        res_id, snapshot_uri = self.mint_provenance_uri(entity_type, entity)
        rdf_label = self.create_provenance_label(entity_type, entity)

        # create the rdflib graph
        g = Graph()
        g.add((snapshot_uri, RDF.type, prov_ns.Entity))
        g.add((snapshot_uri, RDFS.label, Literal(rdf_label)))
        g.add((snapshot_uri, prov_ns.specializationOf, entity.uri))
        g.add((snapshot_uri, prov_ns.wasGeneratedBy, activity_uri))
        g.add(
            (
                snapshot_uri,
                prov_ns.hadPrimarySource,
                URIRef(source_uri)
            )
        )
        g.add(
            (
                snapshot_uri,
                prov_ns.generatedAtTime,
                Literal(datetime.utcnow())
            )
        )

        e = ProvenanceEntity(
            resource_id=res_id,
            type=entity_type,
            uri=snapshot_uri,
            graph=g,
            described_resource_id=entity.resource_id,
            described_resource_type=entity.type
        )
        logger.debug(f"Created {entity_type.replace('_', ' ')}: {repr(e)}")
        return e

    #####################################
    # methods to create RDF statements  #
    #####################################

    # TODO: make this reusable also for publishers (or create a new one)
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

    def _create_bibliographic_resource(
        self,
        publication_data: dict,
        resource_type: str,
        agent_role_uris : List[URIRef] = None,
        identifier_uris : List[URIRef] = None,
        part_of : URIRef = None
    ) -> Entity:
        """Maps a publication to an OCDM BibliographiResource."""
        iccu_base_url  = "http://id.sbn.it/bid/"

        # define fields
        local_id = publication_data["id"]
        entity_type = "bibliographic_resource"
        resource_id, bibl_resource_uri = self.mint_uri(entity_type)
        rdf_label = self.create_label(entity_type)

        # create the rdflib Graph
        g = Graph()

        if resource_type == "book":
            # basic metadata
            permalink = URIRef(os.path.join(
                iccu_base_url,
                publication_data['bid']
            ))
            g.add((bibl_resource_uri, RDFS.label, Literal(rdf_label)))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.Expression))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.Book))

            # add authorship relation (via AgentRoles)
            if agent_role_uris:
                for ar_uri in agent_role_uris:
                    g.add(
                        (
                            bibl_resource_uri,
                            spar_prov_ns.isDocumentContextFor,
                            ar_uri
                        )
                    )

            # link to ICCU via BID
            g.add((bibl_resource_uri, dcterms_ns.relation, permalink))

            # publication place
            if publication_data['place']:
                g.add((
                    bibl_resource_uri,
                    fabio_ns.hasPlaceOfPublication,
                    Literal(publication_data['place'].strip())
                ))

            # book publisher
            if publication_data['publisher']:
                g.add((
                    bibl_resource_uri,
                    dcterms_ns.publisher,
                    Literal(publication_data['publisher'].strip())
                ))

            # book title
            g.add(
                (
                    bibl_resource_uri,
                    DCTERMS.title,
                    Literal(publication_data['title'])
                )
            )

            # publication date
            if (
                publication_data["year"] != "" and
                publication_data["year"] is not None
            ):
                g.add(
                    (
                        bibl_resource_uri,
                        fabio_ns.hasPublicationYear,
                        Literal(publication_data["year"], datatype=XSD.year)
                    )
                )
            # - BID/ISBN identifier
            # another related resource could be the ScholarLibrary link
        elif resource_type == "journal_article":
            g.add((bibl_resource_uri, RDFS.label, Literal(rdf_label)))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.Expression))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.JournalArticle))

            # add authorship relation (via AgentRoles)
            if agent_role_uris:
                for ar_uri in agent_role_uris:
                    g.add(
                        (
                            bibl_resource_uri,
                            spar_prov_ns.isDocumentContextFor,
                            ar_uri
                        )
                    )

            # article title
            g.add(
                (
                    bibl_resource_uri,
                    DCTERMS.title,
                    Literal(publication_data['title'])
                )
            )

            # add relation to journal issue or volume
            g.add((bibl_resource_uri, frbr_ns.partOf, part_of))

            # publication date
            if (
                publication_data["year"] != "" and
                publication_data["year"] is not None
            ):
                g.add(
                    (
                        bibl_resource_uri,
                        fabio_ns.hasPublicationYear,
                        Literal(publication_data["year"], datatype=XSD.year)
                    )
                )
        elif resource_type == "journal":
            local_id = publication_data['bid']
            g.add((bibl_resource_uri, RDFS.label, Literal(rdf_label)))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.Expression))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.Journal))

            # journal title
            g.add(
                (
                    bibl_resource_uri,
                    DCTERMS.title,
                    Literal(publication_data['journal_short_title'])
                )
            )
        elif resource_type == "journal_issue":
            journal_bid = publication_data['bid']
            volume_number = publication_data['volume']
            issue_number = publication_data['issue_number']
            local_id = f"{journal_bid}-{volume_number}-{issue_number}"

            g.add((bibl_resource_uri, RDFS.label, Literal(rdf_label)))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.Expression))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.JournalIssue))
            g.add((bibl_resource_uri, frbr_ns.partOf, part_of))
            g.add((
                bibl_resource_uri,
                fabio_ns.hasSequenceIdentifier,
                Literal(issue_number)
            ))
        elif resource_type == "journal_volume":
            journal_bid = publication_data['bid']
            volume_number = publication_data['volume']
            local_id = f"{journal_bid}-{volume_number}"

            g.add((bibl_resource_uri, RDFS.label, Literal(rdf_label)))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.Expression))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.JournalVolume))
            g.add((bibl_resource_uri, frbr_ns.partOf, part_of))
            g.add((
                bibl_resource_uri,
                fabio_ns.hasSequenceIdentifier,
                Literal(volume_number)
            ))
        elif resource_type == "primary_source":
            psource_label = publication_data['label']
            psource_id = publication_data['internal_id']
            link = publication_data['link']
            psource_uri = URIRef(os.path.join(
                "http://data.dhlab.epfl.ch",
                "asve",
                psource_id
            ))

            g.add((bibl_resource_uri, RDFS.label, Literal(rdf_label)))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.Expression))
            g.add((bibl_resource_uri, RDF.type, fabio_ns.ArchivalDocumentSet))
            g.add(
                (
                    bibl_resource_uri,
                    DCTERMS.title,
                    Literal(f"{psource_label} ({psource_id})")
                )
            )
            g.add((bibl_resource_uri, dcterms_ns.relation, URIRef(link)))
            g.add((bibl_resource_uri, RDFS.seeAlso, psource_uri))

        # create the intermediate Entity
        e = Entity(
            resource_id=resource_id,
            mongo_id=local_id,
            type=entity_type,
            uri=bibl_resource_uri,
            graph=g
        )
        logger.debug(f"Serialised {resource_type}: {repr(e)}")
        return e

    def _create_agent_role(
        self,
        agent_uri: URIRef,
        role: str,
        next_uri: None
    ) -> Entity:
        """
        In OCDM authorship is represented as the role of an author
        (ResponsibleAgent) in relation to a given publication
        (BibliographiResource) which becomes the context for this role
        relationship."""

        # define fields
        entity_type = "agent_role"
        resource_id, agent_role_uri = self.mint_uri(entity_type)
        rdf_label = self.create_label(entity_type)

        # create the rdflib Graph
        g = Graph()
        g.add((agent_role_uri, RDF.type, spar_prov_ns.RoleInTime))
        if next_uri is not None:
            g.add((agent_role_uri, oc_ns.hasNext, next_uri))
        g.add((agent_role_uri, spar_prov_ns.isHeldBy, agent_uri))
        if role == 'author':
            g.add((agent_role_uri, spar_prov_ns.withRole, spar_prov_ns.author))
        g.add((agent_role_uri, RDFS.label, Literal(rdf_label)))

        # create the intermediate Entity
        e = Entity(
            resource_id=resource_id,
            mongo_id=None,
            type=entity_type,
            uri=agent_role_uri,
            graph=g
        )
        logger.debug(f"Serialised {entity_type}: {repr(e)}")
        return e

    def _create_identifier(self):
        pass

    # TODO: implement
    def _create_reference_pointer(self):
        pass

    def _create_discourse_element(self):
        pass

    ####################################
    # methods to map API data to OCDM  #
    ####################################

    def export(self):
        """Exports data from ScholarIndex API into RDF."""

        ce = self._create_common_entities()
        self._save(ce)

        books = []
        articles = []
        citations = []

        # export authors
        author_ids = self._api_wrapper.get_authors()
        for item in author_ids:
            author_id = item['author']['id']
            api_url, author_data = self._api_wrapper.get_author(author_id)
            self._export_author(author_data['author'], api_url)
            books += author_data['publications']['books'] # only for dev
            articles += author_data['publications']['articles'] # only for dev

        # export primary sources
        primary_sources = [('asve', '59157b67fe76835e499fa4cd')]
        for archive_id, primary_source_id in primary_sources:
            api_url, psource_data = self._api_wrapper.get_primary_source(
                archive_id,
                primary_source_id
            )
            citing_publications = psource_data['citing']
            psource_data = psource_data['primary_source']
            self._export_primary_source(psource_data, api_url)

            books += [book for book in citing_publications['books']]
            articles += [article for article in citing_publications['articles']]

        # export books
        for book in books:
            book_id = book['id']
            #api_url, book_data = self._api_wrapper.get_book(book_id)
            #self._export_book(book_data['book'], api_url)
            api_url = "bogus-url"
            self._export_book(book, api_url)

            """
            for book in book_data['cited']['books']:
                for reference in book['incoming_references']:
                    citation = {
                        "citing_document_id": book_id,
                        "cited_document_id": book['id'],
                        "reference_id": reference['id']
                    }
            """
            # TODO: do the same for articles and ps

        # export journal articles
        for article in articles:
            article_id = article['id']
            # api_url, book_data = self._api_wrapper.get_book(book_id)
            # self._export_book(book_data, api_url)
            api_url = "bogus-url"
            self._export_article(article, api_url)

        # export all references (this relies on book/articles/sources already exported)
        #   and requires updating the RDF graph of the citing bibl. resource

        # reference_ids = self._api_wrapper.get_references()
        reference_ids = []
        for reference in reference_ids:
            pass

    def _export_author(self, author_data: dict, api_url: str) -> None:
        """Creates  an RDF representation of an author according to OCDM.

        ..note::
            Creates also the provenance statements.
        """
        new_entities = []
        author_entity = self._create_responsible_agent(author_data)
        new_entities.append(author_entity)
        new_entities += self._create_provenance_record(author_entity, api_url)
        self._save(new_entities)

    def _export_book(self, book_data: dict, api_url: str) -> None:
        """
        Creates  an RDF representation of a book publication according to OCDM.

        ..note::
            Creates also the provenance statements.
        """
        # create identifier and pass uri to `_create_bibliographic_resource()`
        # retrieve author URIs and `_create_agent_role`

        authors_field = book_data['author']
        agent_roles = []
        if authors_field:
            author_uris = [
                (   author['id'],
                    self.find_existing_uri(author['id'], 'responsible_agent')
                )
                for author in authors_field
            ]

            for author_id, author_uri in author_uris:
                if author_uri:
                    ar_entity = self._create_agent_role(
                        agent_uri=author_uri,
                        role='author',
                        next_uri=None
                    )
                    ar_prov_entities = self._create_provenance_record(
                        ar_entity,
                        api_url
                    )
                    agent_roles.append(ar_entity.uri)
                    self._save([ar_entity])
                    self._save(ar_prov_entities)
                else:
                    logger.error(f'Author {author_id} not serialized yet!')
                    pass

        #pdb.set_trace()
        new_entities = []
        book_entity = self._create_bibliographic_resource(
            book_data,
            resource_type='book',
            agent_role_uris=agent_roles
        )
        new_entities.append(book_entity)
        new_entities += self._create_provenance_record(book_entity, api_url)
        self._save(new_entities)

    # TODO: finish implementation
    # TODO: create provenance records
    def _export_article(self, article_data: dict, api_url: str) -> None:
        """Creates an RDF representation of a journal article according to OCDM.

        It creates (if necessary) also implied entities, i.e. journal,
        journal volume, and journal issue. For all the new entities,
        it creates a provenance record.
        """
        new_entities = []


        journal_id = article_data['bid']
        journal_volume = article_data['volume']
        journal_issue_number = article_data['issue_number']

        # check if journal was already processed
        journal_uri = self.find_existing_uri(
            journal_id,
            "bibliographic_resource"
        )

        # if it does not exist, create it
        if journal_uri is None:
            j = self._create_bibliographic_resource(article_data, 'journal')
            self._save([j])
            journal_uri = j.uri

        # check if journal volume was already processed
        journal_volume_uri = self.find_existing_uri(
            f"{journal_id}-{journal_volume}",
            "bibliographic_resource"
        )

        # if journal volume does not exist, create it
        if journal_volume_uri is None:
            jv = self._create_bibliographic_resource(
                article_data,
                'journal_volume',
                part_of=journal_uri
            )
            self._save([jv])
            journal_volume_uri = jv.uri

        # TODO: handle journal issue
        if journal_issue_number:
            # pdb.set_trace()
            pass

        # create entities for journal article itself
        a = self._create_bibliographic_resource(
            article_data,
            'journal_article',
            part_of=journal_volume_uri if journal_volume is not None else None
        )
        new_entities.append(a)

        self._save(new_entities)

        # check if journal volume was already processed
        # check if journal issue was already processed
        # check if journal article was already processed
        pass

    def _export_reference(self):
        pass

    def _export_primary_source(
        self,
        primary_source_data: dict,
        api_url: str
    ) -> None:
        """TODO"""
        new_entities = []
        ps_entity = self._create_bibliographic_resource(
            primary_source_data,
            'primary_source'
        )
        new_entities.append(ps_entity)
        new_entities += self._create_provenance_record(ps_entity, api_url)
        self._save(new_entities)


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
    logging.getLogger("requests").setLevel(logging.WARNING)

    handler = logging.FileHandler(filename=log_file, mode='w')\
        if log_file is not None else logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # let's get down to businnes
    serializer = OCCSerializer(
        out_dir,
        json_context=OCC_CONTEXT_URI,
        queue_size=10
    )
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
