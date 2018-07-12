# -*- coding: utf-8 -*-
"""
Functions/script to export the LinkedBooks data into OpenCitation's RDF format.

Usage:
    rdf_exporter/rdf_exporter.py --help
    rdf_exporter/rdf_exporter.py --api-base=<url> --out-dir=<path>

python -m pdb rdf_exporter.py --api-base=http://cdh-dhlabpc6.epfl.ch:8888/api\
        --out-dir=/Users/rromanello/Documents/LinkedBooks/LinkedBooksCitationCorpus/data


"""
__author__ = """Matteo Romanello"""

import codecs
import logging
import os
import pdb
import shutil
import sys
from collections import namedtuple

import pandas as pd
import requests
from docopt import docopt
from datetime import datetime
from rdflib_jsonld.context import Context
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import FOAF, OWL, RDF, RDFS, DCTERMS, XSD
from rdflib import Namespace

sys.path += ["../", "./"]

global PORT, API_BASEURI, AUTHOR_ENDPOINT, AUTHORS_ENDPOINT, ARTICLE_ENDPOINT,\
    ARTICLE_ENDPOINT, BOOKS_ENDPOINT, BOOK_ENDPOINT, PRIMARY_SOURCE_ENDPOINT, \
    PRIMARY_SOURCES_ENDPOINT, REFERENCES_ENDPOINT, REFERENCE_ENDPOINT,\
    STATS_ENDPOINT

logger = logging.getLogger(__name__)


ProvenanceEntity = namedtuple(
    "ProvenanceEntity", [
        'resource_id',
        'type',
        'uri',
        'graph',
        'described_resource_id',
        'described_resource_type'
        # TODO: add `graph`
    ]
)

Entity = namedtuple(
    "Entity", [
        'mongo_id',
        'type',
        'uri',
        'path',  # TODO: remove
        'resource_id',
        # TODO: add `graph`
    ]
)

####################################
# declaration of rdflib namespaces #
####################################

oc_ns = Namespace("https://w3id.org/oc/ontology/")
spar_prov_ns = Namespace("http://purl.org/spar/pro/")
prov_ns = Namespace("http://www.w3.org/ns/prov#")
occ_ns = Namespace("https://w3id.org/oc/ontology/")
fabio_ns = Namespace("http://purl.org/spar/fabio/")
cito_ns = Namespace("http://purl.org/spar/cito/")


################################
# API => RDF mapping functions #
################################

def export(api_base_uri, base_uri, out_dir):
    """Map authors, publications and references onto OpenCitation's data model.
    """
    logger.info("Creating export of LinkedBooks data.")
    logger.info("API Base URI = {}".format(api_base_uri))
    logger.info("RDF base URI = {}".format(base_uri))
    logger.info("Output directory = {}".format(out_dir))

    # TODO: cycle instead of taking 10 records
    offset = 0
    limit = 3
    r = requests.get(
        AUTHORS_ENDPOINT,
        params={'offset': offset, 'limit': limit}
    )

    authors_data = r.json()

    created_entities = []

    # instantiate the curation agent for the LB API
    api_curation_agent = create_prov_agent(
        "LinkedBooks API v1.0",
        os.path.join(base_uri, 'prov'),
        os.path.join(out_dir, 'prov')
    )
    created_entities.append(api_curation_agent)

    df = save_entities(
        pd.DataFrame([], columns=Entity._fields),
        created_entities
    )

    for record in authors_data:

        author_id = record["author"]["id"]
        api_url = AUTHOR_ENDPOINT % author_id
        r = requests.get(api_url)
        data = r.json()

        # check if author not yet contained in df
        if author_id not in list(df['mongo_id']):
            new_entities = export_author(
                data["author"],
                api_url,
                base_uri,
                api_curation_agent,
                out_dir
            )
            df = save_entities(df, new_entities)

        publications = data["publications"]
        pubs = []
        pubs += [("book", pub["id"]) for pub in publications["books"]]
        pubs += [("article", pub["id"]) for pub in publications["articles"]]

        for pub_type, pub_id in pubs:

            # determine the API endpoint to call, based on publication type
            if pub_type == "book":
                api_url = BOOK_ENDPOINT % pub_id
            elif pub_type == "article":
                api_url = ARTICLE_ENDPOINT % pub_id

            r = requests.get(api_url)
            data = r.json()
            pub_data = data["book"] if pub_type == "book" else data["article"]
            cited_pubs = []
            cited_pubs += [
                (pub["id"], "book", pub)
                for pub in data["cited"]["books"]
            ]
            cited_pubs += [
                (pub["id"], "article", pub)
                for pub in data["cited"]["articles"]
            ]
            cited_pubs += [
                (pub["id"], "primary_source", pub)
                for pub in data["cited"]["primary_sources"]
            ]

            ########################################
            # TODO: instantiate cited publications #
            ########################################

            for pub_id, pub_type, pub_info in cited_pubs:

                if pub_type == "book":
                    cited_pub_api_url = BOOK_ENDPOINT % pub_id
                elif pub_type == "article":
                    cited_pub_api_url = ARTICLE_ENDPOINT % pub_id
                elif pub_type == "primary_source":
                    cited_pub_api_url = PRIMARY_SOURCE_ENDPOINT % (
                        "asve",
                        pub_id
                    )

                df = export_publication(
                    pub_id,
                    pub_type,
                    pub_info,
                    cited_pub_api_url,
                    df,
                    base_uri,
                    api_curation_agent,
                    out_dir,
                )

            if len(cited_pubs) > 0:
                # get URIs of cited publications
                cited_pubs_uris = list(
                    df[df['mongo_id'].isin(
                        [p_id for p_id, p_type, p_data in cited_pubs]
                    )]['uri'].unique()
                )
            else:
                cited_pubs_uris = None

            df = export_publication(
                pub_id,
                pub_type,
                pub_data,
                api_url,
                df,
                base_uri,
                api_curation_agent,
                out_dir,
                cited_pubs_uris
            )

    return df


def save_entities(temp_df, entities):
    """TODO."""
    df = pd.concat([
        temp_df,
        pd.DataFrame(entities, columns=Entity._fields)
    ]).reset_index(drop=True)

    logger.debug(
        "Saved {} entities to a dataframe (now with {} rows).".format(
            len(entities),
            df.shape[0]
        )
    )
    return df


def export_journal():
    pass


# NEW
def export_publication(
    publication_id,
    publication_type,
    publication_data,
    api_url,
    df_temp_entities,
    base_uri,
    api_curation_agent,
    out_dir,
    cited_publications_uris=None,
    reference_data=None
):

    created_entities = []

    #######################
    # instantiate authors #
    #######################

    if publication_type != 'primary_source':
        authors = publication_data["author"]
        new_entities = []
        for author in authors:
            mongoid = author["id"]

            if mongoid not in list(df_temp_entities['mongo_id']):
                logger.debug("Author {} has not been serialized".format(
                    mongoid
                ))

                new_entities += export_author(
                    author,
                    AUTHOR_ENDPOINT % mongoid,
                    base_uri,
                    api_curation_agent,
                    out_dir
                )

        df_temp_entities = save_entities(df_temp_entities, new_entities)

        # get author URIs
        author_uris = list(
            df_temp_entities[df_temp_entities['mongo_id'].isin(
                [a['id'] for a in authors]
            )]['uri'].unique()
        )

        # for each publ create agent roles
        agent_role_entities = create_agent_roles(
            author_uris,
            base_uri,
            out_dir
        )
        created_entities += agent_role_entities

    # TODO: create instances for the journal article
    #   hierarchy: journal, volume. Volume => pass the journal
    #   it is part of; article => pass the volume it is part of

    # if journal not in the DataFrame, instantiate

    # if volume not in the DataFrame, instantiate

    #################################
    # TODO: instantiate publication #
    #################################

    containing_resource_uri = None

    if publication_type != "primary_source":
        agent_role_entities_uris = [ar.uri for ar in agent_role_entities]
    else:
        agent_role_entities_uris = []

    br_entity = create_bibliographic_resource(
        publication_data,
        publication_type,
        agent_role_entities_uris,
        containing_resource_uri,
        cited_publications_uris,
        base_uri,
        out_dir,
    )
    created_entities.append(br_entity)

    prov_dir = br_entity.path.replace(".json", "")
    created_entities += create_prov_record(
        br_entity.resource_id,
        "bibliographic resource",
        br_entity.uri,
        api_url,
        base_uri,
        api_curation_agent.uri,
        prov_dir,
        out_dir
    )

    """
    ################################
    # TODO: instantiate references #
    ################################

    reference_data = [
        ref
        for pub_id, pub_type, pub in cited_pubs
        for ref in pub['incoming_references']
    ]

    for reference in reference_data:
        created_entities += export_reference(
            reference,
            base_uri,
            out_dir
        )
    """

    return save_entities(df_temp_entities, created_entities)


# NEW
def export_author(
    author_data,
    api_url,
    base_uri,
    api_curation_agent,
    out_dir
):

    # Export the author to RDF
    ra_entity = create_responsible_agent(author_data, base_uri, out_dir)

    # Create the provenance records concerning the creation of the author
    prov_dir = ra_entity.path.replace(".json", "")
    prov_entity = create_prov_record(
        ra_entity.resource_id,
        "responsible agent",
        ra_entity.uri,
        api_url,
        base_uri,
        api_curation_agent.uri,
        prov_dir,
        out_dir
    )

    return [ra_entity] + prov_entity


def export_journal_volume():
    pass


def export_dataset_metadata(df, base_uri, out_dir):
    pass


#########################
# OCC-related functions #
#########################


def create_bibliographic_resource(
    pub_data,
    pub_type,
    agent_role_uris,
    containing_resource_uri,
    cited_publications_uris,
    base_uri,
    out_dir,
    prefix="br"
):
    """TODO."""
    # create record, dump to disk and create prov record

    # check if dir exists otherwise create it
    # if exists count record and set counter
    bib_resource_path = os.path.join(out_dir, prefix)

    if not os.path.exists(bib_resource_path):
        os.mkdir(bib_resource_path)
        c = 0
    else:
        existing_records = [
            f for f in os.listdir(bib_resource_path)
            if ".json" in f
        ]
        c = len(existing_records)

    n = str(c + 1)
    res_id = "{}:{}".format(prefix, n)
    rdf_label = "bibliographic resource %s [%s/%s]" % (n, prefix, n)

    g = Graph()
    bib_resource_uri = URIRef(os.path.join(base_uri, prefix, n))
    g.add((bib_resource_uri, RDFS.label, Literal(rdf_label)))
    g.add((bib_resource_uri, RDF.type, fabio_ns.Expression))

    for ar_uri in agent_role_uris:
        g.add(
            (
                bib_resource_uri,
                spar_prov_ns.isDocumentContextFor,
                ar_uri
            )
        )

    if cited_publications_uris is not None:
        for pub_uri in cited_publications_uris:
            g.add(
                (
                    bib_resource_uri,
                    cito_ns.cites,
                    pub_uri
                )
            )

    if pub_type == "book":
        """
        - publisher (?)
        """
        g.add((bib_resource_uri, RDF.type, fabio_ns.Book))
        g.add((bib_resource_uri, DCTERMS.title, Literal(pub_data['title'])))

        if pub_data["year"] != "" and pub_data["year"] is not None:
            g.add(
                (
                    bib_resource_uri,
                    fabio_ns.hasPublicationYear,
                    Literal(pub_data["year"], datatype=XSD.year)
                )
            )
    elif pub_type == "article":
        pass
    elif pub_type == "primary_source":
        pass
    else:
        raise Exception

    out_file_path = os.path.join(bib_resource_path, n + ".json")
    with codecs.open(out_file_path, 'wb') as out_file:
        out_file.write(g.serialize(format="json-ld", context="https://w3id.org/oc/corpus/context.json"))

    e = Entity(
        pub_data["id"],
        pub_type,
        bib_resource_uri,
        out_file_path,
        res_id
    )
    logger.debug("Serialised {}: {}".format(pub_type, e))

    return e


# TODO: make this reusable also for publishers (or create a new one)
def create_responsible_agent(author_data, base_uri, out_dir, prefix="ra"):
    """Map an author onto an OpenCitation's Responsible Agent entity.
    """
    # create record, dump to disk and create prov record

    # check if dir exists otherwise create it
    # if exists count record and set counter
    author_path = os.path.join(out_dir, prefix)

    if not os.path.exists(author_path):
        os.mkdir(author_path)
        c = 0
    else:
        existing_records = [f for f in os.listdir(author_path) if ".json" in f]
        c = len(existing_records)

    n = str(c + 1)
    res_id = "{}:{}".format(prefix, n)
    lastname, firstname = author_data["name"].split(',')[:2]
    desc = "responsible agent %s [%s/%s]" % (n, prefix, n)

    g = Graph()
    author_uri = URIRef(os.path.join(base_uri, prefix, n))
    g.add((author_uri, RDF.type, FOAF.agent))
    g.add((author_uri, FOAF.givenName, Literal(firstname)))
    g.add((author_uri, FOAF.familyName, Literal(lastname)))
    g.add((author_uri, RDFS.label, Literal(desc)))

    if author_data["viaf_link"] is not None:
        g.add((author_uri, OWL.sameAs, URIRef(author_data["viaf_link"])))

    out_file_path = os.path.join(author_path, n + ".json")
    with codecs.open(out_file_path, 'wb') as out_file:
        out_file.write(g.serialize(format="json-ld"))

    e = Entity(author_data["id"], "author", author_uri, out_file_path, res_id)
    logger.debug("Serialised author: %s" % repr(e))
    return e


# TODO: implement
def create_bibliographic_entry(reference_data, base_uri, out_dir, prefix="be"):
    """Map a bibliographic reference onto OCC's bibliographic entry entity."""
    # pdb.set_trace()
    return []


# TODO: make this reusable also for publishers (or create a new one)
def create_agent_roles(author_uris, base_uri, out_dir, prefix="ar"):
    agent_role_entities = []

    for n, author_uri in enumerate(author_uris):

        # check if dir exists otherwise create it
        # if exists count record and set counter
        agent_roles_path = os.path.join(out_dir, prefix)
        if not os.path.exists(agent_roles_path):
            os.mkdir(agent_roles_path)
            c = 0
        else:
            existing_records = [
                f for f in os.listdir(agent_roles_path) if ".json" in f
            ]
            c = len(existing_records)

        next_uri = None
        if n + 1 == len(author_uris):
            # it's the last one, there is no next
            pass
        else:
            # it's not the last one
            next_uri = URIRef(os.path.join(base_uri, prefix, str(c + 2)))

        n = str(c + 1)
        res_id = "{}:{}".format(prefix, n)
        rdf_label = "agent role {} [{}]".format(n, res_id)

        g = Graph()
        agent_role_uri = URIRef(os.path.join(base_uri, prefix, n))
        g.add((agent_role_uri, RDF.type, spar_prov_ns.RoleInTime))
        if next_uri is not None:
            g.add((agent_role_uri, oc_ns.hasNext, next_uri))
        g.add((agent_role_uri, spar_prov_ns.isHeldBy, author_uri))
        g.add((agent_role_uri, spar_prov_ns.withRole, spar_prov_ns.author))
        g.add((agent_role_uri, RDFS.label, Literal(rdf_label)))

        out_file_path = os.path.join(agent_roles_path, n + ".json")
        with codecs.open(out_file_path, 'wb') as out_file:
            out_file.write(g.serialize(format="json-ld"))

        e = Entity(
            res_id,
            "agent role",
            agent_role_uri,
            out_file_path,
            res_id
        )
        logger.debug("Serialised agent role: %s" % repr(e))
        agent_role_entities.append(e)

    return agent_role_entities


################################
# Provenance-related functions #
################################


def create_prov_record(
        resource_id,
        resource_label,
        resource_uri,
        source_uri,
        base_uri,
        curation_agent_uri,
        resource_dir,
        out_dir,
        prefix="prov"
):
    # this gets "triggered" for the creation of each entity

    # create the subdirs needed for the provenance
    logger.info(
        "Creating provenance record: {} ({})".format(
            resource_id,
            resource_label
        )
    )
    provenance_entities = []

    # instantiate the curatorial activity
    curatorial_activity = create_prov_activity(
        resource_id,
        resource_label,
        resource_uri,
        curation_agent_uri,
        os.path.join(out_dir, resource_id.replace(":", "/"), prefix)
    )
    provenance_entities.append(curatorial_activity)

    provenance_entities.append(
        create_prov_role(
            resource_id,
            resource_label,
            resource_uri,
            curatorial_activity.uri,
            curation_agent_uri,
            os.path.join(out_dir, resource_id.replace(":", "/"), prefix)
        )
    )

    provenance_entities.append(
        create_prov_snapshot(
            resource_id,
            resource_label,
            resource_uri,
            source_uri,
            curatorial_activity.uri,
            os.path.join(out_dir, resource_id.replace(":", "/"), prefix)
        )
    )

    return provenance_entities


def create_prov_snapshot(
        resource_id,
        resource_label,
        uri,
        source_uri,
        activity_uri,
        out_dir,
        prefix="se"
):
    # each entity can have 1+ snapshots
    g = Graph()

    # determine the sequence number `n` dynamically
    # by looking at the content of the target directory
    prov_path = os.path.join(out_dir, prefix)
    if not os.path.exists(prov_path):
        os.makedirs(prov_path)
        c = 0
    else:
        existing_records = [f for f in os.listdir(prov_path) if ".json" in f]
        c = len(existing_records)
    n = str(c + 1) if c == 0 else str(c)

    snapshot_uri = URIRef("{}/{}/{}/{}".format(uri, "prov", prefix, n))
    out_file_path = os.path.join(prov_path, n + ".json")

    if c == 0:
        n = str(c + 1)
        str_tpl = "snapshot of entity metadata {} related to {} {} [{} -> {}]"
        rdf_label = str_tpl.format(
            n,
            resource_label,
            resource_id.split(":")[-1],
            "{}/{}".format(prefix, n),
            "{}/{}".format(
                resource_id.split(":")[0], resource_id.split(":")[-1]
            ),
        )

        g.add((snapshot_uri, RDF.type, prov_ns.Entity))
        g.add((snapshot_uri, RDFS.label, Literal(rdf_label)))
        g.add((snapshot_uri, prov_ns.specializationOf, uri))
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

        with codecs.open(out_file_path, 'wb') as out_file:
            out_file.write(g.serialize(format="json-ld"))
    else:
        n = str(c)

    e = Entity(
        "{} {}/{}".format(resource_id, prefix, n),
        "entity snapshot",
        snapshot_uri,
        out_file_path,
        "{}/{}".format(prefix, n)
    )
    logger.debug("Created entity snapshot: %s" % repr(e))
    return e


def create_prov_agent(name, base_uri, out_dir, prefix="pa"):
    # list dir to set counter
    # create agent and serialize to disk
    entity_type = "provenance_agent"
    g = Graph()

    prov_path = os.path.join(out_dir, prefix)

    if not os.path.exists(prov_path):
        os.makedirs(prov_path)
        c = 0
    else:
        existing_records = [f for f in os.listdir(prov_path) if ".json" in f]
        c = len(existing_records)

    n = str(c + 1) if c == 0 else str(c)
    agent_uri = URIRef("{}/{}/{}".format(base_uri, prefix, n))
    out_file_path = os.path.join(prov_path, n + ".json")

    if c == 0:
        n = str(c + 1)
        rdf_label = "provenance agent {} [{}/{}]".format(n, prefix, n)

        g.add((agent_uri, RDF.type, prov_ns.Agent))
        g.add((agent_uri, RDFS.label, Literal(rdf_label)))
        g.add((agent_uri, FOAF.name, Literal(name)))

        with codecs.open(out_file_path, 'wb') as out_file:
            out_file.write(g.serialize(format="json-ld"))
    else:
        n = str(c)

    e = Entity(
        "{}/{}".format(prefix, n),
        "provenance_agent",
        agent_uri,
        out_file_path,
        "{}/{}".format(prefix, n)
    )
    logger.debug("Created provenance agent: %s" % repr(e))
    return e


def create_prov_role(
        id,
        label,
        uri,
        activity_uri,
        prov_agent_uri,
        out_dir,
        prefix="cr"
):
    # to associate each prov agent with an activity

    g = Graph()

    prov_path = os.path.join(out_dir, prefix)

    if not os.path.exists(prov_path):
        os.makedirs(prov_path)
        c = 0
    else:
        existing_records = [f for f in os.listdir(prov_path) if ".json" in f]
        c = len(existing_records)

    n = str(c + 1)
    role_uri = URIRef("{}/{}/{}/{}".format(uri, "prov", prefix, n))
    curation_subj_id = "{}/{}".format(prefix, n)
    curation_obj_id = "{}/{}".format(id.split(":")[0], id.split(":")[-1])
    rdf_label = "curatorial role {} related to {} {} [{} -> {}]".format(
        n,
        label,
        id.split(":")[-1],
        curation_subj_id,
        curation_obj_id,
    )

    g.add((role_uri, RDF.type, prov_ns.Association))
    g.add((role_uri, RDFS.label, Literal(rdf_label)))
    g.add((role_uri, prov_ns.agent, prov_agent_uri))
    g.add((role_uri, prov_ns.hadRole, occ_ns.occ_curator))

    out_file_path = os.path.join(prov_path, n + ".json")
    with codecs.open(out_file_path, 'wb') as out_file:
        out_file.write(g.serialize(format="json-ld"))

    e = Entity(
        "{} {}/{}".format(id, prefix, n),
        "curatorial_role",
        role_uri,
        out_file_path,
        curation_obj_id + " " + curation_subj_id
    )
    logger.debug("Serialised curatorial_role: %s" % repr(e))
    return e


def create_prov_activity(
        id,
        label,
        uri,
        curator_agent_uri,
        out_dir,
        prefix="ca"
):

    g = Graph()

    prov_path = os.path.join(out_dir, prefix)

    if not os.path.exists(prov_path):
        os.makedirs(prov_path)
        c = 0
    else:
        existing_records = [f for f in os.listdir(prov_path) if ".json" in f]
        c = len(existing_records)

    n = str(c + 1)
    activity_uri = URIRef("{}/{}/{}/{}".format(uri, "prov", prefix, n))
    rdf_label = "curatorial activity {} related to {} {} [{} -> {}]".format(
        n,
        label,
        id.split(":")[-1],
        "{}/{}".format(prefix, n),
        "{}/{}".format(id.split(":")[0], id.split(":")[-1]),
    )
    desc = "The entity \'{}\' has been created.".format(uri)

    g.add((activity_uri, RDF.type, prov_ns.Activity))
    g.add((activity_uri, RDF.type, prov_ns.Create))
    g.add((activity_uri, RDFS.label, Literal(rdf_label)))
    g.add((activity_uri, DCTERMS.description, Literal(desc)))
    g.add(
        (
            activity_uri,
            prov_ns.qualifiedAssociation, URIRef(curator_agent_uri)
        )
    )

    out_file_path = os.path.join(prov_path, n + ".json")
    with codecs.open(out_file_path, 'wb') as out_file:
        out_file.write(g.serialize(format="json-ld"))

    e = Entity(
        "{} {}/{}".format(id, prefix, n),
        "curatorial_activity",
        activity_uri,
        out_file_path,
        "{}/{}".format(prefix, n)
    )
    logger.debug("Serialised entity: %s" % repr(e))
    return e


########
# MAIN #
########


def main(arguments):

    # use a dataframe to keep track of:
    # Mongo ID, entity_type, uri, path to disk
    # verify that out_dir exists otherwise create it
    out_dir = arguments["--out-dir"]

    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
        logger.debug("Removed existing output directory (%s)" % out_dir)

    subdirs = [
        'ra',
        'ar',
        'br',
        'prov/se',  # entity snapshot
        'prov/pa',  # provenance agent
    ]
    for subdir in subdirs:
        os.makedirs(os.path.join(out_dir, subdir))

    data = export(
        API_BASEURI,
        "https://w3id.org/oc/corpus/",
        out_dir
    )

    data.to_csv(os.path.join(out_dir, "data.csv"), encoding="utf-8")


if __name__ == "__main__":
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
    arguments = docopt(__doc__)

    API_BASEURI = arguments["--api-base"]
    AUTHOR_ENDPOINT = "%s/authors/%s" % (API_BASEURI, "%s")
    AUTHORS_ENDPOINT = "%s/authors" % API_BASEURI
    ARTICLES_ENDPOINT = "%s/articles/" % API_BASEURI
    ARTICLE_ENDPOINT = "%s/articles/%s" % (API_BASEURI, "%s")
    BOOKS_ENDPOINT = "%s/books/" % API_BASEURI
    BOOK_ENDPOINT = "%s/books/%s" % (API_BASEURI, "%s")
    PRIMARY_SOURCE_ENDPOINT = "%s/primary_sources/%s/%s" % (
        API_BASEURI,
        "%s",
        "%s"
    )
    PRIMARY_SOURCES_ENDPOINT = "%s/primary_sources/%s" % (API_BASEURI, "%s")
    REFERENCES_ENDPOINT = "%s/references/" % API_BASEURI
    REFERENCE_ENDPOINT = "%s/references/%s" % (API_BASEURI, "%s")
    STATS_ENDPOINT = "%s/stats/" % API_BASEURI

    main(arguments)
