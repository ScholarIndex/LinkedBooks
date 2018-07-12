from collections import namedtuple


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
        # 'mongo_id',  # do we need it
        'type',
        'uri',
        'graph',
    ]
)

PREFIX_MAPPINGS = {
    "br": "bibliographic_resource",
    "pa": "provenance_agent"
}

TYPE_MAPPINGS = {
    "bibliographic_resource": "br",
    "provenance_agent": "pa"
}
