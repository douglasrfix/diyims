# import enum
from sqlmodel import Field, SQLModel, Enum


class Address_Type(str, Enum):
    IPV4 = "4"
    IPV6 = "6"


class Address_Source(str, Enum):
    PROVIDER_PEER = "PP"
    FIND_PEER = "FP"
    BITSWAP_PEER = "BP"
    SWARM_PEER = "SP"


class Peer_Address(SQLModel, table=True):
    peer_ID: str = Field(primary_key=True)
    multiaddress: str | None = None
    insert_DTS: str
    address_suspect: bool
    address_string: str = Field(primary_key=True)
    address_type: str | None = None
    address_source: str
    address_global: bool
    in_use: bool | None = False
    connect_DTS: str | None = None
    peering_add_DTS: str | None = None
    dis_connect_DTS: str | None = None
    peering_remove_DTS: str | None = None
    reset_DTS: str | None = None


class Peer_Table(SQLModel, table=True):
    peer_ID: str = Field(primary_key=True)
    IPNS_name: str | None = None
    id: str | None = None
    signature: str | None = None
    signature_valid: int | None = None
    peer_type: str
    origin_update_DTS: str
    local_update_DTS: str | None = None
    execution_platform: str | None = None
    python_version: str | None = None
    IPFS_agent: str | None = None
    processing_status: str
    agent: str | None = None
    version: str | None = None


class Want_List_Table(SQLModel, table=True):
    peer_ID: str = Field(primary_key=True)
    object_CID: str = Field(primary_key=True)
    insert_DTS: str
    last_update_DTS: str | None = None
    insert_update_delta: int | None = 0
    source_peer_type: str | None = None
