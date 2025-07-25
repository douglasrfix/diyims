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


class Clean_Up(SQLModel, table=True):
    DTS: str = Field(primary_key=True)
    want_item_file: str | None = None
    beacon_CID: str | None = None


class Header_Chain_Status(SQLModel, table=True):
    insert_DTS: str = Field(primary_key=True)
    peer_ID: str = Field(primary_key=True)
    missing_header_CID: str = Field(primary_key=True)
    message: str | None = None


class Header_Table(SQLModel, table=True):
    version: str | None = None
    object_CID: str
    object_type: str
    insert_DTS: str
    prior_header_CID: str | None = None
    header_CID: str = Field(primary_key=True)
    peer_ID: str
    processing_status: str | None = None


class Log(SQLModel, table=True):
    DTS: str = Field(primary_key=True)
    process: str | None = None
    pid: int = Field(primary_key=True)
    peer_type: str | None = None
    msg: str | None = None


class Network_Table(SQLModel, table=True):
    network_name: str = Field(primary_key=True)


class Peer_Address(SQLModel, table=True):
    peer_ID: str = Field(primary_key=True)
    multiaddress: str | None = None
    insert_DTS: str
    address_ignored: bool
    ignored_reason: str | None = None
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
    available: bool


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


class Shutdown(SQLModel, table=True):
    enabled: int = Field(primary_key=True)


class Subscription(SQLModel, table=True):
    peer_ID: str | None = None
    object_type: str | None = None
    notify_queue: str | None = None
    header_CID: str = Field(primary_key=True)


class Want_List_Table(SQLModel, table=True):
    peer_ID: str = Field(primary_key=True)
    object_CID: str = Field(primary_key=True)
    insert_DTS: str
    last_update_DTS: str | None = None
    insert_update_delta: int | None = 0
    source_peer_type: str | None = None
