# import enum
from typing import Any, Optional

#from typing import Any, Dict, Optional
from pydantic import ConfigDict

#from sqlalchemy import JSON
from sqlmodel import JSON, Column, Enum, Field, SQLModel, String


class Address_Type(str, Enum):
    IPV4 = "4"
    IPV6 = "6"


class Address_Source(str, Enum):
    PROVIDER_PEER = "PP"
    FIND_PEER = "FP"
    BITSWAP_PEER = "BP"
    SWARM_PEER = "SP"


class IPFS_Agents(SQLModel, table=True):
    agent_id: str = Field(primary_key=True)


class Clean_Up(SQLModel, table=True):
    insert_DTS: str = Field(primary_key=True)
    satisfy_target_DTS: str | None = None
    status: str | None = None
    want_item_file: str | None = None
    beacon_CID: str | None = None


class Beacon(SQLModel, table=True):
    insert_DTS: str = Field(primary_key=True)
    satisfy_target_DTS: str | None = None
    status: str | None = None
    want_item_dict_str: str | None = None
    beacon_CID: str | None = None


class Header_Chain_Status(SQLModel, table=True):
    insert_DTS: str | None = None
    peer_ID: str = Field(primary_key=True)
    missing_header_CID: str = Field(primary_key=True)
    message: str | None = None


class Object_Meta_Data(SQLModel, table=True):
    __table_args__ = {
        "comment": "This table stores data that would normally be accessed to determine if the primary object is to be retrieved"
    }
    model_config = ConfigDict(arbitrary_types_allowed=True)
    version: int
    object_CID: str
    object_type: str | None = None
    peer_ID: str = Field(
        sa_column=Column(
            String,
            comment="peer_ID appears in this table as human readable source of entry",
        )
    )
    insert_DTS: str
    prior_meta_CID: str | None = None
    meta_CID: str = Field(primary_key=True)
    meta_data: Optional[dict[str, Any]] = Field(default=None, sa_column=Column(JSON))  # noqa: UP045
    tags: Optional[list] = Field(default=None, sa_column=Column(JSON))  # noqa: UP045


class Header_Table(SQLModel, table=True):
    version: str | None = None 
    object_CID: str | None = None
    object_type: str | None = None
    insert_DTS: str
    origin_insert_DTS: str
    prior_header_CID: str | None = None
    header_CID: str = Field(primary_key=True)
    peer_ID: str = Field(primary_key=True
        
    )
    processing_status: str
    processing_status_DTS: str | None = None


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
    IPNS_name: str = "unknown"
    id: str | None = None
    signature: str | None = None
    signature_valid: int | None = 0
    peer_type: str
    origin_update_DTS: str | None = None
    local_update_DTS: str

    execution_platform: str | None = None
    python_version: str | None = None
    IPFS_agent: str | None = None
    processing_status: str
    agent: str | None = None
    version: str | None
    disabled: int | None = 0


class Peer_Control(SQLModel, table=True):
    peer_ID: str = Field(primary_key=True)
    insert_DTS: str | None = None
    processing_status: str
    disabled: int | None = 0
    WLW_retry_enabled: bool | None = False
    WLW_retry_count: int | None = 1


class Peer_Telemetry(SQLModel, table=True):
    peer_ID: str = Field(primary_key=True)
    insert_DTS: str | None = None
    update_DTS: str | None = None
    execution_platform: str | None = None
    python_version: str | None = None
    IPFS_agent: str | None = None
    DIYIMS_agent: str | None = None


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

class Config(SQLModel, table=True):
    section: str = Field(primary_key=True)
    key: str = Field(primary_key=True)
    value: str