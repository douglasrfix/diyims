from sqlmodel import Field, SQLModel


class Peer_Address(SQLModel, table=True):
    peer_ID: str = Field(primary_key=True)
    multiaddress: str = Field(primary_key=True)
    insert_timestamp: str | None = None
    suspect_address: bool = Field(default=False)


class Peer_Table(SQLModel, table=True):
    peer_ID: str = Field(primary_key=True)
    IPNS_name: str | None = None
    id: str | None = None
    signature: str | None = None
    signature_valid: int | None = 0
    peer_type: str | None = None
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
