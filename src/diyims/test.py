""" """

import json
import ipaddress
from sqlmodel import SQLModel, Session, select, create_engine, col

# from sqlalchemy import create_engine
from rich import print
from diyims.requests_utils import execute_request
from diyims.path_utils import get_path_dict
from diyims.general_utils import get_DTS
from sqlalchemy.exc import IntegrityError
from diyims.sqlmodels import Peer_Address
# from sqlalchemy.engine import Engine
# from sqlalchemy import event


# class Peer_Address(SQLModel, table=True):
#    peer_ID: str = Field(primary_key=True)
#    multiaddress: str = Field(primary_key=True)
#    insert_timestamp: str | None = None


path_dict = get_path_dict()
connect_path = path_dict["db_file"]
db_url = f"sqlite:///{connect_path}"

# engine = create_engine(db_url, echo=True)
engine = create_engine(db_url, echo=True, connect_args={"timeout": 10})


# @event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA busy_timeout = 100000;")
    cursor.close()


def create_db_and_tables() -> None:
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"

    # engine = create_engine(db_url, echo=True)
    engine = create_engine(db_url, echo=True, connect_args={"timeout": 10})
    SQLModel.metadata.create_all(engine)
    return


def test() -> None:
    peer_ID_list = []
    peer_ID_list.append("12D3KooWBJ4ipFpVD3NvZQpJvdLKmvuVgvEFFLpWG16WBjmVjMV4")
    peer_ID_list.append("12D3KooWEVZcNKwWkxC9edn13qxMsG3bYLmmWkmz3XCrqdCV7Uj1")
    peer_ID_list.append("12D3KooWRwJtRqZQcvThkq2dU5ZbrS5zj6grE8rf4swG7NeFC3dH")
    # extract(peer_ID_list)
    # peer_ID = "12D3KooWBJ4ipFpVD3NvZQpJvdLKmvuVgvEFFLpWG16WBjmVjMV4"
    # peer_ID = "12D3KooWEVZcNKwWkxC9edn13qxMsG3bYLmmWkmz3XCrqdCV7Uj1"
    # peer_ID = "12D3KooWRwJtRqZQcvThkq2dU5ZbrS5zj6grE8rf4swG7NeFC3dH"
    address_source = "PP"
    for peer_ID in peer_ID_list:
        capture_provider_addresses(peer_ID, address_source)
    return


def capture_provider_addresses(peer_ID: str, address_source: str) -> bool:
    address_available = False

    # if capture_peer_addresses(address_list, peer_ID):
    #    address_available = True
    param = {"arg": peer_ID}
    response, status_code, response_dict = execute_request(
        url_key="id",
        param=param,
    )
    print(status_code)
    if status_code == 200:
        peer_dict = json.loads(response.text)
        address_list = peer_dict["Addresses"]
        if capture_peer_addresses(address_list, peer_ID, address_source):
            address_available = True

    return address_available


def capture_peer_addresses(
    address_list: list, peer_ID: str, address_source: str
) -> bool:
    address_available = False
    for address in address_list:
        address_string = address
        print(address)
        address_suspect = False
        address_global = False
        multiaddress = ""
        address_type = ""
        index = address.lower().find(
            "/p2p-circuit"
        )  # most often observed in data order
        if index == -1:
            index = address.lower().find("/web")
            if index == -1:
                index = address.lower().find("/dns")
                if index == -1:
                    index = address.lower().find("/tls")  # this might be ok
                    if index == -1:
                        ip_version = address[:5]

                        index = address.lower().find("/", 5)
                        ip_string = address[5:index]
                        ip_index = index
                        index = address.lower().find(
                            "/p2p/", ip_index
                        )  # if index != to -1 then it is assumed to be multiaddress
                        if index != -1:
                            multiaddress = address

                        if (
                            index == -1
                        ):  # probably a findprovs address which are not multiaddress format
                            port_start = index + 5
                            port = address[port_start:]
                            if port.isnumeric():
                                multiaddress = address + "/p2p/" + peer_ID
                            else:
                                multiaddress = address
                                address_suspect = True

                        if ip_version == "/ip4/":
                            address_type = "4"
                            if ipaddress.IPv4Address(ip_string).is_global:
                                address_global = True
                                insert_DTS = get_DTS()
                                # print(index, ip_version, ip_string, address)

                        else:
                            address_type = "5"
                            if ipaddress.IPv6Address(ip_string).is_global:
                                address_global = True
                                insert_DTS = get_DTS()
                                # print(index, ip_version, ip_string, address)
        try:
            insert_DTS = get_DTS()
            create_peer_address(
                peer_ID,
                multiaddress,
                insert_DTS,
                address_suspect,
                address_string,
                address_type,
                address_source,
                address_global,
            )
            address_available = True

        except IntegrityError:
            pass

    return address_available


def create_peer_address(
    peer_ID: str,
    multiaddress: str,
    insert_DTS: str,
    address_suspect: bool,
    address_string: str,
    address_type: str,
    address_source: str,
    address_global: bool,
) -> None:
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"

    engine = create_engine(db_url, echo=True, connect_args={"timeout": 120})
    session = Session(engine)
    # statement = text("PRAGMA busy_timeout = 100000;")
    # session.exec(statement)
    # statement = text("PRAGMA busy_timeout;")
    # print(session.exec(statement))
    address_row = Peer_Address(
        peer_ID=peer_ID,
        multiaddress=multiaddress,
        insert_DTS=insert_DTS,
        address_suspect=address_suspect,
        address_string=address_string,
        address_type=address_type,
        address_source=address_source,
        address_global=address_global,
    )

    session.add(address_row)
    session.commit()
    session.close()
    return


def select_want_list():
    from diyims.sqlmodels import Want_List_Table
    from datetime import datetime, timedelta, timezone

    # provider_peer_ID = "12D3KooWBJ4ipFpVD3NvZQpJvdLKmvuVgvEFFLpWG16WBjmVjMV4"
    # provider_peer_ID = "12D3KooWEVZcNKwWkxC9edn13qxMsG3bYLmmWkmz3XCrqdCV7Uj1"
    provider_peer_ID = "12D3KooWRwJtRqZQcvThkq2dU5ZbrS5zj6grE8rf4swG7NeFC3dH"

    current_DT = datetime.now(timezone.utc)
    start_off_set = timedelta(hours=1)
    window_duration = timedelta(hours=1)
    start_dts = current_DT - start_off_set
    end_dts = start_dts + window_duration
    query_start_dts = datetime.isoformat(start_dts)
    query_stop_dts = datetime.isoformat(end_dts)
    largest_delta = int(340)
    smallest_delta = int(240)

    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"
    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})
    # address_row = Peer_Address(peer_ID=peer_ID, multiaddress=multiaddress, insert_timestamp=insert_timestamp)
    with Session(engine) as session:
        statement = (
            select(Want_List_Table)
            .where(Want_List_Table.peer_ID == provider_peer_ID)
            .where(
                col(Want_List_Table.last_update_DTS) >= query_start_dts,
                col(Want_List_Table.last_update_DTS) <= query_stop_dts,
                col(Want_List_Table.insert_update_delta) <= largest_delta,
                col(Want_List_Table.insert_update_delta) >= smallest_delta,
            )
            .order_by(col(Want_List_Table.insert_update_delta).desc())
        )
        results = session.exec(statement).all()
        line_list = []
        for want_list_item in results:
            line_list.append(want_list_item)

    print(line_list)

    return


if __name__ == "__main__":
    test()
