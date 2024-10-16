import json
import sqlite3
from datetime import datetime, timezone
from sqlite3 import Error

import aiosql
import requests
from rich import print

from diyims.error_classes import (
    ApplicationNotInstalledError,
    CreateSchemaError,
    PreExistingInstallationError,
)
from diyims.header_utils import ipfs_header_create
from diyims.ipfs_utils import test_ipfs_version
from diyims.path_utils import get_path_dict
from diyims.py_version_dep import get_car_path, get_sql_str
from diyims.sql_table_dict import get_network_table_dict, get_peer_table_dict
from diyims.url_utils import get_url_dict


def create():
    try:
        path_dict = get_path_dict()

    except ApplicationNotInstalledError:
        raise

    sql_str = get_sql_str()
    queries = aiosql.from_str(sql_str, "sqlite3")
    connect_path = path_dict["db_file"]
    conn = sqlite3.connect(connect_path)

    try:
        queries.create_schema(conn)
        print("DB Schema creation successful.")

    except Error as e:
        conn.close()
        raise (CreateSchemaError(e))


def init():
    try:
        path_dict = get_path_dict()

    except ApplicationNotInstalledError:
        raise

    url_dict = get_url_dict()
    sql_str = get_sql_str()
    queries = aiosql.from_str(sql_str, "sqlite3")
    connect_path = path_dict["db_file"]
    conn = sqlite3.connect(connect_path)
    network_name = queries.select_network_name(conn)

    if network_name is not None:
        conn.close()
        raise (PreExistingInstallationError(" "))

    conn.close()
    conn = sqlite3.connect(connect_path)

    test_ipfs_version()

    """
    Create anchor entry of the linked list.
    It is created so that the entries pointing to real objects have a prior CID
    """

    """
    DTS is the same for all artifacts of this transaction
    """
    DTS = str(datetime.now(timezone.utc))

    print("This process can take several minutes. Have a cup of coffee.")
    object_CID = "null"
    object_type = "linked_list_header"
    header_CID, IPNS_name = ipfs_header_create(DTS, object_CID, object_type)

    print(f"First header CID (head of chain) '{header_CID}'")
    print("Published first header")

    object_CID = IPNS_name
    object_type = "IPNS_name"
    header_CID, IPNS_name = ipfs_header_create(DTS, object_CID, object_type)

    """

    Create the initial peer table entry for this peer.
    """

    DTS = str(datetime.now(timezone.utc))

    with requests.post(url_dict["id"], stream=False) as r:
        r.raise_for_status()
        json_dict = json.loads(r.text)

    peer_table_dict = get_peer_table_dict()
    peer_table_dict["peer_id"] = json_dict["ID"]
    peer_table_dict["IPNS_name"] = IPNS_name

    peer_file = path_dict["peer_file"]
    add_params = {"only-hash": "false", "pin": "true"}

    with open(peer_file, "w") as write_file:
        json.dump(peer_table_dict, write_file, indent=4)

    add_files = {"file": open(peer_file, "rb")}

    with requests.post(url=url_dict["add"], params=add_params, files=add_files) as r:
        r.raise_for_status()
        json_dict = json.loads(r.text)

    object_CID = json_dict["Hash"]
    object_type = "peer_table_entry"
    header_CID, IPNS_name = ipfs_header_create(DTS, object_CID, object_type)

    print(f"Second header CID '{header_CID}'")
    print("Published second header")

    print(f"First network peer entry CID '{object_CID}'")
    network_table_dict = get_network_table_dict()
    network_table_dict["network_name"] = import_car()
    print(network_table_dict["network_name"])

    object_CID = network_table_dict["network_name"]
    object_type = "network_name"
    header_CID, IPNS_name = ipfs_header_create(DTS, object_CID, object_type)
    print(IPNS_name)

    queries.insert_peer_row(
        conn,
        peer_table_dict["version"],
        peer_table_dict["peer_id"],
        peer_table_dict["update_seq"],
        peer_table_dict["IPNS_name"],
        peer_table_dict["update_dts"],
        peer_table_dict["platform"],
        peer_table_dict["python_version"],
        peer_table_dict["ipfs_agent"],
    )

    queries.commit(conn)
    queries.insert_network_row(
        conn,
        network_table_dict["version"],
        network_table_dict["network_name"],
    )
    queries.commit(conn)
    conn.close()


def import_car():
    url_dict = get_url_dict()

    car_path = get_car_path()
    dag_import_files = {"file": car_path}
    dag_import_params = {
        "pin-roots": "true",  # NOTE: http status 500 if false but true does not pin given not in off-line mode
        "silent": "false",
        "stats": "false",
        "allow-big-block": "false",
    }

    with requests.post(
        url=url_dict["dag_import"], params=dag_import_params, files=dag_import_files
    ) as r:
        r.raise_for_status()
        json_dict = json.loads(r.text)
        imported_CID = json_dict["Root"]["Cid"]["/"]

        # NOTE: import does not pin so it must be done manually
        pin_add_params = {"arg": imported_CID}
        with requests.post(
            url_dict["pin_add"], params=pin_add_params, stream=False
        ) as r:
            r.raise_for_status()

    return imported_CID
