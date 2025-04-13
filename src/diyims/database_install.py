import json
import os
import sqlite3
from sqlite3 import Error

import aiosql
import requests
from rich import print

from diyims.database_utils import (
    insert_network_row,
    insert_peer_row,
    refresh_network_table_dict,
    refresh_peer_table_dict,
)
from diyims.error_classes import (
    ApplicationNotInstalledError,
    CreateSchemaError,
    PreExistingInstallationError,
)
from diyims.general_utils import get_DTS
from diyims.header_utils import ipfs_header_create
from diyims.ipfs_utils import get_url_dict, test_ipfs_version
from diyims.path_utils import get_path_dict
from diyims.platform_utils import get_python_version, test_os_platform
from diyims.py_version_dep import get_car_path, get_sql_str


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

    try:
        queries.set_pragma(conn)
        print("DB PRAGMA set successfully.")

    except Error as e:
        conn.close()
        raise (CreateSchemaError(e))

    conn.close()
    return


def init():  # NOTE: add wait on ipfs
    # NOTE: change header to pointer
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

    # conn.close()
    # conn = sqlite3.connect(connect_path)

    IPFS_agent = test_ipfs_version()
    os_platform = test_os_platform()

    try:
        python_version = os.environ["OVERRIDE_PYTHON_VERSION"]

    except KeyError:
        python_version = get_python_version()

    """
    Create anchor entry of the linked list.
    It is created so that the entries pointing to real objects have a prior CID
    """

    """
    DTS is the same for all artifacts of this transaction
    """
    DTS = get_DTS()

    print("This process can take several minutes. Have a cup of coffee.")

    # This is required to get the IPNS_name
    object_CID = "null"
    object_type = "linked_list_header"
    header_CID, IPNS_name = ipfs_header_create(DTS, object_CID, object_type)

    print(f"First header CID (head of chain) '{header_CID}'")
    print("Published first header")
    print(f"IPNS_name : '{IPNS_name}'")

    object_CID = IPNS_name
    object_type = "IPNS_name"
    header_CID, IPNS_name = ipfs_header_create(DTS, object_CID, object_type)

    """
    Create the initial peer table entry for this peer.
    """

    DTS = get_DTS()

    with requests.post(url_dict["id"], stream=False) as r:
        r.raise_for_status()
        json_dict = json.loads(r.text)

    peer_table_dict = refresh_peer_table_dict()

    peer_table_dict["peer_ID"] = json_dict["ID"]
    peer_table_dict["IPNS_name"] = IPNS_name
    peer_table_dict["peer_type"] = "LP"  # local provider peer
    peer_table_dict["origin_update_DTS"] = DTS
    peer_table_dict["local_update_DTS"] = DTS
    peer_table_dict["execution_platform"] = os_platform
    peer_table_dict["python_version"] = python_version
    peer_table_dict["IPFS_agent"] = IPFS_agent
    peer_table_dict["processing_status"] = "NPC"  # Normal peer processing complete

    peer_file = path_dict["peer_file"]
    add_params = {"cid-version": 1, "only-hash": "false", "pin": "true"}
    with open(peer_file, "w") as write_file:
        json.dump(peer_table_dict, write_file, indent=4)

    add_files = {"file": open(peer_file, "rb")}

    with requests.post(url=url_dict["add"], params=add_params, files=add_files) as r:
        r.raise_for_status()
        json_dict = json.loads(r.text)

    object_CID = json_dict["Hash"]
    object_type = "peer_table_entry"  # NOTE: distinguish between local and remote entries in table
    header_CID, IPNS_name = ipfs_header_create(DTS, object_CID, object_type)

    print(f"Second header for the peer_table CID '{header_CID}'")

    # print(f"First network peer entry CID '{object_CID}'")
    network_table_dict = refresh_network_table_dict()
    network_table_dict["network_name"] = import_car()
    network_name = network_table_dict["network_name"]
    print(f"network_name : '{network_name}'")  # NOTE: use logging

    object_CID = network_table_dict[
        "network_name"
    ]  # NOTE: Replace the need for this transaction by looking in the peer_table
    object_type = "network_name"
    header_CID, IPNS_name = ipfs_header_create(DTS, object_CID, object_type)

    insert_peer_row(conn, queries, peer_table_dict)
    conn.commit()

    insert_network_row(conn, queries, network_table_dict)
    conn.commit()

    conn.close()
    return


def import_car():
    url_dict = get_url_dict()

    car_path = get_car_path()
    dag_import_files = {"file": car_path}
    dag_import_params = {
        "pin-roots": "true",  # http status 500 if false but true does not pin given not in off-line mode
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

        # import does not pin unless in offline mode so it must be done manually
        pin_add_params = {"arg": imported_CID}
        with requests.post(
            url_dict["pin_add"], params=pin_add_params, stream=False
        ) as r:
            r.raise_for_status()

    return imported_CID


if __name__ == "__main__":
    init()
